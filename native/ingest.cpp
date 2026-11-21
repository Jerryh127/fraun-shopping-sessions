#include <sys/mman.h>
#include "shared.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <charconv>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string_view>
#include <vector>
#include <chrono>
#include <iostream>
// Stage timings for SHOP_PROFILE=1, as in main.cpp.
static void lap(const char* name) {
    static const bool profiling = std::getenv("SHOP_PROFILE") != nullptr;
    static auto previous = std::chrono::steady_clock::now();
    const auto now = std::chrono::steady_clock::now();
    if (profiling) std::cerr << "  " << name << " " << std::chrono::duration<double>(now - previous).count() << " s\n";
    previous = now;
}

namespace {
constexpr int64_t invalidTime = INT64_MIN;
constexpr int64_t dayMs = 86'400'000;
constexpr unsigned partitionBits = 8;
constexpr size_t partitions = 1u << partitionBits;

int pairValue(const char* p) {
    return 10 * (p[0] - '0') + p[1] - '0';
}

bool digit(char c) { return static_cast<unsigned>(c - '0') < 10; }

// A dataset usually spans only a few weeks. Cache validated dates, comparing
// all ten input bytes so even malformed dates cannot alias a cached date.
int64_t midnight(const char* p) {
    struct Entry { uint64_t yearMonth; uint16_t day; int64_t ms; };
    static thread_local std::array<Entry, 64> cache{};
    uint64_t yearMonth;
    uint16_t digits;
    std::memcpy(&yearMonth, p, 8);
    std::memcpy(&digits, p + 8, 2);
    auto& entry = cache[((static_cast<unsigned>(p[8]) & 3) << 4)
                        | (static_cast<unsigned>(p[9]) & 15)];
    if (entry.yearMonth == yearMonth && entry.day == digits && yearMonth != 0)
        return entry.ms;
    if (p[4] != '-' || p[7] != '-' || !digit(p[0]) || !digit(p[1]) ||
        !digit(p[2]) || !digit(p[3]) || !digit(p[5]) || !digit(p[6]) ||
        !digit(p[8]) || !digit(p[9]))
        return invalidTime;
    int year = 100 * pairValue(p) + pairValue(p + 2);
    int month = pairValue(p + 5);
    int day = pairValue(p + 8);
    if (year == 0 || month < 1 || month > 12 || day < 1) return invalidTime;
    constexpr int lengths[]{0,31,28,31,30,31,30,31,31,30,31,30,31};
    bool leap = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0));
    if (day > lengths[month] + (month == 2 && leap)) return invalidTime;

    year -= month <= 2;
    const int era = year / 400;
    const int yoe = year % 400;
    const int doy = (153 * (month + (month > 2 ? -3 : 9)) + 2) / 5 + day - 1;
    const int days = era * 146097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719468;
    entry = {yearMonth, digits, days * dayMs};
    return entry.ms;
}

int64_t validIngest(const char* p) {
    // The input contract guarantees a valid, whole-second UTC ingest_ts.
    return midnight(p) + 1000LL * (3600 * pairValue(p + 11) +
                                   60 * pairValue(p + 14) + pairValue(p + 17));
}

struct InputChunk {
    std::string_view file;
    size_t begin, end;
};

// Boundaries depend only on file sizes. Readers fault in their own chunks
// and consume every line whose first byte belongs to their range.
std::vector<InputChunk> chunks(const std::vector<std::string_view>& files) {
    std::vector<InputChunk> result;
    constexpr size_t bytes = 2u << 20;
    for (auto file : files)
        for (size_t begin = 0; begin < file.size(); begin += bytes)
            result.push_back({file, begin, std::min(file.size(), begin + bytes)});
    return result;
}

template<class F> void chunkLines(const InputChunk& chunk, F&& visit) {
    const char* bytes = chunk.file.data();
    const size_t fileEnd = chunk.file.size();
    size_t pos = chunk.begin;
    if (pos == 0 || bytes[pos - 1] != '\n') {
        // At offset zero skip the header. Other readers skip the previous
        // chunk's last line, searching at most as far as their own boundary.
        auto prefix = chunk.file.substr(pos, chunk.end - pos);
        size_t newline = prefix.find('\n');
        if (newline == prefix.npos) return;
        pos += newline + 1;
    }
    // Newlines are found 64 bytes at a time; `pos` is the open line's start.
    // 256-bit operations only: the same code runs on the scorer and under
    // emulation, and the cores never enter an AVX-512 frequency licence.
    auto newlinesIn = [](const char* p) -> uint64_t {
        const __m256i newline = _mm256_set1_epi8('\n');
        const uint32_t a = _mm256_movemask_epi8(_mm256_cmpeq_epi8(
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p)), newline));
        const uint32_t b = _mm256_movemask_epi8(_mm256_cmpeq_epi8(
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p + 32)), newline));
        return a | (uint64_t{b} << 32);
    };
    size_t scan = pos;
    while (pos < chunk.end) {
        if (scan + 64 <= fileEnd) {
            uint64_t found = newlinesIn(bytes + scan);
            while (found) {
                const size_t end = scan + std::countr_zero(found);
                visit(std::string_view(bytes + pos, end - pos));
                pos = end + 1;
                if (pos >= chunk.end) return;
                found &= found - 1;
            }
            scan += 64;
        } else {
            size_t end = chunk.file.find('\n', std::max(pos, scan));
            if (end == chunk.file.npos) end = fileEnd;
            visit(std::string_view(bytes + pos, end - pos));
            pos = scan = end + 1;
        }
    }
}

// ---- Block storage -------------------------------------------------------
// Every row stream is a chain of equally sized, cache-line-aligned blocks cut
// from large per-thread extents. Nothing is freed: the process exits first.
constexpr size_t blockBytes = 61440;                 // 960 cache lines
constexpr size_t extentBytes = 512 * blockBytes;     // 30 MB, a multiple of 2 MB
// Blocks whose rows have been consumed are handed to later writers. They
// are written with non-temporal stores, so a cold block costs nothing extra
// and the kernel has no fresh page to supply.
class Recycled {
    std::mutex lock;
    std::vector<void*> blocks;
    std::atomic<size_t> available{0};
public:
    void give(const std::vector<void*>& more) {
        if (more.empty()) return;
        std::lock_guard guard(lock);
        blocks.insert(blocks.end(), more.begin(), more.end());
        available.store(blocks.size(), std::memory_order_relaxed);
    }
    void* take() {
        if (!available.load(std::memory_order_relaxed)) return nullptr;
        std::lock_guard guard(lock);
        if (blocks.empty()) return nullptr;
        void* block = blocks.back();
        blocks.pop_back();
        available.store(blocks.size(), std::memory_order_relaxed);
        return block;
    }
} recycled;

class Slab {
    char* next = nullptr;
    char* end = nullptr;
public:
    void* take() {
        if (void* block = recycled.take()) return block;
        if (next == end) {
            constexpr size_t huge = 2u << 20;
            void* raw = mmap(nullptr, extentBytes + huge, PROT_READ | PROT_WRITE,
                             MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
            if (raw == MAP_FAILED) throw std::bad_alloc();
            next = reinterpret_cast<char*>((reinterpret_cast<uintptr_t>(raw) + huge - 1) & ~(huge - 1));
            end = next + extentBytes;
            madvise(next, extentBytes, MADV_HUGEPAGE);
        }
        void* block = next;
        next += blockBytes;
        return block;
    }
};

template<class T> constexpr size_t rowsPerBlock = blockBytes / sizeof(T);

// Radix scatter with software write combining: rows collect in a small
// per-partition buffer and leave as whole cache lines with non-temporal
// stores, so the destination is never read and the open set stays in L2.
template<class T, size_t Group> class Scatter {
    static_assert(std::is_trivially_copyable_v<T> && blockBytes % sizeof(T) == 0);
    static_assert((Group * sizeof(T)) % 64 == 0 && rowsPerBlock<T> % Group == 0);
    struct alignas(64) Buffer { T rows[Group]; };
    Slab& slab;
    std::vector<Buffer> buffers;
    T* destination(size_t part, size_t start) {
        const size_t offset = start % rowsPerBlock<T>;
        if (!offset) blocks[part].push_back(static_cast<T*>(slab.take()));
        return blocks[part].back() + offset;
    }
public:
    std::vector<uint32_t> counts;
    std::vector<std::vector<T*>> blocks;
    Scatter(Slab& slab, size_t parts) : slab(slab), buffers(parts), counts(parts), blocks(parts) {}

    // Returns the row's position in its partition's chain.
    uint32_t add(size_t part, const T& row) {
        const uint32_t index = counts[part]++;
        const size_t slot = index % Group;
        Buffer& buffer = buffers[part];
        buffer.rows[slot] = row;
        if (slot == Group - 1) {
            auto* from = reinterpret_cast<const __m256i*>(&buffer);
            auto* to = reinterpret_cast<__m256i*>(destination(part, index - slot));
            for (size_t i = 0; i < Group * sizeof(T) / 32; ++i)
                _mm256_stream_si256(to + i, _mm256_load_si256(from + i));
        }
        return index;
    }
    void finish() {
        for (size_t part = 0; part < counts.size(); ++part) {
            const size_t rest = counts[part] % Group;
            if (rest) std::memcpy(destination(part, counts[part] - rest), &buffers[part], rest * sizeof(T));
        }
        _mm_sfence();
        std::vector<Buffer>().swap(buffers);
    }
    void release(size_t part, std::vector<void*>& to) {
        to.insert(to.end(), blocks[part].begin(), blocks[part].end());
    }
    T& at(size_t part, size_t index) { return blocks[part][index / rowsPerBlock<T>][index % rowsPerBlock<T>]; }
    template<class F> void pieces(size_t part, F&& visit) {
        size_t remaining = counts[part];
        for (T* block : blocks[part]) {
            const size_t count = std::min(remaining, rowsPerBlock<T>);
            visit(block, count);
            remaining -= count;
        }
    }
};

// ---- Rows ----------------------------------------------------------------
// A parsed event waits under its raw user ID until identities are known.
struct StagedEvent { Key user; int64_t ts; uint32_t price, category; };
static_assert(sizeof(StagedEvent) == 32 && sizeof(Event) == 32 && sizeof(Click) == 48);

// Every well-formed copy of an event, valid or not, leaves one of these under
// its event_id. `staged` locates the copy's StagedEvent, if it has one.
struct IdRecord { Key id; int64_t ingest; uint64_t staged; };
static_assert(sizeof(IdRecord) == 32);
constexpr uint64_t notStaged = ~uint64_t{0};
constexpr unsigned stagedPartShift = 32, stagedReaderShift = 48;

struct Reader {
    Slab slab;
    Scatter<StagedEvent, 8> events;
    Scatter<Click, 4> clicks;
    Scatter<IdRecord, 4> ids;
    Reader(size_t parts, size_t idShards) : events(slab, parts), clicks(slab, parts), ids(slab, idShards) {}
};
struct Router {
    Slab slab;
    Scatter<Event, 8> events;
    Scatter<Click, 4> clicks;
    explicit Router(size_t buckets) : events(slab, buckets), clicks(slab, buckets) {}
};

struct ParsedEvent {
    Key id;
    int64_t ingest;
    bool valid = false;
    StagedEvent row;
};

// Malformed lines and empty IDs never take part in deduplication. Every other
// copy does, valid or not: an invalid earliest copy suppresses later ones.
inline bool parseEvent(std::string_view line, const Context& ctx, ParsedEvent& out) {
    std::array<std::string_view,7> f;
    if (!csv(line, f) || f[0].empty()) return false;
    out.id = Key::from(f[0]);
    out.ingest = f[3].size() == 20 ? validIngest(f[3].data()) : timestamp(f[3]);
    out.valid = false;
    if (f[1].empty() || f[5].empty()) return true;
    const auto ts = timestamp(f[2]);
    if (ts < ctx.start || ts >= ctx.end || out.ingest - dayMs > ts) return true;
    uint32_t price = 0, category = 0;
    if (f[4].size() == 8 && std::memcmp(f[4].data(), "purchase", 8) == 0) {
        auto priceEnd = f[6].data() + f[6].size();
        auto [end, error] = std::from_chars(f[6].data(), priceEnd, price);
        if (error != std::errc{} || end != priceEnd || price == 0 || price > 10'000'000)
            return true;
        category = ctx.category(Key::from(f[5]));
    } else if (f[4].size() != 4 || (std::memcmp(f[4].data(), "view", 4) != 0 &&
                                    std::memcmp(f[4].data(), "cart", 4) != 0)) {
        return true;
    }
    out.row = {Key::from(f[1]), ts, price, category};
    out.valid = true;
    return true;
}

inline bool parseClick(std::string_view line, const Context& ctx, Click& row) {
    std::array<std::string_view,4> f;
    if (!csv(line, f) || f[0].empty() || f[1].empty()) return false;
    const auto ts = timestamp(f[3]);
    if (ts < ctx.start - 7 * dayMs || ts >= ctx.end) return false;
    const auto campaign = ctx.campaign(Key::from(f[2]));
    if (campaign == UINT32_MAX) return false;
    row = {Key::from(f[1]), Key::from(f[0]), ts, campaign};
    return true;
}

// One ID shard. Of the copies sharing an event_id only the earliest ingested
// keeps its staged row; copies that tie are identical, so any one may stay.
// Tags keep almost all probes inside the slot array, and every potential
// match compares the complete ID.
void deduplicate(size_t shard, unsigned shardBits, std::vector<std::unique_ptr<Reader>>& readers) {
    size_t count = 0;
    for (auto& r : readers) count += r->ids.counts[shard];
    if (count < 2) return;
    if (count >> 31) throw std::runtime_error("Too many rows in one ID shard");
    struct Slot { uint32_t tag, index; };
    static thread_local std::vector<Slot> slots;
    static thread_local std::vector<IdRecord*> blocks;
    const size_t mask = std::bit_ceil(2 * count) - 1;
    slots.assign(mask + 1, Slot{0, 0});
    blocks.clear();
    constexpr size_t perBlock = rowsPerBlock<IdRecord>;
    auto drop = [&](uint64_t staged) {
        if (staged == notStaged) return;
        readers[staged >> stagedReaderShift]->events
            .at((staged >> stagedPartShift) & 0xFFFF, staged & 0xFFFFFFFFu).ts = invalidTime;
    };
    for (auto& r : readers)
        r->ids.pieces(shard, [&](IdRecord* records, size_t n) {
            const uint32_t base = static_cast<uint32_t>(blocks.size() * perBlock);
            blocks.push_back(records);
            for (size_t i = 0; i < n; ++i) {
                IdRecord& record = records[i];
                const uint64_t hash = record.id.hash();
                const uint32_t tag = static_cast<uint32_t>(hash >> 32);
                size_t pos = (hash >> shardBits) & mask;
                for (;; pos = (pos + 1) & mask) {
                    Slot& slot = slots[pos];
                    if (!slot.index) {
                        slot = {tag, base + static_cast<uint32_t>(i) + 1};
                        break;
                    }
                    if (slot.tag != tag) continue;
                    IdRecord& kept = blocks[(slot.index - 1) / perBlock][(slot.index - 1) % perBlock];
                    if (!(kept.id == record.id)) continue;
                    if (record.ingest < kept.ingest) {
                        drop(kept.staged);
                        slot.index = base + static_cast<uint32_t>(i) + 1;
                    } else {
                        drop(record.staged);
                    }
                    break;
                }
            }
        });
}
} // namespace

namespace {
// Eight ASCII digits, most significant first.
inline bool eightDigits(const char* p, uint64_t& value) {
    uint64_t x;
    std::memcpy(&x, p, 8);
    constexpr uint64_t highs = 0xF0F0F0F0F0F0F0F0ull, threes = 0x3030303030303030ull;
    // Every byte is 0x30..0x3F, so adding six cannot carry between bytes.
    if ((x & highs) != threes || ((x + 0x0606060606060606ull) & highs) != threes) return false;
    x = (x & 0x0F0F0F0F0F0F0F0Full) * 2561 >> 8;
    x = (x & 0x00FF00FF00FF00FFull) * 6553601 >> 16;
    value = (x & 0x0000FFFF0000FFFFull) * 42949672960001ull >> 32;
    return true;
}

// Digits only, any number of leading zeros; too large for 64 bits is invalid
// (and would lie outside every window anyway).
inline int64_t epochDigits(std::string_view s) {
    const char* p = s.data();
    const size_t size = s.size();
    if (size > 16) {
        int64_t ms;
        if (!digit(*p)) return invalidTime;
        auto [end, ec] = std::from_chars(p, p + size, ms);
        return ec == std::errc{} && end == p + size ? ms : invalidTime;
    }
    uint64_t head = 0, tail = 0;
    size_t lead = size;
    if (size >= 8) {
        lead = size - 8;
        if (!eightDigits(p + lead, tail)) return invalidTime;
    }
    for (size_t i = 0; i < lead; ++i) {
        const unsigned d = static_cast<unsigned char>(p[i]) - '0';
        if (d > 9) return invalidTime;
        head = head * 10 + d;
    }
    if (size == 0) return invalidTime;
    return static_cast<int64_t>(size >= 8 ? head * 100000000ull + tail : head);
}

// "HH:MM:SS" as seconds since midnight, or -1.
inline int clock(const char* p) {
    uint64_t x;
    std::memcpy(&x, p, 8);
    x ^= 0x30303A30303A3030ull;
    // Digit bytes must now be 0..9 and the colons zero. With every digit
    // byte below sixteen the addition cannot carry.
    if ((x | (x + 0x0606000606000606ull)) & 0xF0F0FFF0F0FFF0F0ull) return -1;
    const unsigned hour = (x & 0xFF) * 10 + (x >> 8 & 0xFF);
    const unsigned min = (x >> 24 & 0xFF) * 10 + (x >> 32 & 0xFF);
    const unsigned sec = (x >> 48 & 0xFF) * 10 + (x >> 56);
    if (hour > 23 || min > 59 || sec > 59) return -1;
    return static_cast<int>(3600 * hour + 60 * min + sec);
}
} // namespace

int64_t timestamp(std::string_view s) {
    const char* p = s.data();
    if ((s.size() != 20 && s.size() != 25) || p[4] != '-') return epochDigits(s);
    const auto base = midnight(p);
    const int seconds = clock(p + 11);
    if (base == invalidTime || p[10] != 'T' || seconds < 0) return invalidTime;
    int offset = 0;
    if (s.size() == 25) {
        if ((p[19] != '+' && p[19] != '-') || p[22] != ':' || !digit(p[20]) ||
            !digit(p[21]) || !digit(p[23]) || !digit(p[24])) return invalidTime;
        const int oh = pairValue(p+20), om = pairValue(p+23);
        if (oh > 23 || om > 59) return invalidTime;
        offset = (p[19] == '+' ? 1 : -1) * (60 * oh + om);
    } else if (p[19] != 'Z') {
        return invalidTime;
    }
    return base + 1000LL * (seconds - 60 * offset);
}

void ingest(const Context& ctx, const std::vector<std::string_view>& eventFiles,
            const std::vector<std::string_view>& clickFiles, std::vector<Bucket>& out,
            unsigned workers, const std::function<void()>& awaitIdentities) {
    if (!std::has_single_bit(out.size())) throw std::logic_error("Bucket count must be a power of two");
    lap("start");
    workers = std::min(workers, 64u);

    auto spans = chunks(eventFiles);
    const size_t eventSpans = spans.size();
    size_t bytes = 0;
    for (auto file : eventFiles) bytes += file.size();
    for (auto& span : chunks(clickFiles)) spans.push_back(span);
    if (spans.empty()) { awaitIdentities(); return; }

    // Staging partitions follow the top hash bits, like the identity table, so
    // one partition later probes one slice of it. ID shards use the low bits.
    size_t clickBytes = 0;
    for (auto file : clickFiles) clickBytes += file.size();
    const unsigned partBits = std::clamp<int>(std::bit_width((bytes + clickBytes) >> 23), 0, 9);
    const unsigned idShardBits = std::countr_zero(std::clamp<size_t>(std::bit_ceil(bytes >> 22), 1, 1024));
    const size_t parts = size_t{1} << partBits;
    const size_t idShards = size_t{1} << idShardBits;
    auto partOf = [&](uint64_t hash) { return static_cast<size_t>((hash >> 1) >> (63 - partBits)); };

    const unsigned readerCount = static_cast<unsigned>(std::min<size_t>(spans.size(), workers));
    std::vector<std::unique_ptr<Reader>> readers(readerCount);
    std::atomic<size_t> next = 0;
    parallel(readerCount, readerCount, [&](size_t r) {
        readers[r] = std::make_unique<Reader>(parts, idShards);
        Reader& reader = *readers[r];
        ParsedEvent event;
        Click click;
        for (;;) {
            const size_t i = next.fetch_add(1, std::memory_order_relaxed);
            if (i >= spans.size()) break;
            const InputChunk& span = spans[i];
            if (i < eventSpans) {
                chunkLines(span, [&](std::string_view line) {
                    if (!parseEvent(line, ctx, event)) return;
                    uint64_t staged = notStaged;
                    if (event.valid) {
                        const size_t part = partOf(event.row.user.hash());
                        staged = (uint64_t(r) << stagedReaderShift) | (uint64_t(part) << stagedPartShift)
                                 | reader.events.add(part, event.row);
                    }
                    reader.ids.add(event.id.hash() & (idShards - 1), IdRecord{event.id, event.ingest, staged});
                });
            } else {
                chunkLines(span, [&](std::string_view line) {
                    if (parseClick(line, ctx, click))
                        reader.clicks.add(partOf(click.person.hash()), click);
                });
            }
        }
        reader.events.finish();
        reader.clicks.finish();
        reader.ids.finish();
    });
    lap("parse");

    parallel(idShards, workers, [&](size_t shard) {
        deduplicate(shard, idShardBits, readers);
        std::vector<void*> spent;
        for (auto& reader : readers) reader->ids.release(shard, spent);
        recycled.give(spent);
    });
    lap("dedup");
    awaitIdentities();
    lap("await-links");

    // Route every surviving row to its person's bucket. A partition's lookups
    // stay within one slice of the identity table.
    std::vector<std::unique_ptr<Router>> routers(workers);
    std::atomic<size_t> nextPart = 0;
    const size_t bucketMask = out.size() - 1;
    parallel(workers, workers, [&](size_t w) {
        routers[w] = std::make_unique<Router>(out.size());
        Router& router = *routers[w];
        std::vector<void*> spent;
        for (;;) {
            const size_t p = nextPart.fetch_add(1, std::memory_order_relaxed);
            if (p >= parts) break;
            spent.clear();
            for (auto& reader : readers) {
                reader->events.pieces(p, [&](const StagedEvent* rows, size_t n) {
                    for (size_t i = 0; i < n; ++i) {
                        const StagedEvent& row = rows[i];
                        if (row.ts == invalidTime) continue;
                        const Key person = ctx.person(row.user);
                        router.events.add(person.hash() & bucketMask,
                                          Event{person, row.ts, row.price, row.category});
                    }
                });
                reader->clicks.pieces(p, [&](const Click* rows, size_t n) {
                    for (size_t i = 0; i < n; ++i) {
                        Click click = rows[i];
                        click.person = ctx.person(click.person);
                        router.clicks.add(click.person.hash() & bucketMask, click);
                    }
                });
                reader->events.release(p, spent);
                reader->clicks.release(p, spent);
            }
            recycled.give(spent);
        }
        router.events.finish();
        router.clicks.finish();
    });
    for (size_t b = 0; b < out.size(); ++b)
        for (auto& router : routers) {
            router->events.pieces(b, [&](const Event* rows, size_t n) { out[b].events.add(rows, n); });
            router->clicks.pieces(b, [&](const Click* rows, size_t n) { out[b].clicks.add(rows, n); });
        }
    // The blocks outlive this function: buckets point into them.
    for (auto& reader : readers) (void)reader.release();
    for (auto& router : routers) (void)router.release();
    lap("route");
}
