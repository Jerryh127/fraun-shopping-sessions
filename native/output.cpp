#include "shared.hpp"

#include <cerrno>
#include <fcntl.h>
#include <span>
#include <unistd.h>

// A minimal Parquet writer: uncompressed, PLAIN-encoded, one v1 data page per
// column chunk. The two tables need nothing more, and the binary then has no
// library to load at start.
namespace fs = std::filesystem;
namespace {

enum Physical : int32_t { INT32 = 1, INT64 = 2, BYTE_ARRAY = 6 };
enum Converted : int32_t { NONE = -1, UTF8 = 0, DATE = 6 };
constexpr int32_t encodingPlain = 0, encodingRle = 3;

struct Column {
    std::string_view name;
    Physical type;
    Converted converted = NONE;
    bool optional = false;
};

class Bytes {
    std::vector<char> data;
    size_t used = 0;
public:
    size_t size() const { return used; }
    const char* begin() const { return data.data(); }
    void clear() { used = 0; }
    // Room for `count` more bytes plus slack, so short strings can be copied whole.
    char* grow(size_t count) {
        if (used + count + 32 > data.size()) data.resize(std::max(data.size() * 2, used + count + 32));
        char* at = data.data() + used;
        used += count;
        return at;
    }
    void byte(uint8_t value) { *grow(1) = static_cast<char>(value); }
    void put(const void* from, size_t count) { std::memcpy(grow(count), from, count); }
};

// Thrift compact protocol, as far as Parquet metadata needs it.
class Thrift {
    Bytes& out;
    int16_t last = 0;
    std::vector<int16_t> enclosing;
    enum : uint8_t { I32 = 5, I64 = 6, BINARY = 8, LIST = 9, STRUCT = 12 };
    void varint(uint64_t value) {
        while (value >= 0x80) {
            out.byte(static_cast<uint8_t>(value) | 0x80);
            value >>= 7;
        }
        out.byte(static_cast<uint8_t>(value));
    }
    void zigzag(int64_t value) { varint((static_cast<uint64_t>(value) << 1) ^ static_cast<uint64_t>(value >> 63)); }
    void field(int16_t id, uint8_t type) {
        const int delta = id - last;
        if (delta > 0 && delta <= 15) {
            out.byte(static_cast<uint8_t>(delta << 4 | type));
        } else {
            out.byte(type);
            zigzag(id);
        }
        last = id;
    }
    void listHeader(uint8_t type, size_t count) {
        if (count < 15) {
            out.byte(static_cast<uint8_t>(count << 4 | type));
        } else {
            out.byte(0xF0 | type);
            varint(count);
        }
    }
public:
    explicit Thrift(Bytes& out) : out(out) {}
    void i32(int16_t id, int32_t value) { field(id, I32); zigzag(value); }
    void i64(int16_t id, int64_t value) { field(id, I64); zigzag(value); }
    void text(int16_t id, std::string_view value) {
        field(id, BINARY);
        varint(value.size());
        out.put(value.data(), value.size());
    }
    void structList(int16_t id, size_t count) { field(id, LIST); listHeader(STRUCT, count); }
    void i32List(int16_t id, std::initializer_list<int32_t> values) {
        field(id, LIST);
        listHeader(I32, values.size());
        for (int32_t v : values) zigzag(v);
    }
    void textList(int16_t id, std::string_view only) {
        field(id, LIST);
        listHeader(BINARY, 1);
        varint(only.size());
        out.put(only.data(), only.size());
    }
    void open(int16_t id) { field(id, STRUCT); element(); }   // a struct-valued field
    void element() { enclosing.push_back(last); last = 0; }    // a struct inside a list, or the root
    void close() {
        out.byte(0);
        last = enclosing.back();
        enclosing.pop_back();
    }
};

class ParquetFile {
    struct Chunk { uint64_t offset, bytes, values; };
    struct Group { uint64_t rows; std::vector<Chunk> chunks; };
    int fd;
    std::vector<Column> columns;
    std::vector<Group> groups;
    Bytes pending;
    uint64_t written = 0;
    uint64_t totalRows = 0;

    void flush() {
        const char* at = pending.begin();
        size_t left = pending.size();
        while (left) {
            const ssize_t n = ::write(fd, at, left);
            if (n < 0) {
                if (errno == EINTR) continue;
                throw std::runtime_error("Cannot write output");
            }
            at += n;
            left -= static_cast<size_t>(n);
        }
        written += pending.size();
        pending.clear();
    }

public:
    ParquetFile(const fs::path& path, std::vector<Column> columns) : columns(std::move(columns)) {
        fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
        if (fd < 0) throw std::runtime_error("Cannot create " + path.string());
        pending.put("PAR1", 4);
    }
    ParquetFile(const ParquetFile&) = delete;

    void beginGroup(size_t rows) {
        if (pending.size() > (4u << 20)) flush();
        groups.push_back({rows, {}});
        totalRows += rows;
    }

    // The next column chunk of the open group: its page header, the validity
    // levels of an optional column, then `valueBytes` for the caller to fill.
    // `present` holds one flag per row and is only read for optional columns.
    char* chunk(size_t valueBytes, const uint8_t* present = nullptr) {
        Group& group = groups.back();
        const Column& column = columns[group.chunks.size()];
        const size_t rows = group.rows;
        Bytes levels;
        if (column.optional) {
            // One bit-packed run, least significant bit first, behind its byte length.
            const size_t packed = (rows + 7) / 8;
            uint8_t header[10];
            size_t headerSize = 0;
            for (uint64_t v = packed << 1 | 1;; v >>= 7) {
                header[headerSize++] = static_cast<uint8_t>(v & 0x7F) | (v >= 0x80 ? 0x80 : 0);
                if (v < 0x80) break;
            }
            const uint32_t length = static_cast<uint32_t>(headerSize + packed);
            levels.put(&length, 4);
            levels.put(header, headerSize);
            char* bits = levels.grow(packed);
            std::memset(bits, 0, packed);
            for (size_t i = 0; i < rows; ++i)
                bits[i >> 3] = static_cast<char>(bits[i >> 3] | (present[i] & 1) << (i & 7));
        }
        const size_t pageBytes = levels.size() + valueBytes;
        if (pageBytes > INT32_MAX) throw std::runtime_error("Page too large");
        const uint64_t start = written + pending.size();
        Thrift header(pending);
        header.element();
        header.i32(1, 0);                                   // DATA_PAGE
        header.i32(2, static_cast<int32_t>(pageBytes));     // uncompressed size
        header.i32(3, static_cast<int32_t>(pageBytes));     // compressed size
        header.open(5);
        header.i32(1, static_cast<int32_t>(rows));
        header.i32(2, encodingPlain);
        header.i32(3, encodingRle);
        header.i32(4, encodingRle);
        header.close();
        header.close();
        pending.put(levels.begin(), levels.size());
        char* values = pending.grow(valueBytes);
        group.chunks.push_back({start, written + pending.size() - start, rows});
        return values;
    }

    void close() {
        flush();
        Thrift meta(pending);
        meta.element();
        meta.i32(1, 1);
        meta.structList(2, columns.size() + 1);
        meta.element();
        meta.i32(3, 0);
        meta.text(4, "schema");
        meta.i32(5, static_cast<int32_t>(columns.size()));
        meta.close();
        for (const Column& column : columns) {
            meta.element();
            meta.i32(1, column.type);
            meta.i32(3, column.optional ? 1 : 0);
            meta.text(4, column.name);
            if (column.converted != NONE) {
                meta.i32(6, column.converted);
                meta.open(10);                              // LogicalType union
                meta.open(column.converted == UTF8 ? 1 : 6);   // STRING or DATE, both empty
                meta.close();
                meta.close();
            }
            meta.close();
        }
        meta.i64(3, static_cast<int64_t>(totalRows));
        meta.structList(4, groups.size());
        for (const Group& group : groups) {
            uint64_t bytes = 0;
            for (const Chunk& chunk : group.chunks) bytes += chunk.bytes;
            meta.element();
            meta.structList(1, group.chunks.size());
            for (size_t c = 0; c < group.chunks.size(); ++c) {
                const Chunk& chunk = group.chunks[c];
                meta.element();
                meta.i64(2, static_cast<int64_t>(chunk.offset));
                meta.open(3);
                meta.i32(1, columns[c].type);
                meta.i32List(2, {encodingPlain, encodingRle});
                meta.textList(3, columns[c].name);
                meta.i32(4, 0);                             // UNCOMPRESSED
                meta.i64(5, static_cast<int64_t>(chunk.values));
                meta.i64(6, static_cast<int64_t>(chunk.bytes));
                meta.i64(7, static_cast<int64_t>(chunk.bytes));
                meta.i64(9, static_cast<int64_t>(chunk.offset));
                meta.close();
                meta.close();
            }
            meta.i64(2, static_cast<int64_t>(bytes));
            meta.i64(3, static_cast<int64_t>(group.rows));
            meta.i64(5, static_cast<int64_t>(group.chunks.front().offset));
            meta.i64(6, static_cast<int64_t>(bytes));
            meta.close();
        }
        meta.text(6, "fraun shopping-sessions");
        meta.close();
        const uint32_t length = static_cast<uint32_t>(pending.size());
        pending.put(&length, 4);
        pending.put("PAR1", 4);
        flush();
        if (::close(fd) < 0) throw std::runtime_error("Cannot close output");
    }
};

template<class T, class Rows, class Field>
void fixed(ParquetFile& file, const Rows& rows, Field field) {
    char* out = file.chunk(rows.size() * sizeof(T));
    for (const auto& row : rows) {
        const T value = static_cast<T>(field(row));
        std::memcpy(out, &value, sizeof(T));
        out += sizeof(T);
    }
}

// PLAIN byte arrays: a four-byte length, then the bytes.
inline char* putText(char* out, const Key& key) {
    const auto text = key.view();
    const uint32_t size = static_cast<uint32_t>(text.size());
    std::memcpy(out, &size, 4);
    std::memcpy(out + 4, text.data(), text.size());
    return out + 4 + text.size();
}
template<class Rows, class Field>
void text(ParquetFile& file, const Rows& rows, Field field) {
    size_t bytes = 0;
    for (const auto& row : rows) bytes += 4 + field(row).view().size();
    char* out = file.chunk(bytes);
    for (const auto& row : rows) out = putText(out, field(row));
}

void writeUsers(ParquetFile& file, std::span<const UserRow> rows, const Context& ctx,
                std::vector<uint8_t>& present) {
    file.beginGroup(rows.size());
    text(file, rows, [](const UserRow& r) -> const Key& { return r.person; });
    fixed<int32_t>(file, rows, [](const UserRow& r) { return r.day; });
    fixed<int32_t>(file, rows, [](const UserRow& r) { return r.sessions; });
    fixed<int32_t>(file, rows, [](const UserRow& r) { return r.events; });
    fixed<int64_t>(file, rows, [](const UserRow& r) { return r.revenue; });
    present.resize(rows.size());
    size_t bytes = 0;
    for (size_t i = 0; i < rows.size(); ++i) {
        present[i] = rows[i].top != UINT32_MAX;
        if (present[i]) bytes += 4 + ctx.categories[rows[i].top].view().size();
    }
    char* out = file.chunk(bytes, present.data());
    for (const UserRow& row : rows)
        if (row.top != UINT32_MAX) out = putText(out, ctx.categories[row.top]);
}

struct CampaignRow { uint32_t id; int32_t day; CampaignStats stats; };
}

void writeOutputs(const fs::path& root, const Context& ctx, const std::vector<Result>& results, unsigned workers) {
    auto userDir = root / "user_daily", campaignDir = root / "campaign_daily";
    fs::create_directories(userDir);
    fs::create_directories(campaignDir);
    size_t files = std::max<size_t>(1, std::min<size_t>(workers, results.size()));
    parallel(files, workers, [&](size_t part) {
        ParquetFile file(userDir / ("part-" + std::to_string(part) + ".parquet"), {
            {"user_id", BYTE_ARRAY, UTF8}, {"day", INT32, DATE}, {"sessions", INT32}, {"events", INT32},
            {"revenue_cents", INT64}, {"top_category", BYTE_ARRAY, UTF8, true}});
        std::vector<uint8_t> present;
        for (size_t i = part; i < results.size(); i += files) {
            std::span<const UserRow> rows = results[i].users;
            for (const UserRow& row : rows)
                if (row.sessions > INT32_MAX || row.events > INT32_MAX)
                    throw std::runtime_error("Count exceeds 32 bits");
            while (!rows.empty()) {
                size_t batch = std::min<size_t>(131072, rows.size());
                writeUsers(file, rows.first(batch), ctx, present);
                rows = rows.subspan(batch);
            }
        }
        file.close();
    });
    std::unordered_map<int32_t, std::vector<CampaignStats>> totals;
    for (const auto& result : results) for (const auto& [day, stats] : result.byDay) {
        auto& daily = totals.try_emplace(day, stats.size()).first->second;
        for (size_t i = 0; i < stats.size(); ++i) {
            daily[i].sessions += stats[i].sessions;
            daily[i].purchasing += stats[i].purchasing;
            daily[i].revenue += stats[i].revenue;
            daily[i].assists += stats[i].assists;
        }
    }
    std::vector<CampaignRow> rows;
    for (const auto& [day, daily] : totals)
        for (uint32_t i = 0; i < daily.size(); ++i)
            if (daily[i].sessions || daily[i].assists) rows.push_back({i, day, daily[i]});

    ParquetFile file(campaignDir / "part-0.parquet", {
        {"campaign_id", BYTE_ARRAY, UTF8}, {"channel", BYTE_ARRAY, UTF8}, {"day", INT32, DATE},
        {"sessions", INT64}, {"purchasing_sessions", INT64}, {"revenue_cents", INT64},
        {"assisted_sessions", INT64}});
    if (!rows.empty()) {
        file.beginGroup(rows.size());
        text(file, rows, [&](const CampaignRow& r) -> const Key& { return ctx.campaignNames[r.id]; });
        text(file, rows, [&](const CampaignRow& r) -> const Key& { return ctx.channels[r.id]; });
        fixed<int32_t>(file, rows, [](const CampaignRow& r) { return r.day; });
        fixed<int64_t>(file, rows, [](const CampaignRow& r) { return r.stats.sessions; });
        fixed<int64_t>(file, rows, [](const CampaignRow& r) { return r.stats.purchasing; });
        fixed<int64_t>(file, rows, [](const CampaignRow& r) { return r.stats.revenue; });
        fixed<int64_t>(file, rows, [](const CampaignRow& r) { return r.stats.assists; });
    }
    file.close();
}
