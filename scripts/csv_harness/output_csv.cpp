// Test-only replacement for output.cpp: same tables as CSV, so the engine can be
// checked without Arrow. Not part of the image.
#include "shared.hpp"
#include <cstdio>
#include <ctime>
namespace fs = std::filesystem;
static std::string dayText(int32_t day) {
    time_t t = time_t(day) * 86400; struct tm tm; gmtime_r(&t, &tm);
    char buf[16]; strftime(buf, sizeof buf, "%Y-%m-%d", &tm); return buf;
}
void writeOutputs(const fs::path& root, const Context& ctx, const std::vector<Result>& results, unsigned) {
    fs::create_directories(root);
    FILE* u = fopen((root / "user_daily.csv").c_str(), "w");
    for (auto& r : results) for (auto& row : r.users) {
        auto p = row.person.view();
        std::string_view top = row.top == UINT32_MAX ? std::string_view{} : ctx.categories[row.top].view();
        fprintf(u, "%.*s,%s,%u,%u,%lld,%.*s\n", int(p.size()), p.data(), dayText(row.day).c_str(), row.sessions,
                row.events, (long long)row.revenue, int(top.size()), top.data());
    }
    fclose(u);
    std::unordered_map<int32_t, std::vector<CampaignStats>> totals;
    for (const auto& result : results) for (const auto& [day, stats] : result.byDay) {
        auto& d = totals.try_emplace(day, stats.size()).first->second;
        for (size_t i = 0; i < stats.size(); ++i) {
            d[i].sessions += stats[i].sessions; d[i].purchasing += stats[i].purchasing;
            d[i].revenue += stats[i].revenue; d[i].assists += stats[i].assists;
        }
    }
    FILE* c = fopen((root / "campaign_daily.csv").c_str(), "w");
    for (auto& [day, d] : totals) for (size_t i = 0; i < d.size(); ++i) if (d[i].sessions || d[i].assists) {
        auto n = ctx.campaignNames[i].view(), ch = ctx.channels[i].view();
        fprintf(c, "%.*s,%.*s,%s,%lld,%lld,%lld,%lld\n", int(n.size()), n.data(), int(ch.size()), ch.data(),
                dayText(day).c_str(), (long long)d[i].sessions, (long long)d[i].purchasing,
                (long long)d[i].revenue, (long long)d[i].assists);
    }
    fclose(c);
}
