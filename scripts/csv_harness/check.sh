#!/bin/sh
# Exact, order-insensitive check of a csv_harness binary against every fixture,
# and against the dev answer when a dev dataset is given.
# Usage: scripts/csv_harness/check.sh BIN [DEV_DIR]
bin=$1; fail=0; root=$(cd "$(dirname "$0")/../.." && pwd)
same() {
  for t in user_daily campaign_daily; do
    tail -n +2 "$2/$t.csv" | LC_ALL=C sort > /tmp/expected.$$; LC_ALL=C sort "$1/$t.csv" > /tmp/actual.$$
    cmp -s /tmp/expected.$$ /tmp/actual.$$ || return 1
  done
}
for d in "$root"/fixtures/*/; do
  o=$(mktemp -d)
  "$bin" "$d/input" "$o" && same "$o" "$d/expected" || { echo "FAIL $d"; fail=1; }
  rm -rf "$o"
done
if [ -n "${2:-}" ]; then
  o=$(mktemp -d)
  "$bin" "$2" "$o" && same "$o" "$root/tools/dev-answer" && echo "dev MATCH" || { echo "dev DIFFER"; fail=1; }
  rm -rf "$o"
fi
rm -f /tmp/expected.$$ /tmp/actual.$$
[ $fail = 0 ] && echo ALL-OK
exit $fail
