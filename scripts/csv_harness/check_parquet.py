#!/usr/bin/env python3
"""Independent minimal Parquet reader (thrift compact, v1 PLAIN pages) used to check the
hand-written writer. Usage: pq.py OUT_DIR EXPECTED_DIR  -> compares both tables as sorted rows."""
import struct, sys, os, datetime, glob

class R:
    def __init__(s, b, p=0): s.b, s.p = b, p
    def byte(s): v = s.b[s.p]; s.p += 1; return v
    def varint(s):
        v = sh = 0
        while True:
            c = s.byte(); v |= (c & 0x7F) << sh; sh += 7
            if c < 0x80: return v
    def zz(s): v = s.varint(); return (v >> 1) ^ -(v & 1)
    def value(s, t):
        if t in (1, 2): return t == 1
        if t == 3: return s.byte()
        if t in (4, 5, 6): return s.zz()
        if t == 8: n = s.varint(); v = s.b[s.p:s.p+n]; s.p += n; return bytes(v)
        if t == 9:
            h = s.byte(); n = h >> 4; et = h & 15
            if n == 15: n = s.varint()
            return [s.value(et) for _ in range(n)]
        if t == 12: return s.struct()
        raise ValueError(f"type {t}")
    def struct(s):
        out = {}; last = 0
        while True:
            h = s.byte()
            if h == 0: return out
            d, t = h >> 4, h & 15
            fid = last + d if d else s.zz()
            last = fid
            out[fid] = s.value(t)

def read(path):
    b = open(path, 'rb').read()
    assert b[:4] == b'PAR1' and b[-4:] == b'PAR1', "magic"
    flen = struct.unpack('<I', b[-8:-4])[0]
    r = R(b, len(b) - 8 - flen); meta = r.struct()
    assert r.p == len(b) - 8, "footer length"
    assert meta[1] == 1
    schema = meta[2]; root = schema[0]; cols = schema[1:]
    assert root[5] == len(cols) and 1 not in root
    names = [c[4].decode() for c in cols]
    table = {n: [] for n in names}; total = 0
    for g in meta.get(4, []):
        assert len(g[1]) == len(cols)
        gb = 0
        for c, chunk in zip(cols, g[1]):
            m = chunk[3]
            assert m[1] == c[1] and m[4] == 0 and m[3] == [c[4]] and m[5] == g[3]
            pr = R(b, m[9]); ph = pr.struct()
            assert ph[1] == 0 and ph[2] == ph[3]
            dp = ph[5]; assert dp[1] == g[3] and dp[2] == 0
            assert pr.p - m[9] + ph[2] == m[7] == m[6], "chunk size"
            gb += m[7]
            p, end = pr.p, pr.p + ph[2]; n = dp[1]
            present = [True] * n
            if c[3] == 1:
                ln = struct.unpack_from('<I', b, p)[0]; lr = R(b, p + 4); p += 4 + ln
                present = []
                while lr.p < p and len(present) < n:
                    h = lr.varint()
                    if h & 1:
                        for _ in range(h >> 1):
                            x = lr.byte(); present += [bool(x >> k & 1) for k in range(8)]
                    else:
                        v = lr.byte(); present += [bool(v)] * (h >> 1)
                present = present[:n]; assert len(present) == n
            vals = []
            for ok in present:
                if not ok: vals.append(None); continue
                if c[1] == 1: v = struct.unpack_from('<i', b, p)[0]; p += 4
                elif c[1] == 2: v = struct.unpack_from('<q', b, p)[0]; p += 8
                elif c[1] == 6:
                    ln = struct.unpack_from('<I', b, p)[0]; v = b[p+4:p+4+ln].decode(); p += 4 + ln
                else: raise ValueError
                if c.get(6) == 6: v = str(datetime.date(1970, 1, 1) + datetime.timedelta(days=v))
                vals.append(v)
            assert p == end, f"page bytes {p} {end}"
            table[c[4].decode()] += vals
        assert gb == g[2]; total += g[3]
    assert total == meta[3]
    types = {c[4].decode(): (c[1], c.get(6), c[3]) for c in cols}
    return names, table, types

def rows_of(folder, table):
    out = []; names = None
    files = sorted(glob.glob(os.path.join(folder, table, '*.parquet')))
    assert files, "no parquet"
    for f in files:
        names, t, types = read(f)
        out += list(zip(*[t[n] for n in names])) if t[names[0]] else []
    return names, out, types

def main():
    out, exp = sys.argv[1:3]; ok = True
    for table in ('user_daily', 'campaign_daily'):
        names, rows, types = rows_of(out, table)
        lines = open(os.path.join(exp, table + '.csv')).read().splitlines()
        header = lines[0].split(',')
        if set(header) != set(names): print("columns differ", names, header); ok = False; continue
        assert types['day'] == (1, 6, 0), types
        idx = [names.index(h) for h in header]
        got = sorted(','.join('' if r[i] is None else str(r[i]) for i in idx) for r in rows)
        if got != sorted(lines[1:]): ok = False; print(table, "DIFFER", len(got), len(lines) - 1)
    sys.exit(0 if ok else 1)
main()
