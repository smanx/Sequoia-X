"""合并多个个股缓存 SQLite 库到基准库，剔除重复键。

合并两张表：
- stock_cache   键 (code, kind, asof)：12 类个股明细数据
- analysis_cache 键 (asof)：每日分析选出的股票列表
相同键保留 updated_at 较新的一份，避免旧数据覆盖新数据。
合并完成后对基准库做 wal_checkpoint(TRUNCATE)，确保数据全部落在主库文件，
随后压缩上传（避免 -wal 残留导致解压缺数据）。

用法：
  python merge_cache.py --base data/stock_cache.db --merge a.db --merge b.db
"""

import argparse
import sqlite3
import sys

# 表名 -> (主键列, 数据列)
TABLES = {
    "stock_cache": {
        "pkey": "code,kind,asof",
        "sel": "code,kind,asof,payload,updated_at",
        "ins": "code,kind,asof,payload,updated_at",
        "params": lambda r: (r[0], r[1], r[2], r[3], r[4]),
        "wh": "code=? AND kind=? AND asof=?",
    },
    "analysis_cache": {
        "pkey": "asof",
        "sel": "asof,payload,updated_at",
        "ins": "asof,payload,updated_at",
        "params": lambda r: (r[0], r[1], r[2]),
        "wh": "asof=?",
    },
}

_SCHEMAS = {
    "stock_cache": (
        "CREATE TABLE IF NOT EXISTS stock_cache("
        "code TEXT NOT NULL, kind TEXT NOT NULL, asof TEXT NOT NULL,"
        " payload TEXT NOT NULL, updated_at REAL NOT NULL,"
        " PRIMARY KEY(code, kind, asof))"
    ),
    "analysis_cache": (
        "CREATE TABLE IF NOT EXISTS analysis_cache("
        "asof TEXT PRIMARY KEY, payload TEXT NOT NULL, updated_at REAL NOT NULL)"
    ),
}


def _conn(path: str) -> sqlite3.Connection:
    c = sqlite3.connect(path, timeout=20)
    c.execute("PRAGMA busy_timeout=20000")
    return c


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True, help="基准缓存库（本地，合并目标）")
    ap.add_argument("--merge", action="append", required=True, help="要合并进来的缓存库（可多次）")
    args = ap.parse_args()

    base = _conn(args.base)
    base.execute("PRAGMA journal_mode=WAL")
    for schema in _SCHEMAS.values():
        base.execute(schema)

    grand = {t: {"new": 0, "upd": 0, "dup": 0} for t in TABLES}
    for src in args.merge:
        try:
            s = _conn(src)
        except sqlite3.Error as e:
            print(f"[merge] 跳过 {src}（打开失败：{e}）")
            continue
        any_rows = False
        for t, spec in TABLES.items():
            try:
                rows = s.execute(f"SELECT {spec['sel']} FROM {t}").fetchall()
            except sqlite3.Error:
                rows = []  # 源库没有该表（旧版本），跳过
            if not rows:
                continue
            any_rows = True
            new = upd = dup = 0
            for r in rows:
                cur = base.execute(
                    f"SELECT updated_at FROM {t} WHERE {spec['wh']}", spec["params"](r)
                ).fetchone()
                if cur is None:
                    base.execute(f"INSERT INTO {t}({spec['ins']}) VALUES({','.join('?'*len(spec['ins'].split(',')))})", list(r))
                    new += 1
                elif r[-1] > cur[0]:
                    cols = spec["ins"].split(",")
                    upd_cols = [c for c in cols if c != "updated_at"]
                    set_clause = ", ".join(f"{c}=?" for c in upd_cols)
                    vals = [r[cols.index(c)] for c in upd_cols] + [r[-1]]
                    base.execute(
                        f"UPDATE {t} SET {set_clause}, updated_at=? WHERE {spec['wh']}",
                        vals + list(spec["params"](r)),
                    )
                    upd += 1
                else:
                    dup += 1
            grand[t]["new"] += new
            grand[t]["upd"] += upd
            grand[t]["dup"] += dup
            print(f"[merge] {t}@{src}：共 {len(rows)} 条 → 新增 {new}、更新 {upd}、重复剔除 {dup}")
        s.close()
        if not any_rows:
            print(f"[merge] {src}：空库，跳过")
        base.commit()

    base.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    total_s = base.execute("SELECT COUNT(*) FROM stock_cache").fetchone()[0]
    total_a = base.execute("SELECT COUNT(*) FROM analysis_cache").fetchone()[0]
    base.close()
    g = grand
    print(
        f"[merge] 合并完成：stock_cache 新增 {g['stock_cache']['new']}、更新 {g['stock_cache']['upd']}、"
        f"剔除 {g['stock_cache']['dup']}，现有 {total_s} 条；"
        f"analysis_cache 新增 {g['analysis_cache']['new']}、更新 {g['analysis_cache']['upd']}、"
        f"剔除 {g['analysis_cache']['dup']}，现有 {total_a} 条"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
