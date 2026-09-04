"""合并多个个股缓存 SQLite 库到基准库，剔除重复键 (code, kind, asof)。

用于 GitHub Action：自动分析补齐本地缓存后，上传前再次拉取远程 cache 分支，
把远程新产生的数据合并进来；相同键保留 updated_at 较新的一份，避免旧数据覆盖新数据。
合并完成后对基准库做 wal_checkpoint(TRUNCATE)，确保数据全部落在主库文件，
随后压缩上传（避免 -wal 残留导致解压缺数据）。

用法：
  python merge_cache.py --base data/stock_cache.db --merge a.db --merge b.db
"""

import argparse
import sqlite3
import sys

_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS stock_cache("
    "code TEXT NOT NULL, kind TEXT NOT NULL, asof TEXT NOT NULL,"
    " payload TEXT NOT NULL, updated_at REAL NOT NULL,"
    " PRIMARY KEY(code, kind, asof))"
)


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
    base.execute(_SCHEMA)

    total_new = total_upd = total_dup = 0
    for src in args.merge:
        try:
            s = _conn(src)
            rows = s.execute(
                "SELECT code, kind, asof, payload, updated_at FROM stock_cache"
            ).fetchall()
            s.close()
        except sqlite3.Error as e:
            print(f"[merge] 跳过 {src}（打开失败：{e}）")
            continue
        if not rows:
            print(f"[merge] {src}：空库，跳过")
            continue
        new = upd = dup = 0
        for code, kind, asof, payload, updated_at in rows:
            cur = base.execute(
                "SELECT updated_at FROM stock_cache WHERE code=? AND kind=? AND asof=?",
                (code, kind, asof),
            ).fetchone()
            if cur is None:
                base.execute(
                    "INSERT INTO stock_cache(code, kind, asof, payload, updated_at) VALUES(?, ?, ?, ?, ?)",
                    (code, kind, asof, payload, updated_at),
                )
                new += 1
            elif updated_at > cur[0]:
                base.execute(
                    "UPDATE stock_cache SET payload=?, updated_at=? WHERE code=? AND kind=? AND asof=?",
                    (payload, updated_at, code, kind, asof),
                )
                upd += 1
            else:
                dup += 1
        base.commit()
        print(f"[merge] {src}：共 {len(rows)} 条 → 新增 {new}、更新 {upd}、重复剔除 {dup}")
        total_new += new
        total_upd += upd
        total_dup += dup

    # 确保所有数据落入主库文件，避免压缩时漏掉 -wal 中的提交
    base.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    total = base.execute("SELECT COUNT(*) FROM stock_cache").fetchone()[0]
    base.close()
    print(f"[merge] 合并完成：累计新增 {total_new}、更新 {total_upd}、剔除重复 {total_dup}，基准库现有 {total} 条")
    return 0


if __name__ == "__main__":
    sys.exit(main())
