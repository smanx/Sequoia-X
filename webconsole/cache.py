"""个股 baostock 数据查询的本地缓存：进程内存 + SQLite 持久化（默认 7 天 TTL）。

设计要点：
1. 缓存键 = (baostock code, kind)；时间窗由后端固定（_QY_MAX），无需额外维度。
2. 双层：先查内存（零开销），未命中查 SQLite（跨重启 / 热重载仍命中），
   再未命中才走真实 baostock 请求。
3. TTL：读取时按 updated_at 判断，超过 ttl 秒视为未命中并自动重拉。
4. 缓存键 = (code, kind, as_of)：as_of 区分历史时点与最新数据，互不覆盖。
5. 并发：单一锁串行化内存与 SQLite 访问（WAL + busy_timeout，复用项目约定）。
"""

import json
import sqlite3
import threading
import time


class StockCache:
    def __init__(self, path: str, ttl: int = 7 * 86400) -> None:
        self.path = path
        self.ttl = ttl
        # 内存层：{(code, kind, as_of): {"_t": 写时间, "_d": 数据}}
        self._mem: dict[tuple[str, str, str], dict] = {}
        self._lock = threading.Lock()
        # SQLite 持久层：check_same_thread=False + WAL，由 self._lock 串行化访问
        self._conn = sqlite3.connect(path, timeout=20, check_same_thread=False)
        self._conn.execute("PRAGMA busy_timeout=20000")
        self._conn.execute("PRAGMA journal_mode=WAL")
        # 迁移：旧表无 asof 列（旧键只有 code+kind），重建即可（缓存可丢弃重建）
        cols = [r[1] for r in self._conn.execute("PRAGMA table_info(stock_cache)").fetchall()]
        if cols and "asof" not in cols:
            self._conn.execute("DROP TABLE stock_cache")
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS stock_cache("
            "code TEXT NOT NULL, kind TEXT NOT NULL, asof TEXT NOT NULL,"
            " payload TEXT NOT NULL, updated_at REAL NOT NULL,"
            " PRIMARY KEY(code, kind, asof))"
        )
        self._conn.commit()

    def get(self, code: str, kind: str, as_of: str = "latest"):
        """命中（未过期）返回 {fields, rows}，否则返回 None。"""
        key = (code, kind, as_of or "latest")
        now = time.time()
        with self._lock:
            hit = self._mem.get(key)
            if hit is not None:
                if now - hit["_t"] <= self.ttl:
                    return hit["_d"]
                self._mem.pop(key, None)
            row = self._conn.execute(
                "SELECT payload, updated_at FROM stock_cache WHERE code=? AND kind=? AND asof=?",
                (code, kind, as_of or "latest"),
            ).fetchone()
            if row is not None and now - row[1] <= self.ttl:
                data = json.loads(row[0])
                self._mem[key] = {"_t": row[1], "_d": data}
                return data
        return None

    def set(self, code: str, kind: str, as_of: str, data: dict) -> None:
        """写入缓存（覆盖旧值），同时更新内存与 SQLite。"""
        key = (code, kind, as_of or "latest")
        now = time.time()
        payload = json.dumps(data, ensure_ascii=False)
        with self._lock:
            self._mem[key] = {"_t": now, "_d": data}
            self._conn.execute(
                "INSERT INTO stock_cache(code, kind, asof, payload, updated_at) VALUES(?, ?, ?, ?, ?) "
                "ON CONFLICT(code, kind, asof) DO UPDATE SET "
                "payload=excluded.payload, updated_at=excluded.updated_at",
                (code, kind, as_of or "latest", payload, now),
            )
            self._conn.commit()

    def close(self) -> None:
        """关闭底层 SQLite 连接，释放资源（服务停止时调用）。"""
        with self._lock:
            self._conn.close()