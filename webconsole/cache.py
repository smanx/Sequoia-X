"""个股 baostock 数据查询的本地缓存：进程内存 + SQLite 持久化（缓存永久有效，不过期）。

设计要点：
1. 缓存键 = (baostock code, kind)；时间窗由后端固定（_QY_MAX），无需额外维度。
2. 双层：先查内存（零开销），未命中查 SQLite（跨重启 / 热重载仍命中），
   再未命中才走真实 baostock 请求。
3. 永久有效：只按键命中即返回，不做时间过期判断（updated_at 仍写入，供跨库合并使用）。
4. 缓存键 = (code, kind, as_of)：as_of 区分历史时点与最新数据，互不覆盖。
5. 并发：单一锁串行化内存与 SQLite 访问（非 WAL 的 TRUNCATE 日志 + busy_timeout）。
   本进程内由 self._lock 串行访问，无需 WAL；改用非 WAL 还避免服务器容器/网络盘
   上 -wal/-shm 不稳定造成的“database disk image is malformed”，且主文件始终为最新，
   复制/上传缓存库更安全。
6. 写损坏自愈：写库遇 malformed 时，把损坏文件移到一边重建全新空库并重试（缓存可安全重建）。
"""

import json
import os
import sqlite3
import threading
import time


class StockCache:
    def __init__(self, path: str) -> None:
        self.path = path
        # 内存层：{(code, kind, as_of): {"_t": 写入时间, "_d": 数据}}
        self._mem: dict[tuple[str, str, str], dict] = {}
        self._lock = threading.Lock()
        # SQLite 持久层：check_same_thread=False，由 self._lock 单进程串行化访问。
        # 用非 WAL 日志(TRUNCATE)，避免服务器容器/磁盘上 -wal/-shm 不稳引发“malformed”。
        self._conn = None
        try:
            self._conn = self._connect()
            self._init_schema(self._conn)
        except sqlite3.DatabaseError:
            # 打开/建表时即发现缓存库损坏（含“file is not a database”）：自愈重建
            self._rebuild()

    def get(self, code: str, kind: str, as_of: str = "latest"):
        """命中即返回 {fields, rows}（缓存永久有效，不过期），否则返回 None。"""
        key = (code, kind, as_of or "latest")
        with self._lock:
            hit = self._mem.get(key)
            if hit is not None:
                return hit["_d"]
            row = self._conn.execute(
                "SELECT payload FROM stock_cache WHERE code=? AND kind=? AND asof=?",
                (code, kind, as_of or "latest"),
            ).fetchone()
            if row is not None:
                data = json.loads(row[0])
                self._mem[key] = {"_t": time.time(), "_d": data}
                return data
        return None

    def _connect(self):
        conn = sqlite3.connect(self.path, timeout=20, check_same_thread=False)
        try:
            conn.execute("PRAGMA busy_timeout=20000")
            # 非 WAL：避免 -wal/-shm 在服务器(容器/网络盘)上不稳导致“database disk image is malformed”
            conn.execute("PRAGMA journal_mode=TRUNCATE")
            return conn
        except Exception:  # noqa: BLE001
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass
            raise

    def _init_schema(self, conn) -> None:
        # 迁移：旧表无 asof 列（旧键只有 code+kind），重建即可（缓存可丢弃重建）
        cols = [r[1] for r in conn.execute("PRAGMA table_info(stock_cache)").fetchall()]
        if cols and "asof" not in cols:
            conn.execute("DROP TABLE stock_cache")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS stock_cache("
            "code TEXT NOT NULL, kind TEXT NOT NULL, asof TEXT NOT NULL,"
            " payload TEXT NOT NULL, updated_at REAL NOT NULL,"
            " PRIMARY KEY(code, kind, asof))"
        )
        conn.commit()

    def _rebuild(self) -> None:
        """缓存库写损坏自愈：将损坏文件移走并重建全新空库（缓存可安全重建，不丢任何逻辑数据）。"""
        try:
            try:
                if self._conn is not None:
                    self._conn.close()
            except Exception:  # noqa: BLE001
                pass
            # 清掉遗留 wal/shm（若文件系统/权限允许），以免带病被重新使用
            for suffix in ("-wal", "-shm"):
                try:
                    os.remove(self.path + suffix)
                except OSError:
                    pass
            # 先把损坏文件改名保存，留待排查/恢复；再创建全新空库
            os.replace(self.path, self.path + f".corrupt_{int(time.time())}")
        except Exception:  # noqa: BLE001
            pass
        self._conn = self._connect()
        self._init_schema(self._conn)

    def _write(self, code: str, kind: str, as_of: str, payload: str, now: float) -> None:
        self._conn.execute(
            "INSERT INTO stock_cache(code, kind, asof, payload, updated_at) VALUES(?, ?, ?, ?, ?) "
            "ON CONFLICT(code, kind, asof) DO UPDATE SET "
            "payload=excluded.payload, updated_at=excluded.updated_at",
            (code, kind, as_of or "latest", payload, now),
        )
        self._conn.commit()

    def set(self, code: str, kind: str, as_of: str, data: dict) -> None:
        """写入缓存（覆盖旧值），同时更新内存与 SQLite；遇到库损坏自动重建后重试一次。"""
        key = (code, kind, as_of or "latest")
        now = time.time()
        payload = json.dumps(data, ensure_ascii=False)
        with self._lock:
            self._mem[key] = {"_t": now, "_d": data}
            try:
                self._write(code, kind, as_of, payload, now)
            except sqlite3.DatabaseError:
                # 库损坏（malformed）：自愈重建一次再写；仍失败则由上层捕获记录并跳过该条
                self._rebuild()
                self._write(code, kind, as_of, payload, now)

    def close(self) -> None:
        """关闭底层 SQLite 连接，释放资源（服务停止时调用）。"""
        with self._lock:
            self._conn.close()