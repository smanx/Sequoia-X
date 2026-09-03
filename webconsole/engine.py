"""数据引擎：SQLite 存储 + baostock 区间拉取。

复刻改造自原项目 sequoia_x/data/engine.py，新增能力：
- sync_range(start, end)：按任意日期区间拉取全市场日K，覆盖"获取/更新某一段数据"。
- get_ohlcv(symbol, as_of_date)：支持按指定日期截取，供策略回算历史任意交易日。

注意：默认复用原项目的 data/sequoia_v2.db，可用环境变量 DB_PATH 覆盖。
"""

import calendar
import os
import sqlite3
import threading
from datetime import date, timedelta  # noqa: F401  (timedelta 兼容原逻辑)
from pathlib import Path

import pandas as pd

logger = print  # 简化日志：默认打印到 stdout


def get_logger(_name: str):
    return logger


_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_daily (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol   TEXT    NOT NULL,
    date     TEXT    NOT NULL,
    open     REAL,
    high     REAL,
    low      REAL,
    close    REAL,
    volume   REAL,
    turnover REAL,
    UNIQUE (symbol, date)
);
"""

_CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_symbol_date ON stock_daily (symbol, date);
"""

# 股票名称表：名称映射本地持久化，分析时只读本地、不联网
_CREATE_NAME_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_names (
    symbol TEXT PRIMARY KEY,
    name   TEXT NOT NULL
);
"""


def _default_db_path() -> str:
    override = os.environ.get("DB_PATH")
    if override:
        return override
    return str(Path(__file__).resolve().parent.parent / "data" / "sequoia_v2.db")


def _bs_fetch_batch(tasks: list) -> list:
    """多进程 worker：独立 login，批量拉取 baostock 指定区间数据。"""
    import baostock as bs
    bs.login()
    results = []
    try:
        for symbol, bs_code, start, end in tasks:
            rs = bs.query_history_k_data_plus(
                bs_code,
                "date,open,high,low,close,volume,amount",
                start_date=start,
                end_date=end,
                frequency="d",
                adjustflag="1",  # 后复权
            )
            if rs.error_code != "0":
                continue
            while rs.next():
                results.append([symbol] + rs.get_row_data())
    finally:
        bs.logout()
    return results


class DataEngine:
    """行情数据引擎：SQLite 存储 + baostock 区间同步。"""

    def __init__(self, db_path: str | None = None) -> None:
        self.db_path: str = db_path or _default_db_path()
        self._init_db()

    def _init_db(self) -> None:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        with self._conn() as conn:
            # WAL 模式：多线程并发读不互相阻塞，显著提升并行分析性能（DB 级持久化）
            try:
                conn.execute("PRAGMA journal_mode=WAL")
            except sqlite3.Error:
                pass
            conn.execute(_CREATE_TABLE_SQL)
            conn.execute(_CREATE_INDEX_SQL)
            conn.execute(_CREATE_NAME_TABLE_SQL)
            conn.commit()
        print(f"数据库初始化完成：{self.db_path}")

    def _conn(self) -> sqlite3.Connection:
        """建连：加长 busy_timeout，让并发读写等待写锁而不立即报 database locked。"""
        conn = sqlite3.connect(self.db_path, timeout=20.0, check_same_thread=False)
        try:
            conn.execute("PRAGMA busy_timeout=20000")
        except sqlite3.Error:
            pass
        return conn

    # ── 查询 ──

    def get_db_info(self) -> dict:
        """返回数据库覆盖范围信息，用于前端初始化日期控件。"""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT COUNT(DISTINCT symbol), COUNT(*), MIN(date), MAX(date) FROM stock_daily"
            ).fetchone()
        return {
            "symbol_count": row[0] or 0,
            "row_count": row[1] or 0,
            "min_date": row[2] or "",
            "max_date": row[3] or "",
        }

    def get_local_symbols(self) -> list[str]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT DISTINCT symbol FROM stock_daily"
            ).fetchall()
        return [row[0] for row in rows]

    def has_trade_date(self, day: str) -> bool:
        """指定日期是否为可分析的交易日（该日期在库中有行情记录）。"""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT 1 FROM stock_daily WHERE date = ? LIMIT 1", (day,)
            ).fetchone()
        return row is not None

    def get_ohlcv(self, symbol: str, as_of_date: str | None = None) -> pd.DataFrame:
        """读取单只股票日K；as_of_date 非空时仅取 <= as_of_date 的数据（日期升序）。

        策略在某一天回算时，截断后取最后一根即为"该交易日"。
        """
        query = "SELECT * FROM stock_daily WHERE symbol = ?"
        params: tuple = (symbol,)
        if as_of_date:
            query += " AND date <= ?"
            params = (symbol, as_of_date)
        query += " ORDER BY date"
        with self._conn() as conn:
            df = pd.read_sql(query, conn, params=params)
        return df

    def get_ohlcv_all(self, as_of_date: str | None = None) -> pd.DataFrame:
        """读取全表日K（RPS 策略需要横向排位）；as_of_date 时仅取 <= as_of_date 的数据。"""
        query = "SELECT symbol, date, open, high, low, close, volume, turnover FROM stock_daily"
        params: tuple = ()
        if as_of_date:
            query += " WHERE date <= ?"
            params = (as_of_date,)
        with self._conn() as conn:
            df = pd.read_sql(query, conn, params=params)
        return df

    @staticmethod
    def _to_baostock_code(symbol: str) -> str:
        """纯数字代码转 baostock 格式：6/9开头 -> sh，其余 -> sz。"""
        prefix = "sh" if symbol.startswith(("6", "9")) else "sz"
        return f"{prefix}.{symbol}"

    # ── 区间同步 ──

    def get_all_symbols(self) -> list[str]:
        """通过 baostock 获取全市场 A 股代码列表。"""
        import baostock as bs

        lg = bs.login()
        if lg.error_code != "0":
            raise ConnectionError(f"baostock 登录失败: {lg.error_msg}")

        try:
            rs = bs.query_stock_basic(code_name="", code="")
            if rs.error_code != "0":
                raise RuntimeError(f"baostock 获取股票列表失败: {rs.error_msg}")
            symbols = []
            while rs.next():
                row = rs.get_row_data()
                code = row[0]           # "sh.600000" or "sz.000001"
                status = row[4]         # "1" = 上市
                stock_type = row[5]     # "1" = 股票
                if status == "1" and stock_type == "1":
                    symbols.append(code.split(".")[1])
            print(f"获取股票列表完成，共 {len(symbols)} 只")
            return symbols
        finally:
            bs.logout()

    def sync_range(self, start: str, end: str, cancel_event: threading.Event | None = None) -> dict:
        """多进程并行拉取全市场 [start, end] 区间日K（后复权），写入 SQLite。

        先删除该区间已有数据再写入，保证可重复执行、结果一致。
        Returns:
            {"count": 写入条数, "min_date", "max_date", "dates": 实际交易日列表}
        """
        from multiprocessing import Pool

        if start > end:
            raise ValueError("start 不能晚于 end")

        symbols = self.get_all_symbols()
        if not symbols:
            return {"count": 0, "min_date": "", "max_date": "", "dates": []}

        tasks = [(s, self._to_baostock_code(s), start, end) for s in symbols]
        n_workers = min(8, len(tasks))
        chunks = [tasks[i::n_workers] for i in range(n_workers)]

        print(f"拉取 {len(tasks)} 只股票 [{start} ~ {end}]，{n_workers} 进程并行...")
        with Pool(n_workers) as pool:
            if cancel_event is None:
                batch_results = pool.map(_bs_fetch_batch, chunks)
            else:
                # 支持取消：异步 map 并轮询取消标志，一旦取消立即 terminate 子进程；
                # 取消发生在写库(DELETE)之前，原区间已有数据会被完整保留。
                async_res = pool.map_async(_bs_fetch_batch, chunks)
                while not async_res.ready():
                    if cancel_event.is_set():
                        pool.terminate()
                        raise InterruptedError("更新已取消")
                    async_res.wait(timeout=0.3)
                if cancel_event.is_set():
                    pool.terminate()
                    raise InterruptedError("更新已取消")
                batch_results = async_res.get()

        all_rows = []
        for batch in batch_results:
            all_rows.extend(batch)

        if not all_rows:
            return {"count": 0, "min_date": "", "max_date": "", "dates": []}

        df = pd.DataFrame(all_rows, columns=["symbol", "date", "open", "high", "low", "close", "volume", "turnover"])
        for col in ["open", "high", "low", "close", "volume", "turnover"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["close"])
        df = df[df["volume"] > 0]

        count = len(df)
        dates = sorted(df["date"].unique().tolist())
        with self._conn() as conn:
            conn.execute("DELETE FROM stock_daily WHERE date >= ? AND date <= ?", (start, end))
            df.to_sql("stock_daily", conn, if_exists="append", index=False, method="multi", chunksize=500)
            conn.commit()

        print(f"sync_range: 写入 {count} 条，交易日 {dates[0]} ~ {dates[-1]}")
        return {
            "count": count,
            "min_date": dates[0],
            "max_date": dates[-1],
            "dates": dates[-30:],  # 返回最近若干交易日供前端参考
        }

    # ── 股票名称 ──

    _NAME_CACHE: dict[str, str] = {}

    def get_symbol_names(self, refresh: bool = False) -> dict[str, str]:
        """返回全市场 code -> 股票名称 映射。

        默认只读本地（内存缓存 -> SQLite stock_names 表），**不联网**；
        仅当 refresh=True（数据更新时）或本地无任何名称时才访问 baostock 并持久化。
        """
        if not refresh and self._NAME_CACHE:
            return self._NAME_CACHE

        if not refresh:
            # 读本地表，避免分析时联网
            with self._conn() as conn:
                rows = conn.execute("SELECT symbol, name FROM stock_names").fetchall()
            if rows:
                names = dict(rows)
                self._NAME_CACHE.clear()
                self._NAME_CACHE.update(names)
                return names

        return self._refresh_names_remote()

    def _refresh_names_remote(self) -> dict[str, str]:
        """从 baostock 拉取全市场名称并持久化到本地表，更新进程内缓存。"""
        import baostock as bs

        lg = bs.login()
        if lg.error_code != "0":
            print(f"baostock 登录失败: {lg.error_msg}")
            return dict(self._NAME_CACHE)

        try:
            rs = bs.query_stock_basic(code_name="", code="")
            if rs.error_code != "0":
                return dict(self._NAME_CACHE)
            fields = rs.fields  # 按列名定位，避免依赖返回顺序
            code_i = fields.index("code")
            name_i = fields.index("code_name")
            names: dict[str, str] = {}
            while rs.next():
                row = rs.get_row_data()
                code = row[code_i].split(".")[-1]
                name = row[name_i]
                if name:
                    names[code] = name
            if names:
                with self._conn() as conn:
                    conn.execute("DELETE FROM stock_names")
                    conn.executemany(
                        "INSERT INTO stock_names(symbol, name) VALUES(?, ?)",
                        list(names.items()),
                    )
                    conn.commit()
                self._NAME_CACHE.clear()
                self._NAME_CACHE.update(names)
            print(f"获取股票名称完成，共 {len(names)} 只")
            return names
        except Exception as exc:
            print(f"获取股票名称失败: {exc}")
            return dict(self._NAME_CACHE)
        finally:
            bs.logout()

    # ── 未来节点收益统计 ──

    # 节点定义：key=标识, label=显示名, step=相对分析日的第 N 个交易日, months=日历月偏移(仅日历档口用)
    _FUTURE_NODES = [
        ("T+1", 1, None),
        ("T+3", 3, None),
        ("T+5", 5, None),
        ("1个月", None, 1),
        ("3个月", None, 3),
        ("半年", None, 6),
        ("1年", None, 12),
    ]

    @staticmethod
    def _add_months(d: date, months: int) -> date:
        """对日期做整月偏移，处理跨年与月末溢出（如 1/31 +1月 -> 2/28）。"""
        y = d.year + (d.month - 1 + months) // 12
        m = (d.month - 1 + months) % 12 + 1
        day = min(d.day, calendar.monthrange(y, m)[1])
        return date(y, m, day)

    def future_returns(self, symbol: str, as_of: str) -> dict:
        """统计某股票在 as_of 日之后各时间节点的相对涨跌幅（%）。

        规则：
        - T+N：取分析日之后第 N 个【有数据的交易日】的 close。
        - 日历档口(1月/3月/半年/1年)：目标日历日 + 日；该日不是交易日则【往后顺延】找最近有数据的交易日。
        - 若分析日之后数据不够长，返回 None（前端显示"数据不足"）。
        涨跌基于后复权 close，可消除除权影响、口径可比。
        Returns: {节点key: 涨跌幅(百分比, 可为负) 或 None}
        """
        df = self.get_ohlcv(symbol)
        if df is None or df.empty:
            return {node[0]: None for node in self._FUTURE_NODES}

        df = df.sort_values("date").reset_index(drop=True)
        mask = (df["date"] == as_of)
        if not mask.any():
            return {node[0]: None for node in self._FUTURE_NODES}

        i0 = int(mask.idxmax())
        base = float(df.at[i0, "close"])
        if pd.isna(base) or base <= 0:
            return {node[0]: None for node in self._FUTURE_NODES}

        out: dict = {node[0]: None for node in self._FUTURE_NODES}
        dates = df["date"].tolist()

        # 交易日节点 T+N
        for node_key, step, _months in self._FUTURE_NODES:
            if step is None:
                continue
            j = i0 + step
            if j < len(df) and not pd.isna(df.at[j, "close"]):
                out[node_key] = (float(df.at[j, "close"]) - base) / base * 100

        # 日历档口：月初档口找 >= 目标日历日的第一个交易日（往后顺延）
        dy, dm, dd = (int(x) for x in as_of.split("-"))
        base_date = date(dy, dm, dd)
        for node_key, _step, months in self._FUTURE_NODES:
            if months is None:
                continue
            target = self._add_months(base_date, months).strftime("%Y-%m-%d")
            picked = [k for k, d in enumerate(dates) if d >= target]
            if picked:
                j = picked[0]
                if not pd.isna(df.at[j, "close"]):
                    out[node_key] = (float(df.at[j, "close"]) - base) / base * 100
        return out

    def trade_dates_in_range(self, start: str, end: str) -> list[str]:
        """返回 [start, end] 范围内所有有数据的交易日（去重升序）。

        休市/停市日自然不在列表中，范围分析据此逐日跳过。
        """
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT DISTINCT date FROM stock_daily WHERE date >= ? AND date <= ? ORDER BY date",
                (start, end),
            ).fetchall()
        return [row[0] for row in rows]

    def future_returns_for_dates(self, symbol: str, as_of_dates: list[str]) -> dict:
        """批量：一次读取全序列，对多个 as_of 日期分别计算 7 个未来节点收益。

        相比逐个调用 future_returns，只需一次全量读取，范围分析时大幅减少 SQL。
        Returns: {as_of: {节点key: 涨跌幅 或 None}}
        """
        df = self.get_ohlcv(symbol)
        if df is None or df.empty:
            return {d: {n[0]: None for n in self._FUTURE_NODES} for d in as_of_dates}

        df = df.sort_values("date").reset_index(drop=True)
        dates = df["date"].tolist()
        date_index = {d: i for i, d in enumerate(dates)}

        out: dict = {}
        for as_of in as_of_dates:
            i0 = date_index.get(as_of)
            res = {n[0]: None for n in self._FUTURE_NODES}
            if i0 is not None:
                base = float(df.at[i0, "close"])
                if not pd.isna(base) and base > 0:
                    for node_key, step, months in self._FUTURE_NODES:
                        if step is not None:  # T+N 交易日节点
                            j = i0 + step
                            if j < len(df) and not pd.isna(df.at[j, "close"]):
                                res[node_key] = (float(df.at[j, "close"]) - base) / base * 100
                        else:  # 日历档口：>= 目标日历日的第一个交易日（顺延）
                            dy, dm, dd = (int(x) for x in as_of.split("-"))
                            target = self._add_months(date(dy, dm, dd), months).strftime("%Y-%m-%d")
                            for k in range(i0 + 1, len(dates)):
                                if dates[k] >= target:
                                    if not pd.isna(df.at[k, "close"]):
                                        res[node_key] = (float(df.at[k, "close"]) - base) / base * 100
                                    break
            out[as_of] = res
        return out