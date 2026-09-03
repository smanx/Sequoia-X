"""Sequoia-X Web 控制台：标准库 http.server 实现，零新增依赖。

功能：
  GET  /                    → 前端页面
  GET  /api/info            → 数据库覆盖范围（min/max 日期、股票数）
  POST /api/data/update     → {start, end} 拉取/更新该区间数据
  POST /api/analyze         → {date} 策略分析该交易日，返回各策略选股

启动：
  python app.py [port]      # 默认 8000，浏览器访问 http://127.0.0.1:8000
                             # 端口也可用环境变量 SEQUOIA_PORT 指定
                             # 监听地址可用环境变量 SEQUOIA_HOST 覆盖（Docker 内设为 0.0.0.0）
                             # 优先级：命令行参数 > SEQUOIA_PORT 环境变量 > 默认 8000
"""

import base64
import hmac
import importlib
import json
import logging
import os
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

# 分析中逐股票计算未来收益的并行线程数
_FUT_WORKERS = 8

# 线程模式：multi=多线程（按天并行），single=纯串行
# 说明：策略评估已向量化（全表指标一次计算、按日毫秒级评估），耗时主体变为
#       "预加载全表 + 指标一次计算"；按天并行受 Python GIL 限制收益甚微，
#       保留下拉切换仅用于对比验证两模式结果一致。
_THREAD_MODE = "multi"
_DAY_WORKERS = 2        # 范围分析中并行处理的交易日数

# 用「模块引用」方式导入（而非 from ... import 类），以支持热重载
import engine as engine_mod
import strategies as strategies_mod
import cache as cache_mod

logger = logging.getLogger("sequoia-webconsole")
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

# ── 数据源：默认本地，可切换为在线（下载 GitHub 打包的分卷数据库解压得到） ──
ONLINE_ZIP_URL = "https://github.com/smanx/Sequoia-X/archive/refs/heads/data.zip"
# 在线数据源落盘路径：解压出的 db 单独存放，避免覆盖本地默认库
ONLINE_DB_PATH = str((BASE_DIR.parent / "data" / "sequoia_online_v2.db").resolve())
_DATASOURCE = {"source": "local"}

# ── Web Basic 认证：默认 admin/admin，可用环境变量 SEQUOIA_USER / SEQUOIA_PASS 修改 ──
AUTH_USER = os.environ.get("SEQUOIA_USER", "admin")
AUTH_PASS = os.environ.get("SEQUOIA_PASS", "admin")
# 认证失败时浏览器弹出的提示标题
AUTH_REALM = "Sequoia-X"


def _auth_creds():
    """返回当前生效的 Web 认证账号密码（环境变量在启动时已读入）。"""
    return AUTH_USER, AUTH_PASS

# 惰性初始化，避免 Windows 下 multiprocessing 重导入主模块时重复建连
_engine = None
_strategies: dict | None = None

# ── 分析取消机制：跨请求的线程安全标志，供分析线程在检查点及时退出 ──
_CANCEL = threading.Event()


class AnalysisCanceled(Exception):
    """分析被用户主动取消时抛出，接口捕获后返回 canceled 状态。"""


def check_cancel() -> None:
    """在分析循环的检查点调用；已取消则抛出，让后台真正停止后续分析。"""
    if _CANCEL.is_set():
        raise AnalysisCanceled("分析已取消")


# 数据更新取消：与 _CANCEL(分析) 相互独立，运行数据更新请求时传入 sync_range
_UPDATE_CANCEL = threading.Event()

# ── 自动热重载：检测业务模块文件变化，改动后下一次请求自动用新代码 ──
_RELOAD_MODULES: dict[str, object] = {"engine": engine_mod, "strategies": strategies_mod}
_MTIMES: dict[str, int] = {}


def _is_loaded() -> bool:
    return bool(_MTIMES)


def _snapshot_mtime() -> dict[str, int]:
    snap: dict[str, int] = {}
    for name, mod in _RELOAD_MODULES.items():
        path = Path(getattr(mod, "__file__", "")).resolve()
        snap[name] = path.stat().st_mtime_ns if path.is_file() else 0
    return snap


def _maybe_reload() -> None:
    """请求前调用：若业务模块代码已变化，则热重载并重建引擎/策略实例。

    只对 engine.py / strategies.py 生效；app.py 自身的路由改动仍建议重启。
    """
    global _engine, _strategies
    current = _snapshot_mtime()
    if current == _MTIMES:
        return
    if not _is_loaded():
        _MTIMES.update(current)
        return
    for name in _RELOAD_MODULES:
        if current.get(name) == _MTIMES.get(name):
            continue
        importlib.invalidate_caches()
        importlib.reload(_RELOAD_MODULES[name])
        print(f"[reload] 检测到 {name}.py 变化，已自动热重载")
    _MTIMES.update(current)
    # 重建引擎与策略实例（数据库连接/策略都以最新代码重新创建）
    _engine = None
    _strategies = None
    print("[reload] 引擎与策略实例已按最新代码重建")


def get_engine():
    global _engine
    if _engine is None:
        db_path = ONLINE_DB_PATH if _DATASOURCE["source"] == "online" else engine_mod._default_db_path()
        _engine = engine_mod.DataEngine(db_path)
    return _engine


def set_datasource(source: str | None) -> str:
    """切换数据源：local=本地默认库，online=在线下载库。变化时重建引擎连接。"""
    global _engine
    source = source if source in ("local", "online") else "local"
    if source != _DATASOURCE["source"]:
        _DATASOURCE["source"] = source
        _engine = None  # 下次 get_engine 用新库重建连接
    return source


def online_ready() -> bool:
    """在线数据源是否已获取：在线库文件存在且 stock_daily 表有数据。

    注意：切到在线源但未获取时，get_engine 会按路径创建一个空库文件（建表），
    因此不能仅凭文件存在判断，必须以"是否有实际行情数据"为准。
    """
    if not os.path.exists(ONLINE_DB_PATH):
        return False
    try:
        conn = sqlite3.connect(ONLINE_DB_PATH)
        n = conn.execute("SELECT COUNT(*) FROM stock_daily").fetchone()[0]
        conn.close()
        return n > 0
    except Exception:  # noqa: BLE001  (表不存在/损坏等一律视为未获取)
        return False


def get_engine():
    global _engine
    if _engine is None:
        db_path = ONLINE_DB_PATH if _DATASOURCE["source"] == "online" else engine_mod._default_db_path()
        _engine = engine_mod.DataEngine(db_path)
    return _engine


def _download_stream(url: str, dest: str, chunk: int = 1024 * 256) -> None:
    """流式下载大文件到本地路径，避免一次性载入内存。"""
    import urllib.request

    with urllib.request.urlopen(url, timeout=60) as resp, open(dest, "wb") as f:
        while True:
            buf = resp.read(chunk)
            if not buf:
                break
            f.write(buf)


def fetch_online_source() -> str:
    """获取在线数据源：下载 master.zip -> 解压 -> 拼接分卷 -> 解 tar.gz -> 取得 db。

    分卷命名参考 .github/workflows/fetch-data.yml：
      打包: tar -czf - -C . data | split -b 90m - sequoia_v2.tar.gz.
      解包: 拼接所有 data/sequoia_v2.tar.gz.* 分卷 → tar.gz → 解出 data/sequoia_v2.db
    Returns: 在线数据源 db 的落盘路径。
    """
    import glob
    import shutil
    import tarfile
    import tempfile
    import zipfile

    tmp = tempfile.mkdtemp(prefix="seq_online_")
    try:
        zip_path = os.path.join(tmp, "master.zip")
        _download_stream(ONLINE_ZIP_URL, zip_path)

        with zipfile.ZipFile(zip_path) as z:
            top = z.namelist()[0].split("/")[0] if z.namelist() else ""
            z.extractall(tmp)

        src_data = os.path.join(tmp, top, "data")
        parts = sorted(glob.glob(os.path.join(src_data, "sequoia_v2.tar.gz.*")))
        if not parts:
            # 兼容无分卷的情况：单文件 tar.gz 或直接裸 db
            single = os.path.join(src_data, "sequoia_v2.tar.gz")
            direct_db = os.path.join(src_data, "sequoia_v2.db")
            if os.path.isfile(single):
                parts = [single]
            elif os.path.isfile(direct_db):
                os.makedirs(os.path.dirname(ONLINE_DB_PATH), exist_ok=True)
                shutil.copy(direct_db, ONLINE_DB_PATH)
                return ONLINE_DB_PATH
            else:
                raise FileNotFoundError("在线源码包 data 目录未找到数据库分卷(sequoia_v2.tar.gz.*)")

        # 拼接分卷成完整 tar.gz
        tar_path = os.path.join(tmp, "sequoia_v2.tar.gz")
        with open(tar_path, "wb") as out:
            for p in parts:
                with open(p, "rb") as f:
                    shutil.copyfileobj(f, out)

        # 解 tar.gz（内部是相对 . 的 data/ 目录）
        with tarfile.open(tar_path, "r:gz") as t:
            t.extractall(tmp)
        db = os.path.join(tmp, "data", "sequoia_v2.db")
        if not os.path.isfile(db):
            db = os.path.join(src_data, "sequoia_v2.db")
        if not os.path.isfile(db):
            raise FileNotFoundError("解压后未找到 sequoia_v2.db")

        os.makedirs(os.path.dirname(ONLINE_DB_PATH), exist_ok=True)
        shutil.copy(db, ONLINE_DB_PATH)
        return ONLINE_DB_PATH
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def get_strategies() -> dict:
    global _strategies
    if _strategies is None:
        # 策略评估已向量化，只需元数据（key/name/desc）
        _strategies = {m["key"]: m for m in strategies_mod.STRATEGY_META}
    return _strategies


def set_thread_mode(mode: str) -> str:
    """设置分析线程模式（single/multi）。"""
    global _THREAD_MODE
    mode = mode if mode in ("single", "multi") else "multi"
    _THREAD_MODE = mode
    return _THREAD_MODE


def analyze_day(as_of_date: str, keys: list[str] | None = None, ind_df=None) -> tuple[dict, set]:
    """对指定日期运行全部（或 keys 指定的）策略。

    ind_df 为 prepare() 后的全表（须覆盖 as_of 之前足够历史）；不传时按 as_of 现读。
    Returns: (results, 命中代码集合)
    """
    if ind_df is None:
        raw = get_engine().get_ohlcv_all(as_of_date)
        ind_df = strategies_mod.prepare(raw)
    results = strategies_mod.evaluate_day(ind_df, as_of_date, keys)
    all_codes = {s for r in results.values() for s in r.get("symbols", [])}
    return results, all_codes


def _merge_day(results: dict, hit_days_by_code: dict, day: str, day_results: dict) -> None:
    """把某一天的各策略结果并入综合结果容器，并累加该日统计（分析条数/不符原因）。

    bucket["stats"] 跨天累加 total / matched / reasons，供范围分析显示"分析多少条、不符原因分布"。
    """
    for k, r in day_results.items():
        bucket = results.setdefault(k, {
            "name": r["name"], "desc": r["desc"], "days": [], "count": 0,
            "stats": {"total": 0, "matched": 0, "reasons": {}},
        })
        bucket["days"].append({"date": day, "count": r["count"], "symbols": r.get("symbols", [])})
        bucket["count"] += r["count"]
        dp = r.get("stats") or {}
        st = bucket["stats"]
        st["total"] += dp.get("total", 0)
        st["matched"] += dp.get("matched", len(r.get("symbols", [])))
        for reason, cnt in (dp.get("reasons") or {}).items():
            st["reasons"][reason] = st["reasons"].get(reason, 0) + int(cnt)
        for s in r.get("symbols", []):
            hit_days_by_code.setdefault(s, set()).add(day)


def _bs_code(code: str) -> str:
    """纯数字代码或带 sh./sz. 前缀 -> 标准 baostock 代码（如 sh.600000）。"""
    code = code.strip()
    if code.startswith(("sh.", "sz.")):
        return code
    return engine_mod.DataEngine._to_baostock_code(code)


# 个股详情页支持的数据种类（key -> 中文标题）
STOCK_KINDS: list[dict] = [
    {"key": "dividend", "title": "除权除息信息"},
    {"key": "adjust_factor", "title": "复权因子信息"},
    {"key": "qfq", "title": "本地计算前复权"},
    {"key": "profit", "title": "季频盈利能力"},
    {"key": "operation", "title": "季频营运能力"},
    {"key": "growth", "title": "季频成长能力"},
    {"key": "balance", "title": "季频偿债能力"},
    {"key": "cashflow", "title": "季频现金流量"},
    {"key": "dupont", "title": "季频杜邦指数"},
    {"key": "express", "title": "季频业绩快报"},
    {"key": "forecast", "title": "季频业绩预告"},
    {"key": "basic", "title": "证券基本资料"},
]
STOCK_KINDS_MAP = {k["key"]: k["title"] for k in STOCK_KINDS}

# 季频/区间查询回溯的历史年数、单次返回的最大行数
_QY_MAX = 12
_RS_LIMIT = 250

# 个股数据查询的本地缓存：默认 7 天 TTL，可用环境变量 STOCK_CACHE_TTL（秒）覆盖
_CACHE_TTL = int(os.environ.get("STOCK_CACHE_TTL", str(7 * 86400)))
_STOCK_CACHE = None  # 惰性初始化的 StockCache 实例

# baostock 单一全局会话：只登录一次复用，不每次 login/logout。
# 所有 baostock 会话级操作（登录/查询）用同一把锁串行化，避免多线程并发串包。
_BS_LOCK = threading.Lock()
_BS_LOGGED_IN = False


def _collect_rs(rs):
    """把 baostock 结果集收成 (fields, rows)。字段用返回结果动态命名，不硬编码。"""
    fields = list(rs.fields)
    rows = []
    while rs.error_code == "0" and rs.next():
        rows.append(rs.get_row_data())
    return fields, rows


def _today() -> str:
    return time.strftime("%Y-%m-%d")


def _asof_info(as_of: str | None) -> tuple[int, str]:
    """按 as_of（数据截止日期）返回 (截止年份, 截止日期)。as_of 为空则用今天。"""
    d = (as_of or "").strip() or _today()
    return int(d[:4]), d


def _year_range(asof_year: int | None = None) -> range:
    """回溯 _QY_MAX 年到指定年份（默认当前年）。"""
    y0 = asof_year if asof_year is not None else int(time.strftime("%Y"))
    return range(y0 - _QY_MAX + 1, y0 + 1)


def _query_kind(bs, bscode: str, kind: str, as_of: str | None = None) -> tuple[list, list]:
    """执行某一种类的查询，返回 (fields, rows)。kind 必须是 STOCK_KINDS_MAP 的 key。

    as_of 为数据截止日期：季频/除权回溯到该年，区间类查询到该截止日。
    传入 as_of 可查看某个历史时点的数据（与分析日期对齐）；不传则取到今天。
    """
    asof_year, asof_date = _asof_info(as_of)
    if kind in ("profit", "operation", "growth", "balance", "cashflow",
                "dupont"):
        # 季频类：按 (年, 季度) 循环，跨年合并
        fn = {
            "profit": bs.query_profit_data,
            "operation": bs.query_operation_data,
            "growth": bs.query_growth_data,
            "balance": bs.query_balance_data,
            "cashflow": bs.query_cash_flow_data,
            "dupont": bs.query_dupont_data,
        }[kind]
        all_fields, all_rows = None, []
        for year in _year_range(asof_year):
            for quarter in range(1, 5):
                rs = fn(code=bscode, year=str(year), quarter=str(quarter))
                if rs.error_code != "0":
                    continue  # 该期无披露则跳过，不当作报错
                fields, rows = _collect_rs(rs)
                all_fields = fields or all_fields
                all_rows.extend(rows)
        return all_fields or [], all_rows

    if kind in ("express", "forecast"):
        # 业绩快报 / 业绩预告：按"发布日期范围"查询，不传 year/quarter
        fn = {
            "express": bs.query_performance_express_report,
            "forecast": bs.query_forecast_report,
        }[kind]
        rs = fn(bscode, start_date=f"{min(_year_range(asof_year))}-01-01", end_date=asof_date)
        fields, rows = _collect_rs(rs)
        return fields, rows[-_RS_LIMIT:]

    if kind == "dividend":
        # 除权除息：按"实际除权除息年份"逐年查询
        all_fields, all_rows = None, []
        for year in _year_range(asof_year):
            rs = bs.query_dividend_data(code=bscode, year=str(year), yearType="operate")
            if rs.error_code != "0":
                continue
            fields, rows = _collect_rs(rs)
            all_fields = fields or all_fields
            all_rows.extend(rows)
        return all_fields or [], all_rows

    if kind == "adjust_factor":
        # 复权因子
        rs = bs.query_adjust_factor(bscode, f"{min(_year_range(asof_year))}-01-01", asof_date)
        fields, rows = _collect_rs(rs)
        return fields, rows[-_RS_LIMIT:]

    if kind == "basic":
        # 证券基本资料
        rs = bs.query_stock_basic(code_name="", code=bscode)
        fields, rows = _collect_rs(rs)
        return fields, rows

    if kind == "qfq":
        # 本地计算前复权：不复权收盘价 × 前复权因子 = 前复权价
        start = f"{min(_year_range(asof_year))}-01-01"
        rs_k = bs.query_history_k_data_plus(
            bscode, "date,close", start_date=start, end_date=asof_date,
            frequency="d", adjustflag="3",
        )
        _, krows = _collect_rs(rs_k)
        rs_f = bs.query_adjust_factor(bscode, start, asof_date)
        ff_fields, frows = _collect_rs(rs_f)
        # 用字段名定位前复权因子列，避免顺序依赖
        pos_f = ff_fields.index("foreAdjustFactor")
        fact = {r[0]: float(r[pos_f]) for r in frows if r[pos_f] not in ("", None)}
        out = []
        for r in krows[-_RS_LIMIT:]:
            close = r[1]
            fac = fact.get(r[0])
            qfq = ""
            try:
                if fac is not None and close not in ("", None):
                    qfq = round(float(close) * fac, 4)
            except Exception:  # noqa: BLE001
                qfq = ""
            out.append([r[0], close, "" if fac is None else fac, qfq])
        return ["date", "close", "foreAdjustFactor", "qfqClose"], out

    raise ValueError(f"未知数据种类: {kind}")


def _get_stock_cache() -> cache_mod.StockCache:
    """惰性创建个股数据缓存（独立缓存库，不影响主行情库）。"""
    global _STOCK_CACHE
    if _STOCK_CACHE is None:
        path = str((BASE_DIR.parent / "data" / "stock_cache.db").resolve())
        _STOCK_CACHE = cache_mod.StockCache(path, ttl=_CACHE_TTL)
    return _STOCK_CACHE


def _ensure_bs_login():
    """确保 baostock 已登录。复用单一全局会话，成功或首次 login 后不再每次 logout。"""
    global _BS_LOGGED_IN
    import baostock as bs
    if _BS_LOGGED_IN:
        return bs
    lg = bs.login()
    if lg.error_code != "0":
        _BS_LOGGED_IN = False
        raise ConnectionError(f"baostock 登录失败: {lg.error_msg}")
    _BS_LOGGED_IN = True
    return bs


def _bs_query(bscode: str, kind: str, as_of: str | None = None) -> tuple[list, list]:
    """在全局锁内查询 baostock（只保证一次登录并复用）。查询因会话失效异常时自动重登重试一次。"""
    global _BS_LOGGED_IN
    with _BS_LOCK:
        bs = _ensure_bs_login()
        try:
            return _query_kind(bs, bscode, kind, as_of)
        except Exception:
            # 可能是长连接会话/网络失效：清除登录态，重登一次后重试
            _BS_LOGGED_IN = False
            bs = _ensure_bs_login()
            return _query_kind(bs, bscode, kind, as_of)


def query_stock_data(code: str, kind: str, as_of: str | None = None) -> dict:
    """查询个股 baostock 数据并本地缓存，返回 {"fields": [...], "rows": [[...], ...]}。

    as_of 为数据截止日期（可选）：传入后可查看/缓存该历史时点的数据；
    为 None 时取到今天。缓存键含 as_of，不同时点数据互不覆盖。

    查询顺序：内存缓存 → SQLite 缓存 → 真实 baostock 请求（回填两层缓存）。
    """
    if kind not in STOCK_KINDS_MAP:
        raise ValueError(f"未知数据种类: {kind}")
    bscode = _bs_code(code)
    cache = _get_stock_cache()
    as_of_key = (as_of or "").strip() or "latest"
    cached = cache.get(bscode, kind, as_of_key)
    if cached is not None:
        return cached
    fields, rows = _bs_query(bscode, kind, as_of)
    if not fields:
        raise RuntimeError("该数据种类无返回数据（可能该股无此类信息）")
    result = {"fields": fields, "rows": rows}
    cache.set(bscode, kind, as_of_key, result)
    return result


class Handler(BaseHTTPRequestHandler):
    server_version = "SequoiaXWeb/1.0"

    # ── 通用响应 ──
    def _send(self, status: int, body, ctype: str = "application/json") -> None:
        if not isinstance(body, (bytes, str)):
            body = json.dumps(body, ensure_ascii=False)
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", f"{ctype}; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        # 客户端可能在长请求处理期间主动断开（刷新/停止/超时）。
        # 此时端头刷屏和正文写入会抛连接类异常；属于正常弃连，静默处理，避免 traceback 刷屏，
        # 也不再让 socketserver 打印 handler 崩溃。
        try:
            self.end_headers()
            self.wfile.write(body)
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, OSError):
            self.close_connection = True

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length", 0) or 0)
        raw = self.rfile.read(length) if length else b"{}"
        try:
            return json.loads(raw or b"{}")
        except json.JSONDecodeError:
            return {}

    # ── Web Basic 认证：校验 Authorization 头，失败返回 401 ──
    def _check_auth(self) -> bool:
        user, pwd = _auth_creds()
        header = self.headers.get("Authorization", "")
        if not header.startswith("Basic "):
            return False
        try:
            decoded = base64.b64decode(header[6:].strip()).decode("utf-8")
        except Exception:  # noqa: BLE001
            return False
        given_user, _, given_pwd = decoded.partition(":")
        return hmac.compare_digest(given_user, user) and hmac.compare_digest(given_pwd, pwd)

    def _require_auth(self) -> bool:
        if self._check_auth():
            return True
        self.send_response(401)
        self.send_header("WWW-Authenticate", f'Basic realm="{AUTH_REALM}"')
        self.send_header("Content-Length", "0")
        self.end_headers()
        return False

    def _serve_static(self, name: str, ctype: str) -> None:
        path = STATIC_DIR / name
        if not path.is_file():
            self._send(404, {"error": "not found"}, "text/plain")
            return
        self._send(200, path.read_bytes(), ctype)

    # ── 路由 ──
    def do_GET(self) -> None:
        if not self._require_auth():
            return
        _maybe_reload()
        path = urlparse(self.path).path
        if path in ("/", "/index.html"):
            self._serve_static("index.html", "text/html")
        elif path == "/stock.html":
            self._serve_static("stock.html", "text/html")
        elif path == "/api/info":
            try:
                self._send(200, get_engine().get_db_info())
            except Exception as exc:  # noqa: BLE001
                self._send(500, {"error": str(exc)})
        elif path == "/api/strategies":
            metas = [
                {"key": k, "name": s["name"], "desc": s["desc"]}
                for k, s in get_strategies().items()
            ]
            self._send(200, {"strategies": metas})
        elif path == "/api/datasource":
            try:
                self._send(200, {
                    "source": _DATASOURCE["source"],
                    "online_ready": online_ready(),
                    "info": get_engine().get_db_info(),
                })
            except Exception as exc:  # noqa: BLE001
                self._send(500, {"error": str(exc)})
        else:
            self._send(404, {"error": f"unknown path {path}"}, "text/plain")

    def do_POST(self) -> None:
        if not self._require_auth():
            return
        _maybe_reload()
        path = urlparse(self.path).path
        data = self._read_json()

        if path == "/api/data/update":
            start = (data.get("start") or "").strip()
            end = (data.get("end") or "").strip()
            if not start or not end:
                self._send(400, {"error": "需要 start 和 end"})
                return
            _UPDATE_CANCEL.clear()  # 新一次更新复位取消标志
            try:
                eng = get_engine()
                result = eng.sync_range(start, end, cancel_event=_UPDATE_CANCEL)
                # 更新数据时顺带刷新名称映射并持久化本地，保证后续分析纯本地、不联网
                eng.get_symbol_names(refresh=True)
                self._send(200, {"ok": True, **result})
            except InterruptedError:
                self._send(200, {"ok": False, "canceled": True, "error": "更新已取消（原区间数据已保留）"})
            except Exception as exc:  # noqa: BLE001
                self._send(500, {"ok": False, "error": str(exc)})

        elif path == "/api/data/cancel":
            _UPDATE_CANCEL.set()  # 通知正在运行的数据更新在下一个检查点终止
            self._send(200, {"ok": True, "canceled": True})

        elif path == "/api/datasource":
            # 切换数据源：local=本地默认库，online=在线下载库（允许切到在线，即使未获取
            # 以便展示「获取在线数据源」按钮；online_ready 反映是否有实际数据）
            source = set_datasource(data.get("source"))
            try:
                info = get_engine().get_db_info()
                self._send(200, {
                    "ok": True, "source": source, "online_ready": online_ready(),
                    "info": info,
                })
            except Exception as exc:  # noqa: BLE001
                self._send(500, {"ok": False, "error": str(exc)})

        elif path == "/api/online/fetch":
            # 获取在线数据源：下载 master.zip 解压 → 拼分卷 → 解 tar.gz → 得到 db
            try:
                t0 = time.time()
                db = fetch_online_source()
                set_datasource("online")  # 获取成功后自动切到在线数据源
                info = get_engine().get_db_info()
                from pathlib import Path as _P
                self._send(200, {
                    "ok": True, "source": "online", "db": db,
                    "size": _P(db).stat().st_size,
                    "elapsed": round(time.time() - t0, 1),
                    "info": info,
                })
            except AnalysisCanceled as exc:
                self._send(200, {"ok": False, "canceled": True, "error": str(exc)})
            except Exception as exc:  # noqa: BLE001
                self._send(500, {"ok": False, "error": str(exc)})

        elif path == "/api/analyze":
            set_thread_mode(data.get("mode"))  # 应用所选线程模式（single/multi）
            as_of = (data.get("date") or "").strip()
            if not as_of:
                self._send(400, {"error": "需要 date"})
                return
            _CANCEL.clear()  # 新一次分析复位取消标志
            try:
                eng = get_engine()
                # 闭市/非交易日（当日无行情记录）直接报错，不做分析
                if not eng.has_trade_date(as_of):
                    self._send(
                        400,
                        {"ok": False, "error": f"{as_of} 闭市或非交易日（库中无当日行情），本次不分析"},
                    )
                    return
                results, all_codes = analyze_day(as_of)
                # 聚合所有命中股票的未来节点收益，供前端列表展示（多线程模式逐股票并行）
                all_codes = sorted(all_codes)
                futures: dict = {}
                if _THREAD_MODE == "multi":
                    with ThreadPoolExecutor(max_workers=_FUT_WORKERS) as ex:
                        for code, fut in zip(
                            all_codes,
                            ex.map(lambda c: eng.future_returns(c, as_of), all_codes),
                        ):
                            check_cancel()  # 每只股票计算前检查取消
                            futures[code] = fut
                else:
                    for code in all_codes:
                        check_cancel()
                        futures[code] = eng.future_returns(code, as_of)
                name_map = eng.get_symbol_names()
                self._send(200, {
                    "ok": True, "date": as_of, "results": results,
                    "names": {c: name_map.get(c, "") for c in all_codes},
                    "futures": futures,
                })
            except AnalysisCanceled as exc:
                self._send(200, {"ok": False, "canceled": True, "error": str(exc)})
            except Exception as exc:  # noqa: BLE001
                self._send(500, {"ok": False, "error": str(exc)})

        elif path == "/api/analyze_range":
            set_thread_mode(data.get("mode"))  # 应用所选线程模式（single/multi）
            start = (data.get("start") or "").strip()
            end = (data.get("end") or "").strip()
            if not start or not end:
                self._send(400, {"error": "需要 start 和 end"})
                return
            keys = data.get("strategies") or None
            if isinstance(keys, list) and not keys:
                keys = None
            _CANCEL.clear()  # 新一次分析复位取消标志
            try:
                eng = get_engine()
                # 范围内所有有数据的交易日（休市/停市日自动跳过）
                trade_days = eng.trade_dates_in_range(start, end)
                if not trade_days:
                    self._send(400, {"ok": False, "error": f"{start} ~ {end} 范围内无交易日（全部休市或无数据），本次不分析"})
                    return

                # 预加载全表一次并计算全部指标列，随后逐日评估（向量化，毫秒级/天）
                full_df = eng.get_ohlcv_all()
                ind_df = strategies_mod.prepare(full_df)

                # 逐日跑选中策略，综合收集：
                # 多线程模式按天并行（只读共享 ind_df，线程安全），单线程模式逐日串行
                results: dict = {}
                hit_days_by_code: dict[str, set] = {}

                def _eval(day: str) -> dict:
                    return strategies_mod.evaluate_day(ind_df, day, keys)

                if _THREAD_MODE == "multi":
                    with ThreadPoolExecutor(max_workers=_DAY_WORKERS) as ex:
                        for day, day_results in zip(
                            trade_days,
                            ex.map(_eval, trade_days),
                        ):
                            check_cancel()  # 每天完成后检查取消
                            _merge_day(results, hit_days_by_code, day, day_results)
                else:
                    for day in trade_days:
                        check_cancel()  # 每天开始前检查取消
                        day_results = strategies_mod.evaluate_day(ind_df, day, keys)
                        _merge_day(results, hit_days_by_code, day, day_results)

                # 未来节点收益：按股票一次性批量计算（每只股票覆盖其所有命中日）
                all_codes = sorted(hit_days_by_code)
                futures: dict = {}
                if _THREAD_MODE == "multi":
                    with ThreadPoolExecutor(max_workers=_FUT_WORKERS) as ex:
                        for code, fut in zip(
                            all_codes,
                            ex.map(
                                lambda c: eng.future_returns_for_dates(c, sorted(hit_days_by_code[c])),
                                all_codes,
                            ),
                        ):
                            check_cancel()  # 每只股票计算前检查取消
                            futures[code] = fut
                else:
                    for code in all_codes:
                        check_cancel()
                        futures[code] = eng.future_returns_for_dates(code, sorted(hit_days_by_code[code]))

                names = eng.get_symbol_names()
                self._send(200, {
                    "ok": True, "start": start, "end": end,
                    "trade_days": trade_days,
                    "results": results,
                    "names": {c: names.get(c, "") for c in all_codes},
                    "futures": futures,
                })
            except AnalysisCanceled as exc:
                self._send(200, {"ok": False, "canceled": True, "error": str(exc)})
            except Exception as exc:  # noqa: BLE001
                self._send(500, {"ok": False, "error": str(exc)})

        elif path == "/api/stock/data":
            # 个股 baostock 数据查询：{code, kind} -> {fields, rows}
            code = (data.get("code") or "").strip()
            kind = (data.get("kind") or "").strip()
            if not code or not kind:
                self._send(400, {"error": "需要 code 和 kind"})
                return
            as_of = (data.get("as_of") or "").strip() or None
            try:
                self._send(200, {"ok": True, "as_of": as_of or "latest", **query_stock_data(code, kind, as_of)})
            except Exception as exc:  # noqa: BLE001
                logger.error("个股数据查询失败 code=%s kind=%s: %s", code, kind, exc)
                self._send(500, {"ok": False, "error": str(exc)})

        elif path == "/api/cancel":
            _CANCEL.set()  # 通知正在运行的分析线程在下一个检查点停止
            self._send(200, {"ok": True, "canceled": True})

        else:
            self._send(404, {"error": f"unknown path {path}"}, "text/plain")

    def log_message(self, fmt: str, *args) -> None:  # 精简访问日志
        print(f"[{self.address_string()}] {fmt % args}")


def main(port: int = 8000, host: str = "127.0.0.1") -> None:
    import signal

    server = ThreadingHTTPServer((host, port), Handler)
    print(f"Sequoia-X Web 控制台已启动：http://{host}:{port}")
    print("提示：首次使用请先在下方【数据更新】区从较早日期（如 2024-01-01）回填，")
    print("      为均线类策略预留足够历史，否则选中较早交易日时部分策略会因数据不足跳过。")
    print("按 Ctrl+C 或 Ctrl+Break 可安全停止服务。")

    def _shutdown(sig, frame):  # 显式中断处理：保证 CTRL+C / CTRL+Break 能停下来
        print(f"\n收到中断信号 (SIG{sig})，正在停止…")
        raise KeyboardInterrupt

    for _sig in (signal.SIGINT, signal.SIGTERM, getattr(signal, "SIGBREAK", None)):
        if _sig is not None:
            try:
                signal.signal(_sig, _shutdown)
            except ValueError:  # 非主线程无法设置，忽略
                pass

    try:
        # poll_interval 让主循环周期性唤醒，CTRL+C 能及时响应
        server.serve_forever(poll_interval=0.3)
    except KeyboardInterrupt:
        print("已停止。")
    finally:
        server.server_close()
        print("服务已完全停止，端口已释放。")


if __name__ == "__main__":
    # 端口优先级：命令行参数 > 环境变量 SEQUOIA_PORT > 默认 8000
    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    else:
        port = int(os.environ.get("SEQUOIA_PORT", 8000))
    # 监听地址：默认仅本机，Docker 部署时通过 SEQUOIA_HOST=0.0.0.0 暴露给宿主机
    host = os.environ.get("SEQUOIA_HOST", "127.0.0.1")
    main(port, host)