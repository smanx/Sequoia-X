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

# ── 在线数据获取：下载 GitHub 打包的分卷数据库解压后，按"在线较新则覆盖本地库"合并纳入单一库 ──
# 数据源仓库默认为 smanx/Sequoia-X，可用环境变量 SEQUOIA_REPO（形如 "owner/repo"）覆盖
_SEQUOIA_REPO = os.environ.get("SEQUOIA_REPO", "smanx/Sequoia-X").strip().strip("/")
ONLINE_ZIP_URL = f"https://github.com/{_SEQUOIA_REPO}/archive/refs/heads/data.zip"
# 在线数据源下载包的缓存目录：master.zip 落盘缓存，后续获取直接复用，避免重复下载
ONLINE_CACHE_DIR = str((BASE_DIR.parent / "data" / "online_cache").resolve())
ONLINE_CACHE_ZIP = os.path.join(ONLINE_CACHE_DIR, "master.zip")
# 目标库：本地默认库。获取在线数据后按"在线较新则覆盖"规则合并到此处（单一库，不再区分本地/在线）。
# 用 staging 中间文件 + 文件系统级 rename 覆盖，避免整库逐行拷贝，覆盖耗时与库大小基本无关。
LOCAL_DB_PATH = engine_mod._default_db_path()
DB_STAGING = LOCAL_DB_PATH + ".staging"

# ── 在线缓存：个股数据缓存（stock_cache.db）可从 GitHub cache 分支拉取并合并到本地缓存库 ──
ONLINE_CACHE_ZIP_URL = f"https://github.com/{_SEQUOIA_REPO}/archive/refs/heads/cache.zip"
# 本地缓存库：唯一缓存库（单一库）。在线缓存获取后按"保留较新"合并入库。
LOCAL_STOCK_CACHE_PATH = str((BASE_DIR.parent / "data" / "stock_cache.db").resolve())

# 每日单次分析结果缓存（"每天选出来的股票列表"）写入本地缓存库的 analysis_cache 表，
# 与 12 类个股数据共用同一缓存库文件，随 stock_cache.db 一并压缩上传 / 合并。
# 单一缓存库：始终落在 _get_stock_cache() 返回的本地缓存库。

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
    """获取行情引擎，始终指向本地默认库 data/sequoia_v2.db（在线获取成功后按新旧覆盖该库）。
    单一库模式：不再有本地/在线两套库切换。
    """
    global _engine
    if _engine is None:
        _engine = engine_mod.DataEngine(LOCAL_DB_PATH)
    return _engine


def _max_trade_date(db_path: str) -> str | None:
    """返回指定库中 stock_daily 的最大交易日期；库不存在/无数据/损坏时返回 None。"""
    if not os.path.exists(db_path):
        return None
    try:
        conn = sqlite3.connect(db_path, timeout=20.0)
        try:
            row = conn.execute("SELECT MAX(date) FROM stock_daily").fetchone()
        finally:
            conn.close()
        return row[0] if row else None
    except Exception:  # noqa: BLE001  (表不存在/损坏等一律视为无最大值)
        return None


def apply_online_to_local(online_db: str, existed_db: bool, online_date: str) -> tuple[bool, str]:
    """把 staging 处的在线库按新旧规则覆盖到本地默认库（须已判定在线较新）。

    online_db 已由 _extract_online_source 安置到本地库同目录，此处直接 os.replace
    原子覆盖，不做逐行拷贝，覆盖耗时与库大小基本无关。
    """
    global _engine
    if os.path.exists(LOCAL_DB_PATH):
        existed_db = True
    os.makedirs(os.path.dirname(LOCAL_DB_PATH), exist_ok=True)
    os.replace(online_db, LOCAL_DB_PATH)
    _engine = None  # 覆盖后强制重建连接，避免旧连接读到陈旧/损坏数据
    return True, (f"在线数据较新（{online_date}），已覆盖本地库" +
                  ("（新建本地库）" if not existed_db else ""))


def _download_stream(url: str, dest: str, chunk: int = 1024 * 256) -> None:
    """流式下载大文件到本地路径，避免一次性载入内存。"""
    import urllib.request

    with urllib.request.urlopen(url, timeout=60) as resp, open(dest, "wb") as f:
        while True:
            buf = resp.read(chunk)
            if not buf:
                break
            f.write(buf)


def fetch_online_and_apply() -> dict:
    """下载/解压在线数据，并按"在线较新则覆盖本地库"规则合并到单一本地库。

    下载过的 master.zip 会缓存到 data/online_cache/，后续获取不再重复下载。
    分卷命名参考 .github/workflows/fetch-data.yml。
    Returns: 结果 dict（ok / overwrote / message / size / elapsed）。
    """
    import time

    t0 = time.time()
    if not os.path.isfile(ONLINE_CACHE_ZIP):
        os.makedirs(ONLINE_CACHE_DIR, exist_ok=True)
        _download_stream(ONLINE_ZIP_URL, ONLINE_CACHE_ZIP)
    db = _extract_online_source(ONLINE_CACHE_ZIP)

    online_date = _max_trade_date(db)
    if online_date is None:
        if os.path.exists(db):
            os.remove(db)
        return {"ok": True, "overwrote": False,
                "message": "在线数据无行情（MAX(date) 为空），未覆盖",
                "elapsed": round(time.time() - t0, 1)}

    local_date = _max_trade_date(LOCAL_DB_PATH)
    if local_date and local_date > online_date:
        if os.path.exists(db):
            os.remove(db)
        return {"ok": True, "overwrote": False,
                "message": f"本地数据较新（{local_date} > 在线 {online_date}），已保留本地库",
                "elapsed": round(time.time() - t0, 1)}

    existed = os.path.exists(LOCAL_DB_PATH)
    _, msg = apply_online_to_local(db, existed, online_date)
    return {"ok": True, "overwrote": True, "message": msg,
            "size": os.path.getsize(LOCAL_DB_PATH),
            "elapsed": round(time.time() - t0, 1)}


def _extract_online_source(zip_path: str) -> str:
    """从本地缓存的 master.zip 中解出在线数据库，安置到 DB_STAGING（本地库同目录）。

    解包路径：解 zip → 拼分卷 → 解 tar.gz → 得到 sequoia_v2.db。
    放到本地库同目录是为让后续 os.replace 走同卷原子改名，避免跨卷整库拷贝。
    """
    import glob
    import shutil
    import tarfile
    import tempfile
    import zipfile

    tmp = tempfile.mkdtemp(prefix="seq_online_")
    try:
        with zipfile.ZipFile(zip_path) as z:
            top = z.namelist()[0].split("/")[0] if z.namelist() else ""
            z.extractall(tmp)

        src_data = os.path.join(tmp, top, "data")
        parts = sorted(glob.glob(os.path.join(src_data, "sequoia_v2.tar.gz.*")))
        db = None
        if not parts:
            # 兼容无分卷的情况：单文件 tar.gz 或直接裸 db
            single = os.path.join(src_data, "sequoia_v2.tar.gz")
            direct_db = os.path.join(src_data, "sequoia_v2.db")
            if os.path.isfile(single):
                parts = [single]
            elif os.path.isfile(direct_db):
                db = direct_db
            else:
                raise FileNotFoundError("在线源码包 data 目录未找到数据库分卷(sequoia_v2.tar.gz.*)")

        if db is None:
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

        # 搬到本地库同目录的 staging，与本地库同卷，os.replace 原子覆盖免去整库拷贝
        os.makedirs(os.path.dirname(DB_STAGING), exist_ok=True)
        if os.path.exists(DB_STAGING):
            os.remove(DB_STAGING)
        shutil.move(db, DB_STAGING)
        return DB_STAGING
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def fetch_online_cache(log: list | None = None) -> dict:
    """下载 cache 分支 zip -> 解 zip -> 拼接 stock_cache.tar.gz 分卷 -> 解 tar.gz -> 得在线缓存库，
    随后把在线缓存**合并**进本地缓存库（单一库：data/stock_cache.db）。

    合并不覆盖、不丢数据：把在线库的 stock_cache / analysis_cache 两张表按主键 upsert 进本地库，
    冲突行以 updated_at 较新者取胜——本地已有且较新的缓存保持原样，在线新增/更新的补入本地。
    整体在单个事务内完成，合并速度远快于逐条插入。
    log 传入列表时会把每个步骤的明细追加进去（供前端展示）。
    Returns: 结果 dict（rows / size / inserted / updated / skipped / log）。
    """
    import glob as _glob
    import shutil as _shutil
    import tarfile as _tarfile
    import tempfile as _tempfile
    import zipfile as _zipfile

    if log is None:
        log = []

    def note(msg: str) -> None:
        log.append(msg)
        print(f"[online-cache] {msg}")

    tmp = _tempfile.mkdtemp(prefix="seq_cache_")
    try:
        zip_path = os.path.join(tmp, "cache.zip")
        note("开始下载 cache 分支压缩包…")
        _download_stream(ONLINE_CACHE_ZIP_URL, zip_path)
        note(f"下载完成：{os.path.getsize(zip_path) / 1048576:.1f} MB，解压 zip…")

        with _zipfile.ZipFile(zip_path) as z:
            top = z.namelist()[0].split("/")[0] if z.namelist() else ""
            z.extractall(tmp)
        note("zip 解压完毕")

        src_data = os.path.join(tmp, top, "data")
        parts = sorted(_glob.glob(os.path.join(src_data, "stock_cache.tar.gz.*")))
        db = None
        if not parts:
            # 兼容无分卷的情况：单文件 tar.gz 或直接裸 db
            single = os.path.join(src_data, "stock_cache.tar.gz")
            direct = os.path.join(src_data, "stock_cache.db")
            if os.path.isfile(single):
                parts = [single]
            elif os.path.isfile(direct):
                db = direct
            else:
                raise FileNotFoundError("cache 分支未找到缓存分卷(stock_cache.tar.gz.*)")

        if db is None:
            note(f"找到 {len(parts)} 个分卷，开始拼接…")
            tar_path = os.path.join(tmp, "stock_cache.tar.gz")
            with open(tar_path, "wb") as out:
                for i, p in enumerate(parts, 1):
                    with open(p, "rb") as f:
                        _shutil.copyfileobj(f, out)
                    note(f"拼接分卷 {i}/{len(parts)}：{os.path.basename(p)}")
            note("分卷拼接完成，解压 tar.gz…")
            with _tarfile.open(tar_path, "r:gz") as t:
                t.extractall(tmp)
            db = os.path.join(tmp, "data", "stock_cache.db")
            if not os.path.isfile(db):
                raise FileNotFoundError("解压后未找到 stock_cache.db")
        note("tar.gz 解压完成，开始合并到本地缓存库…")

        merged = _merge_cache_db(db)
        note(f"合并完成：在线库 {merged['online']} 条 → 新增/更新 {merged['changed']} 条，"
             f"跳过(本地较新) {merged['skipped']} 条；本地缓存库现共 {_cache_rows(LOCAL_STOCK_CACHE_PATH)} 条")
        return {
            "rows": _cache_rows(LOCAL_STOCK_CACHE_PATH),
            "size": os.path.getsize(LOCAL_STOCK_CACHE_PATH),
            "online": merged["online"],
            "changed": merged["changed"],
            "skipped": merged["skipped"],
            "log": log,
        }
    finally:
        _shutil.rmtree(tmp, ignore_errors=True)


def _merge_cache_db(online_db: str) -> dict:
    """把在线缓存库 online_db 合并进本地缓存库（单一库）。

    单事务 + ATTACH 在线库 + INSERT...SELECT...ON CONFLICT，冲突行取 updated_at 较新者，
    避免逐条读写；返回合并统计（online=在线库总条数, changed=新增/更新的条数, skipped=本地较新跳过的条数）。
    """
    import sqlite3 as _sqlite3

    global _STOCK_CACHE
    # 先释放可能占用本地库连接的缓存实例，避免合并写时的锁冲突
    if _STOCK_CACHE is not None:
        try:
            _STOCK_CACHE.close()
        except Exception:  # noqa: BLE001
            pass
        _STOCK_CACHE = None

    os.makedirs(os.path.dirname(LOCAL_STOCK_CACHE_PATH), exist_ok=True)
    conn = _sqlite3.connect(LOCAL_STOCK_CACHE_PATH, timeout=20)
    try:
        conn.execute("PRAGMA busy_timeout=20000")
        conn.execute("PRAGMA journal_mode=TRUNCATE")
        # 确保本地库含目标表
        conn.execute(
            "CREATE TABLE IF NOT EXISTS stock_cache("
            "code TEXT NOT NULL, kind TEXT NOT NULL, asof TEXT NOT NULL,"
            " payload TEXT NOT NULL, updated_at REAL NOT NULL,"
            " PRIMARY KEY(code, kind, asof))"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS analysis_cache("
            "asof TEXT PRIMARY KEY, payload TEXT NOT NULL, updated_at REAL NOT NULL)"
        )
        conn.execute("ATTACH DATABASE ? AS src", (online_db,))

        conn.execute("BEGIN")
        try:
            # stock_cache 中本地 updated_at 较新而被跳过的条数
            skipped = conn.execute(
                "SELECT COUNT(*) FROM src.stock_cache sc JOIN stock_cache d "
                "ON d.code=sc.code AND d.kind=sc.kind AND d.asof=sc.asof "
                "WHERE d.updated_at > sc.updated_at"
            ).fetchone()[0] or 0
            # analysis_cache 中本地 updated_at 较新而被跳过的条数
            skipped += conn.execute(
                "SELECT COUNT(*) FROM src.analysis_cache sc JOIN analysis_cache d "
                "ON d.asof=sc.asof WHERE d.updated_at > sc.updated_at"
            ).fetchone()[0] or 0

            # 合并 stock_cache：本地无更旧(updated_at 不高于在线)者才写入，冲突时以在线值覆盖。
            # NOT EXISTS 已滤掉"本地较新"行，故剩余行的 ON CONFLICT 更新要么持平要么在线上新，均可覆盖。
            changed = conn.execute(
                "INSERT INTO stock_cache(code, kind, asof, payload, updated_at) "
                "SELECT sc.code, sc.kind, sc.asof, sc.payload, sc.updated_at "
                "FROM src.stock_cache sc WHERE NOT EXISTS ("
                "  SELECT 1 FROM stock_cache d "
                "  WHERE d.code=sc.code AND d.kind=sc.kind AND d.asof=sc.asof "
                "    AND d.updated_at > sc.updated_at) "
                "ON CONFLICT(code, kind, asof) DO UPDATE SET "
                "payload=excluded.payload, updated_at=excluded.updated_at"
            ).rowcount
            changed += conn.execute(
                "INSERT INTO analysis_cache(asof, payload, updated_at) "
                "SELECT sc.asof, sc.payload, sc.updated_at "
                "FROM src.analysis_cache sc WHERE NOT EXISTS ("
                "  SELECT 1 FROM analysis_cache d "
                "  WHERE d.asof=sc.asof AND d.updated_at > sc.updated_at) "
                "ON CONFLICT(asof) DO UPDATE SET "
                "payload=excluded.payload, updated_at=excluded.updated_at"
            ).rowcount
            conn.commit()
        except Exception:
            conn.rollback()
            raise

        src_rows = conn.execute(
            "SELECT (SELECT COUNT(*) FROM src.stock_cache)"
            "     + (SELECT COUNT(*) FROM src.analysis_cache)"
        ).fetchone()[0] or 0
        conn.execute("DETACH DATABASE src")
        return {"online": src_rows, "changed": changed, "skipped": skipped}
    finally:
        conn.close()


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

# 个股数据查询的本地缓存：缓存永久有效，不过期
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
    """惰性创建个股数据缓存（独立缓存库，不影响主行情库）。

    local=本地 data/stock_cache.db（单一缓存库；在线缓存获取后合并于此，不再有在线/本地两套）。
    """
    global _STOCK_CACHE
    if _STOCK_CACHE is None:
        _STOCK_CACHE = cache_mod.StockCache(str(LOCAL_STOCK_CACHE_PATH))
    return _STOCK_CACHE


def _cache_rows(path: str) -> int:
    """返回某个缓存库里 stock_cache 表的数据条数（文件不存在/损坏则 0）。"""
    if not os.path.exists(path):
        return 0
    try:
        conn = sqlite3.connect(path)
        n = conn.execute("SELECT COUNT(*) FROM stock_cache").fetchone()[0]
        conn.close()
        return n or 0
    except Exception:  # noqa: BLE001
        return 0


# ── 当日分析结果缓存（按日期缓存 "选出来的股票列表"，存入当前缓存源库） ──

def _analysis_conn() -> sqlite3.Connection:
    """返回当前缓存源库连接，并确保其中存在 analysis_cache 表。

    与 12 类个股数据共用同一个缓存库（local/online），随其一并上传/合并。
    """
    path = _get_stock_cache().path
    conn = sqlite3.connect(path, timeout=20, check_same_thread=False)
    try:
        conn.execute("PRAGMA busy_timeout=20000")
        # 与 StockCache 一致用非 WAL，避免服务器容器/网络盘上 -wal/-shm 不稳导致 malformed
        conn.execute("PRAGMA journal_mode=TRUNCATE")
    except sqlite3.Error:
        pass
    conn.execute(
        "CREATE TABLE IF NOT EXISTS analysis_cache("
        "asof TEXT PRIMARY KEY, payload TEXT NOT NULL, updated_at REAL NOT NULL)"
    )
    conn.commit()
    return conn


def _load_analysis_cache(as_of: str) -> dict | None:
    """读取某日分析结果缓存；未命中返回 None。"""
    try:
        conn = _analysis_conn()
        try:
            row = conn.execute(
                "SELECT payload FROM analysis_cache WHERE asof=?", (as_of,)
            ).fetchone()
        finally:
            conn.close()
        return json.loads(row[0]) if row else None
    except Exception:  # noqa: BLE001
        return None


def _save_analysis_cache(as_of: str, payload: dict) -> None:
    """写入/覆盖某日分析结果缓存。"""
    try:
        conn = _analysis_conn()
        try:
            conn.execute(
                "INSERT INTO analysis_cache(asof, payload, updated_at) VALUES(?, ?, ?) "
                "ON CONFLICT(asof) DO UPDATE SET "
                "payload=excluded.payload, updated_at=excluded.updated_at",
                (as_of, json.dumps(payload, ensure_ascii=False), time.time()),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception:  # noqa: BLE001
        pass


def get_cache_info() -> dict:
    """返回本地缓存库的统计信息（条数、覆盖股票数），供前端展示。"""
    path = LOCAL_STOCK_CACHE_PATH
    if not os.path.exists(path):
        return {"rows": 0, "keys": 0}
    try:
        conn = sqlite3.connect(path)
        row = conn.execute("SELECT COUNT(*), COUNT(DISTINCT code) FROM stock_cache").fetchone()
        conn.close()
        return {"rows": row[0] or 0, "keys": row[1] or 0}
    except Exception:  # noqa: BLE001
        return {"rows": 0, "keys": 0}


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


def query_stock_data(code: str, kind: str, as_of: str | None = None,
                     only_cache: bool = False, no_cache: bool = False) -> tuple[dict | None, bool]:
    """查询个股 baostock 数据并本地缓存，返回 ({"fields": [...], "rows": [[...], ...]}, from_cache)。

    from_cache 标记本次结果来自缓存(True)还是在线实时获取(False)。
    as_of 为数据截止日期（可选）：传入后可查看/缓存该历史时点的数据；
    为 None 时取到今天。缓存键含 as_of，不同时点数据互不覆盖。

    only_cache=True 时仅读取缓存：命中返回缓存；未命中返回 (None, False)，不发起在线请求。
    no_cache=True 时忽略缓存：跳过缓存读取，强制在线拉取并把结果覆盖写回缓存。
    查询顺序：内存缓存 → SQLite 缓存 → 真实 baostock 请求（回填两层缓存）。
    """
    if kind not in STOCK_KINDS_MAP:
        raise ValueError(f"未知数据种类: {kind}")
    bscode = _bs_code(code)
    cache = _get_stock_cache()
    as_of_key = (as_of or "").strip() or "latest"
    if not no_cache:
        cached = cache.get(bscode, kind, as_of_key)
        if cached is not None:
            return cached, True
    if only_cache:
        return None, False  # 仅缓存模式：未命中不联网
    fields, rows = _bs_query(bscode, kind, as_of)
    if not fields:
        raise RuntimeError("该数据种类无返回数据（可能该股无此类信息）")
    result = {"fields": fields, "rows": rows}
    cache.set(bscode, kind, as_of_key, result)
    return result, False


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
        elif path == "/field_help.js":
            self._serve_static("field_help.js", "application/javascript")
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
            # 单一库模式：无本地/在线切换，仅返回当前本地库概况
            try:
                self._send(200, {
                    "source": "local",
                    "info": get_engine().get_db_info(),
                })
            except Exception as exc:  # noqa: BLE001
                self._send(500, {"error": str(exc)})
        elif path == "/api/online-cache":
            # 单一缓存库：无本地/在线切换，仅返回本地缓存概况
            try:
                self._send(200, {
                    "source": "local",
                    "info": get_cache_info(),
                })
            except Exception as exc:  # noqa: BLE001
                self._send(500, {"error": str(exc)})
        elif path == "/api/analyze_cache":
            # 查询某日分析结果是否已有缓存，供前端决定是否展示"使用缓存"勾选框
            try:
                as_of = urlparse(self.path).query
                as_of = as_of.replace("date=", "").strip() if "date=" in as_of else ""
                cached = _load_analysis_cache(as_of) if as_of else None
                self._send(200, {
                    "ok": True, "date": as_of,
                    "cached": cached is not None,
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

        elif path == "/api/online/fetch":
            # 获取在线数据：下载 master.zip 解压 → 拼分卷 → 解 tar.gz → 按"在线较新则覆盖"合并到本地库
            try:
                res = fetch_online_and_apply()
                info = get_engine().get_db_info()  # 用新库触发重建连接并校验可读
                self._send(200, {
                    "ok": True, "source": "local",
                    "overwrote": res["overwrote"],
                    "message": res["message"],
                    "size": res.get("size"),
                    "elapsed": res["elapsed"],
                    "info": info,
                })
            except AnalysisCanceled as exc:
                self._send(200, {"ok": False, "canceled": True, "error": str(exc)})
            except Exception as exc:  # noqa: BLE001
                self._send(500, {"ok": False, "error": str(exc)})

        elif path == "/api/online-cache/fetch":
            # 获取在线缓存并合并到本地缓存库（获取后一定合并，无"本地已有就跳过"）。
            # 每一步都输出明细日志。
            try:
                t0 = time.time()
                log: list = []
                log.append(f"本地缓存 {_cache_rows(LOCAL_STOCK_CACHE_PATH) or 0} 条，开始下载并合并在线缓存（{LOCAL_STOCK_CACHE_PATH}）")
                res = fetch_online_cache(log)
                self._send(200, {
                    "ok": True, "source": "local",
                    "size": res["size"],
                    "elapsed": round(time.time() - t0, 1),
                    "online": res["online"],
                    "changed": res["changed"],
                    "skipped": res["skipped"],
                    "info": get_cache_info(),
                    "log": log,
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
            only_cache = bool(data.get("only_cache"))  # 只读缓存：无当日缓存则返回空，不联网/不重算
            no_cache = bool(data.get("no_cache"))      # 完全重新分析：忽略已缓存结果，强制重算并覆盖缓存
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
                # 自动缓存：优先读当日结果缓存，命中直接返回（勾选了 no_cache 则忽略缓存强制重算）；
                # 仅缓存模式下未命中才返回空。
                if not no_cache:
                    cached = _load_analysis_cache(as_of)
                    if cached is not None:
                        self._send(200, {
                            "ok": True, "date": as_of, "from_cache": True, **cached,
                        })
                        return
                if only_cache:
                    self._send(200, {
                        "ok": True, "date": as_of, "from_cache": False,
                        "only_cache_empty": True,
                        "results": {}, "names": {}, "futures": {},
                    })
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
                payload = {
                    "results": results,
                    "names": {c: name_map.get(c, "") for c in all_codes},
                    "futures": futures,
                }
                _save_analysis_cache(as_of, payload)  # 每次成功分析都写入缓存，供日后勾选复用
                self._send(200, {"ok": True, "date": as_of, "from_cache": False, **payload})
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
            only_cache = bool(data.get("only_cache"))  # 只读缓存：逐日读当日分析结果缓存聚合，无缓存则不重算
            no_cache = bool(data.get("no_cache"))      # 完全重新分析：忽略已缓存结果，全部逐日强制重算
            _CANCEL.clear()  # 新一次分析复位取消标志
            try:
                eng = get_engine()
                # 范围内所有有数据的交易日（休市/停市日自动跳过）
                trade_days = eng.trade_dates_in_range(start, end)
                if not trade_days:
                    self._send(400, {"ok": False, "error": f"{start} ~ {end} 范围内无交易日（全部休市或无数据），本次不分析"})
                    return

                if only_cache:
                    # 只读缓存：对每个交易日读取当日分析结果缓存（当日命中列表），
                    # 无缓存的日直接跳过；有缓存则并入综合结果，不联网、不重算策略。
                    results: dict = {}
                    hit_days_by_code: dict[str, set] = {}
                    names: dict = {}
                    futures: dict = {}
                    cached_days: list[str] = []
                    for day in trade_days:
                        check_cancel()
                        cached = _load_analysis_cache(day)
                        if cached is None:
                            continue
                        cached_days.append(day)
                        day_results = cached.get("results") or {}
                        if keys is not None:
                            day_results = {k: v for k, v in day_results.items() if k in keys}
                        _merge_day(results, hit_days_by_code, day, day_results)
                        for c, nm in (cached.get("names") or {}).items():
                            names.setdefault(c, nm)
                        for c, fut in (cached.get("futures") or {}).items():
                            futures.setdefault(c, {})[day] = fut
                    self._send(200, {
                        "ok": True, "start": start, "end": end,
                        "from_cache": True,
                        "trade_days": cached_days,
                        "cached_days": cached_days,
                        "results": results,
                        "names": names,
                        "futures": futures,
                    })
                    return

                # 自动缓存：逐交易日优先读当日分析结果缓存，只有无缓存的日期才真正分析。
                # 预加载全表一次并计算全部指标列，随后只对缺失日期逐日评估（向量化，毫秒级/天）
                full_df = eng.get_ohlcv_all()
                ind_df = strategies_mod.prepare(full_df)

                results: dict = {}
                hit_days_by_code: dict[str, set] = {}
                used_cache_days: list[str] = []
                missing_days: list[str] = []

                for day in trade_days:
                    check_cancel()
                    # no_cache：完全重新分析，忽略已缓存结果，全部日走真正计算
                    if no_cache:
                        missing_days.append(day)
                        continue
                    cached = _load_analysis_cache(day)
                    if cached is not None:
                        used_cache_days.append(day)
                        day_results = cached.get("results") or {}
                        if keys is not None:
                            day_results = {k: v for k, v in day_results.items() if k in keys}
                        _merge_day(results, hit_days_by_code, day, day_results)
                    else:
                        missing_days.append(day)

                # 只对无缓存的日期真正运行策略评估：
                # 多线程模式按天并行（只读共享 ind_df，线程安全），单线程模式逐日串行
                def _eval(day: str) -> dict:
                    return strategies_mod.evaluate_day(ind_df, day, keys)

                if missing_days:
                    if _THREAD_MODE == "multi":
                        with ThreadPoolExecutor(max_workers=_DAY_WORKERS) as ex:
                            for day, day_results in zip(
                                missing_days,
                                ex.map(_eval, missing_days),
                            ):
                                check_cancel()  # 每天完成后检查取消
                                _merge_day(results, hit_days_by_code, day, day_results)
                    else:
                        for day in missing_days:
                            check_cancel()  # 每天开始前检查取消
                            day_results = strategies_mod.evaluate_day(ind_df, day, keys)
                            _merge_day(results, hit_days_by_code, day, day_results)

                # 未来节点收益：
                #  - 缓存命中日：直接取缓存里的 future_returns(code, as_of)（单日，按 as_of 组织）
                #  - 真正计算日：按股票一次性批量计算 future_returns_for_dates
                all_codes = sorted(hit_days_by_code)
                futures: dict = {}
                for day in used_cache_days:
                    cached = _load_analysis_cache(day)
                    for c, fut in (cached.get("futures") or {}).items():
                        futures.setdefault(c, {})[day] = fut
                comp_dates_by_code: dict[str, list[str]] = {}
                for day in missing_days:
                    for code in hit_days_by_code:
                        if day in hit_days_by_code[code]:
                            comp_dates_by_code.setdefault(code, []).append(day)
                comp_codes = [c for c in all_codes if c in comp_dates_by_code]
                if _THREAD_MODE == "multi":
                    with ThreadPoolExecutor(max_workers=_FUT_WORKERS) as ex:
                        for code, fut in zip(
                            comp_codes,
                            ex.map(lambda c: eng.future_returns_for_dates(c, sorted(comp_dates_by_code[c])),
                                   comp_codes),
                        ):
                            check_cancel()  # 每只股票计算前检查取消
                            for d, v in fut.items():
                                futures.setdefault(code, {})[d] = v
                else:
                    for code in comp_codes:
                        check_cancel()
                        for d, v in eng.future_returns_for_dates(code, sorted(comp_dates_by_code[code])).items():
                            futures.setdefault(code, {})[d] = v

                names = eng.get_symbol_names()
                self._send(200, {
                    "ok": True, "start": start, "end": end,
                    "from_cache": bool(used_cache_days),
                    "cached_days": used_cache_days,
                    "computed_days": missing_days,
                    "trade_days": trade_days,
                    "results": results,
                    "names": {c: names.get(c, "") for c in all_codes},
                    "futures": futures,
                })
            except AnalysisCanceled as exc:
                self._send(200, {"ok": False, "canceled": True, "error": str(exc)})
            except Exception as exc:  # noqa: BLE001
                self._send(500, {"ok": False, "error": str(exc)})

        elif path == "/api/stock/batch":
            # 长接口：前端一次提交整份股票列表，后端逐项取数（自动复用缓存），
            # 每处理一项即通过 NDJSON 流式把日志 / 结果回传前端，最后返回 done 全文。
            codes = data.get("codes") or []
            if not isinstance(codes, list) or not codes:
                self._send(400, {"error": "需要非空 codes 列表"})
                return
            as_of = (data.get("as_of") or "").strip() or None
            only_cache = bool(data.get("only_cache"))  # 只读缓存：未命中的项不联网，直接跳过
            no_cache = bool(data.get("no_cache"))      # 完全重新获取：忽略本地缓存，强制联网拉取并覆盖
            name_map = get_engine().get_symbol_names()

            # 建立长响应（流式 NDJSON），设置输出头后逐行写入
            try:
                self.send_response(200)
                self.send_header("Content-Type", "application/x-ndjson")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("X-Accel-Buffering", "no")
                self.end_headers()
            except Exception:  # noqa: BLE001
                return

            def emit(obj) -> None:
                try:
                    self.wfile.write((json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8"))
                    self.wfile.flush()
                except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):
                    raise  # 客户端中断，交由外层静默结束

            try:
                buf: dict = {}
                ok = fail = 0
                total = len(codes) * len(STOCK_KINDS)
                emit({"type": "start", "count": len(codes)})
                for si, code in enumerate(codes, 1):
                    code = str(code).strip()
                    buf[code] = {}
                    nm = (name_map or {}).get(code, "")
                    for kd in STOCK_KINDS:
                        kind = kd["key"]
                        try:
                            # 内存→SQLite→baostock，命中即复用缓存；only_cache 时未命中不联网，
                            # no_cache 时忽略缓存、强制在线拉取并覆盖缓存
                            r, from_cache = query_stock_data(code, kind, as_of,
                                                             only_cache=only_cache, no_cache=no_cache)
                            if r is None:
                                # 仅缓存模式未命中：不联网获取，跳过该条
                                fail += 1
                                emit({"type": "item", "idx": si, "code": code, "name": nm,
                                      "title": kd["title"], "ok": False, "error": "无缓存（仅缓存模式，未联网获取）"})
                                continue
                            buf[code][kind] = r
                            ok += 1
                            emit({"type": "item", "idx": si, "code": code, "name": nm,
                                  "title": kd["title"], "ok": True, "cached": from_cache})
                        except Exception as exc:  # noqa: BLE001
                            buf[code][kind] = None
                            fail += 1
                            emit({"type": "item", "idx": si, "code": code, "name": nm,
                                  "title": kd["title"], "ok": False, "error": str(exc)})
                # 先发"完成信号"（只带计数，极小，前端可立刻展示统计与按钮），
                # 再按股票逐条下发数据。绝不能把全部数据压成一条超长 NDJSON 行：
                # 前端发流读取时会对积累中的单行反复全量扫描（O(n²)），导致久等才响应。
                emit({"type": "done", "total": total, "ok": ok, "fail": fail})
                for code, cbuf in buf.items():
                    if cbuf:
                        emit({"type": "payload", "code": code, "data": cbuf})
            except Exception:  # noqa: BLE001  (客户端中断写失败等，静默结束)
                pass

        elif path == "/api/stock/data":
            # 个股 baostock 数据查询：{code, kind} -> {fields, rows}
            code = (data.get("code") or "").strip()
            kind = (data.get("kind") or "").strip()
            if not code or not kind:
                self._send(400, {"error": "需要 code 和 kind"})
                return
            as_of = (data.get("as_of") or "").strip() or None
            try:
                r, from_cache = query_stock_data(code, kind, as_of)
                self._send(200, {"ok": True, "as_of": as_of or "latest", "cached": from_cache, **r})
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