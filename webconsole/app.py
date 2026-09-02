"""Sequoia-X Web 控制台：标准库 http.server 实现，零新增依赖。

功能：
  GET  /                    → 前端页面
  GET  /api/info            → 数据库覆盖范围（min/max 日期、股票数）
  POST /api/data/update     → {start, end} 拉取/更新该区间数据
  POST /api/analyze         → {date} 策略分析该交易日，返回各策略选股

启动：
  python app.py [port]      # 默认 8000，浏览器访问 http://127.0.0.1:8000
"""

import importlib
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

# 用「模块引用」方式导入（而非 from ... import 类），以支持热重载
import engine as engine_mod
import strategies as strategies_mod

logger = engine_mod.get_logger(__name__)
BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

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
        _engine = engine_mod.DataEngine()
    return _engine


def get_strategies() -> dict:
    global _strategies
    if _strategies is None:
        _strategies = strategies_mod.build_strategies(get_engine())
    return _strategies


def run_analyze(as_of_date: str | None, keys: list[str] | None = None) -> dict:
    """对指定日期运行全部（或 keys 指定的）策略，返回 (各策略选股, 代码->名称映射)。"""
    results: dict = {}
    name_engine = get_engine()
    for key, strat in get_strategies().items():
        if keys and key not in keys:
            continue
        check_cancel()  # 每个策略前检查取消
        try:
            selected = strat.run(as_of_date)
            results[key] = {
                "name": getattr(strat, "name", key),
                "desc": getattr(strat, "desc", ""),
                "count": len(selected),
                "symbols": selected,
                "stats": getattr(strat, "stats", None),
            }
        except Exception as exc:  # noqa: BLE001
            logger(f"[{key}] 运行失败: {exc}")
            results[key] = {"name": key, "desc": "", "count": 0, "symbols": [], "error": str(exc)}
    # 汇总所有被选中的代码，一次性取名称，供前端展示
    all_codes = {s for r in results.values() for s in r.get("symbols", [])}
    names = name_engine.get_symbol_names()
    return results, {c: names.get(c, "") for c in all_codes}


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
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length", 0) or 0)
        raw = self.rfile.read(length) if length else b"{}"
        try:
            return json.loads(raw or b"{}")
        except json.JSONDecodeError:
            return {}

    def _serve_static(self, name: str, ctype: str) -> None:
        path = STATIC_DIR / name
        if not path.is_file():
            self._send(404, {"error": "not found"}, "text/plain")
            return
        self._send(200, path.read_bytes(), ctype)

    # ── 路由 ──
    def do_GET(self) -> None:
        _maybe_reload()
        path = urlparse(self.path).path
        if path in ("/", "/index.html"):
            self._serve_static("index.html", "text/html")
        elif path == "/api/info":
            try:
                self._send(200, get_engine().get_db_info())
            except Exception as exc:  # noqa: BLE001
                self._send(500, {"error": str(exc)})
        elif path == "/api/strategies":
            metas = [
                {"key": k, "name": getattr(s, "name", k), "desc": getattr(s, "desc", "")}
                for k, s in get_strategies().items()
            ]
            self._send(200, {"strategies": metas})
        else:
            self._send(404, {"error": f"unknown path {path}"}, "text/plain")

    def do_POST(self) -> None:
        _maybe_reload()
        path = urlparse(self.path).path
        data = self._read_json()

        if path == "/api/data/update":
            start = (data.get("start") or "").strip()
            end = (data.get("end") or "").strip()
            if not start or not end:
                self._send(400, {"error": "需要 start 和 end"})
                return
            try:
                result = get_engine().sync_range(start, end)
                self._send(200, {"ok": True, **result})
            except Exception as exc:  # noqa: BLE001
                self._send(500, {"ok": False, "error": str(exc)})

        elif path == "/api/analyze":
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
                results, names = run_analyze(as_of)
                # 聚合所有命中股票的未来节点收益，供前端列表展示
                all_codes = sorted({s for r in results.values() for s in r["symbols"]})
                futures: dict = {}
                for code in all_codes:
                    check_cancel()  # 每只股票计算前检查取消
                    futures[code] = eng.future_returns(code, as_of)
                self._send(200, {"ok": True, "date": as_of, "results": results, "names": names, "futures": futures})
            except AnalysisCanceled as exc:
                self._send(200, {"ok": False, "canceled": True, "error": str(exc)})
            except Exception as exc:  # noqa: BLE001
                self._send(500, {"ok": False, "error": str(exc)})

        elif path == "/api/analyze_range":
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

                # 逐日跑选中策略，综合收集
                results: dict = {}
                hit_days_by_code: dict[str, set] = {}
                for day in trade_days:
                    check_cancel()  # 每天开始前检查取消
                    day_results, _ = run_analyze(day, keys)
                    for k, r in day_results.items():
                        bucket = results.setdefault(k, {"name": r["name"], "desc": r["desc"], "days": [], "count": 0})
                        bucket["days"].append({"date": day, "count": r["count"], "symbols": r.get("symbols", [])})
                        bucket["count"] += r["count"]
                        for s in r.get("symbols", []):
                            hit_days_by_code.setdefault(s, set()).add(day)

                # 未来节点收益：按股票一次性批量计算（每只股票覆盖其所有命中日）
                all_codes = sorted(hit_days_by_code)
                futures: dict = {}
                for code in all_codes:
                    check_cancel()  # 每只股票计算前检查取消
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

        elif path == "/api/cancel":
            _CANCEL.set()  # 通知正在运行的分析线程在下一个检查点停止
            self._send(200, {"ok": True, "canceled": True})

        else:
            self._send(404, {"error": f"unknown path {path}"}, "text/plain")

    def log_message(self, fmt: str, *args) -> None:  # 精简访问日志
        print(f"[{self.address_string()}] {fmt % args}")


def main(port: int = 8000) -> None:
    import signal

    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"Sequoia-X Web 控制台已启动：http://127.0.0.1:{port}")
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
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    main(port)