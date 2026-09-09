"""GitHub Action 自动分析缓存脚本。

按北京时间从下方逻辑判定本次应分析的工作日：
  - 启动时间在 16:00（含）之前 → 取数据中最近的"上一个工作日"。
  - 启动时间在 16:00 之后   → 取当天；若当天未开市（库中无当日行情），则取"上一个工作日"。

随后：
  1. 用 webconsole 引擎加载 data 分支恢复的市场库（data/sequoia_v2.db）；
  2. 对目标日跑全部策略，汇聚所有命中股票；
  3. 为每只命中股票串行获取 12 类个股数据，边取边写 stock_cache.db（StockCache 每 set 即 commit）：
     - 未传 --date：从"当天"开始补齐，随后逐工作日向后（历史方向）补齐，
       直到遇到某工作日数据已全部有缓存为止；此时停止向后补齐。
     - 传 --date：仅补取指定那一个工作日。

时间预算：默认 5 小时 40 分钟（可用环境变量 AUTO_CACHE_BUDGET 覆盖，单位秒），
为外层 5 小时 50 分钟的步骤限时留出压缩/推送余量。
到点即停止继续获取，但每次 set 已实时落盘，任一部分数据都会保留。

用法：
  uv run python webconsole/auto_cache.py
"""

import os
import sys
import time
from datetime import datetime, timedelta


def beijing_now() -> datetime:
    from zoneinfo import ZoneInfo

    return datetime.now(ZoneInfo("Asia/Shanghai"))


def prev_trade_day(eng, from_date: str) -> str:
    """从 from_date 起往前找最近的交易日（最多 10 天）。"""
    d = datetime.strptime(from_date, "%Y-%m-%d")
    for _ in range(1, 11):
        d -= timedelta(days=1)
        s = d.strftime("%Y-%m-%d")
        if eng.has_trade_date(s):
            return s
    raise RuntimeError(f"从 {from_date} 往前 10 天都找不到交易日，请确认市场库覆盖范围")


def pick_target_date(eng) -> str:
    """按北京时间判定本次要分析的工作日，并把每一步判定过程打到日志。"""
    now = beijing_now()
    today = now.strftime("%Y-%m-%d")
    print(f"[auto-cache] 当前北京时间：{now.strftime('%Y-%m-%d %H:%M:%S')}（时区 Asia/Shanghai）")
    print(f"[auto-cache] 当前日期：{today}")

    if now.hour < 16:
        # 16:00（含）之前：当天还没收盘，取上一工作日
        print(f"[auto-cache] 判定：{now.hour} 点在 16:00 之前，取上一工作日（当天未收盘）")
        target = prev_trade_day(eng, today)
        print(f"[auto-cache] 上一工作日：{target}")
    elif eng.has_trade_date(today):
        # 16:00 之后且当天有行情：取当天
        print(f"[auto-cache] 判定：{now.hour} 点在 16:00 之后，且 {today} 有当日行情，取当天")
        target = today
    else:
        # 16:00 之后但当天未开市（休息日）：取上一工作日
        print(f"[auto-cache] 判定：{now.hour} 点在 16:00 之后，但 {today} 未开市（无当日行情），取上一工作日")
        target = prev_trade_day(eng, today)
        print(f"[auto-cache] 上一工作日：{target}")
    print(f"[auto-cache] 最终目标分析日：{target}")
    return target


def _fill_day(eng, as_of: str, deadline: float) -> tuple[int, int, int]:
    """对指定工作日 as_of：分析命中股票 -> 缓存当日分析列表 -> 补齐缺失的个股数据。

    只补"尚未写入缓存"的 (股票, 种类) 组合；已命中的直接跳过联网。
    返回 (命中股数, 成功数, 失败数)。到时间预算 deadline 即中途停止。
    """
    from app import (STOCK_KINDS_MAP, _bs_code, _get_stock_cache, _save_analysis_cache,
                     analyze_day, query_stock_data)

    results, all_codes = analyze_day(as_of)
    codes = sorted(all_codes)

    # 把"当日选出的股票列表"也写入缓存（analysis_cache 表，与 12 类个股数据同库），
    # 随 stock_cache.db 一并上传；前端分析时勾选缓存即可直接复用该列表。
    try:
        name_map = eng.get_symbol_names()
        futures: dict = {}
        for code in codes:
            try:
                futures[code] = eng.future_returns(code, as_of)
            except Exception:  # noqa: BLE001
                futures[code] = []
        _save_analysis_cache(as_of, {
            "results": results,
            "names": {c: name_map.get(c, "") for c in codes},
            "futures": futures,
        })
        print(f"[auto-cache] {as_of} 已缓存当日分析列表（{len(codes)} 只命中股票）")
    except Exception as exc:  # noqa: BLE001  (缓存失败不影响后续拉取)
        print(f"[auto-cache] {as_of} 写入分析列表缓存失败（跳过）：{exc}")

    if not codes:
        print(f"[auto-cache] {as_of} 当日无任何策略命中，视为已齐（无需补取）")
        return 0, 0, 0

    # 找出该日命中股票里"尚未写入缓存"的 (code, kind) 组合，只补这些；
    # 缓存里存的 code 是 baostock 代码（如 sh.000017），命中股票需统一转成同格式再比对。
    import sqlite3

    conn = sqlite3.connect(_get_stock_cache().path, timeout=20)
    try:
        existing = set(conn.execute(
            "SELECT code, kind FROM stock_cache WHERE asof=?", (as_of,)
        ).fetchall())
    finally:
        conn.close()
    missing = []
    for code in codes:
        bscode = _bs_code(code)
        for kind in STOCK_KINDS_MAP:
            if (bscode, kind) not in existing:
                missing.append((code, kind))

    print(f"[auto-cache] {as_of} 命中 {len(codes)} 只，缺失 {len(missing)} 项待补取")
    if not missing:
        return len(codes), 0, 0

    total_ok = 0
    total_fail = 0
    for code, kind in missing:
        if time.time() >= deadline:
            print(f"[auto-cache] {as_of} 已达时间预算，停止补取（本次成功 {total_ok} 项）")
            break
        kind_title = STOCK_KINDS_MAP[kind]
        try:
            # 命中缓存则不重查；查询结果实时写 stock_cache.db
            _, from_cache = query_stock_data(code, kind, as_of)
            total_ok += 1
            src = "命中缓存" if from_cache else "在线获取"
            print(f"[auto-cache] {as_of} {code} -> {kind_title} 成功（{src}）")
        except Exception as exc:  # noqa: BLE001  (单类失败不影响其它，记录后继续)
            total_fail += 1
            print(f"[auto-cache] {as_of} {code} -> {kind_title} 失败：{exc}")
    print(f"[auto-cache] {as_of} 补取完成：成功 {total_ok} 项，失败 {total_fail} 项")
    return len(codes), total_ok, total_fail


def _close_cache() -> None:
    """关闭缓存连接，确保全部写盘。"""
    try:
        from app import _get_stock_cache
        _get_stock_cache().close()
    except Exception:  # noqa: BLE001
        pass


def main() -> int:
    # 复用 webconsole 的引擎 / 分析 / 个股查询 / 明细种类定义
    from app import get_engine

    import argparse

    ap = argparse.ArgumentParser(description="自动分析缓存")
    ap.add_argument("--date", help="手动指定分析日期（YYYY-MM-DD）；未指定则自动向后补齐多个工作日")
    args = ap.parse_args()

    budget = float(os.environ.get("AUTO_CACHE_BUDGET", str(5 * 3600 + 40 * 60)))  # 秒，默认 5 小时 40 分钟
    deadline = time.time() + budget

    eng = get_engine()

    if args.date:
        # 手动运行：指定日期则只用该日期；无数据直接结束
        target = args.date.strip()
        print(f"[auto-cache] 手动指定日期：{target}（跳过北京时间自动判定，仅补取该日）")
        if not eng.has_trade_date(target):
            print(f"[auto-cache] 指定日期 {target} 无行情数据（非交易日或库中未获取），直接结束")
            return 0
        _fill_day(eng, target, deadline)
        _close_cache()
        return 0

    # 无 --date：从"当天"开始补齐，随后逐工作日向后（历史方向）补齐，
    # 直到遇到某工作日数据已全部有缓存（或无更早工作日 / 到时间预算）为止。
    start = pick_target_date(eng)
    print(f"[auto-cache] 起始目标分析日（北京时间）：{start}")

    # 1. 先补齐"当天"（命中缓存则跳过联网，只补缺失）
    _fill_day(eng, start, deadline)
    if time.time() >= deadline:
        print("[auto-cache] 已达时间预算，停止向后补齐")
        _close_cache()
        return 0

    # 2. 向后逐工作日补齐
    current = start
    while True:
        try:
            nxt = prev_trade_day(eng, current)
        except RuntimeError as exc:
            print(f"[auto-cache] 已无更早的可用工作日，停止向后补齐：{exc}")
            break
        _, ok, fail = _fill_day(eng, nxt, deadline)
        # 该日缺失项为 0（ok==0 且 fail==0，即补取前就已全部有缓存/无命中）→ 停止向后补齐
        if ok == 0 and fail == 0:
            print(f"[auto-cache] 上一工作日 {nxt} 数据已全部有缓存，停止向后补齐")
            break
        if time.time() >= deadline:
            print("[auto-cache] 已达时间预算，停止向后补齐")
            break
        current = nxt

    _close_cache()
    return 0


if __name__ == "__main__":
    sys.exit(main())