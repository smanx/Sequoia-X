"""GitHub Action 自动分析缓存脚本。

按北京时间从下方逻辑判定本次应分析的工作日：
  - 启动时间在 16:00（含）之前 → 取数据中最近的"上一个工作日"。
  - 启动时间在 16:00 之后   → 取当天；若当天未开市（库中无当日行情），则取"上一个工作日"。

随后：
  1. 用 webconsole 引擎加载 data 分支恢复的市场库（data/sequoia_v2.db）；
  2. 对目标日跑全部策略，汇聚所有命中股票；
  3. 为每只命中股票串行获取 12 类个股数据，边取边写 stock_cache.db（StockCache 每 set 即 commit）。

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


def main() -> int:
    # 复用 webconsole 的引擎 / 分析 / 个股查询 / 明细种类定义
    from app import STOCK_KINDS, analyze_day, get_engine, query_stock_data

    import argparse

    ap = argparse.ArgumentParser(description="自动分析缓存")
    ap.add_argument("--date", help="手动指定分析日期（YYYY-MM-DD）；未指定则按北京时间自动判定目标日")
    args = ap.parse_args()

    budget = float(os.environ.get("AUTO_CACHE_BUDGET", str(5 * 3600 + 40 * 60)))  # 秒，默认 5 小时 40 分钟

    eng = get_engine()

    if args.date:
        # 手动运行：指定日期则只用该日期；无数据直接结束
        target = args.date.strip()
        print(f"[auto-cache] 手动指定日期：{target}（跳过北京时间自动判定）")
        if not eng.has_trade_date(target):
            print(f"[auto-cache] 指定日期 {target} 无行情数据（非交易日或库中未获取），直接结束")
            return 0
    else:
        target = pick_target_date(eng)
    print(f"[auto-cache] 目标分析日（北京时间）：{target}")

    # 1. 单日分析：汇聚全部策略命中股票
    results, all_codes = analyze_day(target)
    codes = sorted(all_codes)
    if not codes:
        print("[auto-cache] 当日无任何策略命中，直接结束（仍会上传现有缓存）")
        return 0
    print(f"[auto-cache] 当日命中 {len(codes)} 只股票，开始逐只获取 12 类明细数据（串行、边取边存）")

    deadline = time.time() + budget
    total_ok = 0
    total_fail = 0
    for i, code in enumerate(codes, 1):
        for kind_node in STOCK_KINDS:
            kind = kind_node["key"]
            if time.time() >= deadline:
                print(f"[auto-cache] 已达时间预算（{budget:.0f}s），停止获取，保留已保存数据")
                break
            try:
                query_stock_data(code, kind, target)  # 命中缓存则不重查；查询结果实时写 stock_cache.db
                total_ok += 1
                print(f"[auto-cache] [{i}/{len(codes)}] {code} -> {kind_node['title']} 成功")
            except Exception as exc:  # noqa: BLE001  (单类失败不影响其它，记录后继续)
                total_fail += 1
                print(f"[auto-cache] [{i}/{len(codes)}] {code} -> {kind_node['title']} 失败：{exc}")
        if time.time() >= deadline:
            break

    # 关闭缓存连接，确保全部写盘
    try:
        from app import _get_stock_cache
        _get_stock_cache().close()
    except Exception:  # noqa: BLE001
        pass

    print(f"[auto-cache] 完成：目标日={target}，命中 {len(codes)} 只，成功 {total_ok} 项，失败 {total_fail} 项")
    return 0


if __name__ == "__main__":
    sys.exit(main())