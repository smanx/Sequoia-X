"""全市场日K回填脚本（方案一：交易日历预检查优化版）。

不修改任何源码，在 webconsole 目录独立复刻原项目 sequoia_x/data/engine.py
的 DataEngine.backfill() 逻辑，并叠加方案一优化：

  1. 开头仅调用 1 次 bs.query_trade_dates() 获取【最近交易日】；
  2. 与库内全局 MAX(date) 预检查：已是最新 → 0 次逐股调用直接退出
     （原逻辑在周末/当天重复运行时，会对每只股票都空查 1 次，约 5000 次调用）；
  3. 逐股增量时，skip 判断与 end_date 改用【最近交易日】而非 today，
     避免周末运行时误判"未最新"而发起无效查询。

默认复用原项目同一份市场库 data/sequoia_v2.db（DB_PATH 环境变量可覆盖）。

用法：
  uv run python webconsole/backfill.py                 # 预检查 + 增量回填
  uv run python webconsole/backfill.py --force         # 跳过预检查，强制逐股拉取
  uv run python webconsole/backfill.py --start 2023-01-01   # 自定义新股/空库起始日
"""

import argparse
import sqlite3
import sys
import time
from datetime import date, timedelta

import pandas as pd

from engine import DataEngine

_DEFAULT_START = "2024-01-01"


def _get_last_date(engine: DataEngine, symbol: str) -> str | None:
    """查询单只股票本地最新交易日。"""
    with engine._conn() as conn:
        row = conn.execute(
            "SELECT MAX(date) FROM stock_daily WHERE symbol = ?", (symbol,)
        ).fetchone()
    return row[0] if row and row[0] else None


def _latest_trade_date(bs) -> str | None:
    """一次 query_trade_dates 获取今天（含）之前的最近交易日。

    回看 15 天窗口，覆盖周末与长假；返回窗口内最后一个 is_trading_day=1 的日期。
    """
    today = date.today()
    start = (today - timedelta(days=15)).strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")
    rs = bs.query_trade_dates(start_date=start, end_date=end)
    if rs.error_code != "0":
        print(f"query_trade_dates 失败: {rs.error_msg}")
        return None
    latest = None
    while rs.next():
        row = rs.get_row_data()  # [calendar_date, is_trading_day]
        if row[1] == "1":
            latest = row[0]
    return latest


def backfill(
    engine: DataEngine,
    symbols: list[str],
    start_date: str,
    latest_trade_date: str,
) -> None:
    """逐股增量回填，skip 判断与 end_date 均以最近交易日为准。"""
    import baostock as bs

    max_retries = 3
    reconnect_interval = 200  # 每处理 N 只股票重连一次，防止长连接超时

    def _login() -> bool:
        lg = bs.login()
        if lg.error_code != "0":
            print(f"baostock 登录失败: {lg.error_msg}")
            return False
        return True

    if not _login():
        return

    print(
        f"开始回填：共 {len(symbols)} 只股票 | 起始日: {start_date} | "
        f"最近交易日: {latest_trade_date}"
    )

    success = 0
    skipped = 0
    failed = 0
    since_reconnect = 0

    try:
        for i, symbol in enumerate(symbols):
            last_date = _get_last_date(engine, symbol)
            if last_date and last_date >= latest_trade_date:
                skipped += 1
                if (i + 1) % 500 == 0:
                    print(
                        f"已处理 {i + 1}/{len(symbols)}，"
                        f"成功 {success} 跳过 {skipped} 失败 {failed}"
                    )
                continue

            # 定期重连，防止长连接超时
            since_reconnect += 1
            if since_reconnect >= reconnect_interval:
                bs.logout()
                time.sleep(1)
                if not _login():
                    print("重连失败，终止回填")
                    return
                since_reconnect = 0

            start = last_date or start_date
            if last_date:
                start = (date.fromisoformat(last_date) + timedelta(days=1)).strftime("%Y-%m-%d")

            bs_code = engine._to_baostock_code(symbol)

            print(f"[{symbol}] 拉取 {start} ~ {latest_trade_date}（{bs_code}）")

            # 带重试的查询
            rows: list = []
            query_ok = False
            for attempt in range(max_retries):
                try:
                    rs = bs.query_history_k_data_plus(
                        bs_code,
                        "date,open,high,low,close,volume,amount",
                        start_date=start,
                        end_date=latest_trade_date,
                        frequency="d",
                        adjustflag="1",  # 后复权
                    )
                    if rs.error_code != "0":
                        raise RuntimeError(rs.error_msg)

                    rows = []
                    while rs.next():
                        rows.append(rs.get_row_data())
                    query_ok = True
                    break

                except Exception as exc:
                    if attempt < max_retries - 1:
                        wait = 2 ** (attempt + 1)
                        print(
                            f"[{symbol}] 第{attempt + 1}次失败: {exc}，{wait}s 后重试"
                        )
                        time.sleep(wait)
                        bs.logout()
                        time.sleep(1)
                        _login()
                    else:
                        print(f"[{symbol}] {max_retries}次重试均失败，跳过")

            if not query_ok:
                failed += 1
                continue

            if not rows:
                skipped += 1
                continue

            df = pd.DataFrame(rows, columns=rs.fields)
            for col in ["open", "high", "low", "close", "volume", "amount"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df = df.dropna(subset=["close"])
            df = df[df["volume"] > 0]

            if df.empty:
                skipped += 1
                continue

            df["symbol"] = symbol
            df = df.rename(columns={"amount": "turnover"})
            df = df[["symbol", "date", "open", "high", "low", "close", "volume", "turnover"]]

            try:
                with engine._conn() as conn:
                    df.to_sql(
                        "stock_daily", conn, if_exists="append",
                        index=False, method="multi", chunksize=500,
                    )
            except sqlite3.IntegrityError:
                pass

            success += 1

            if (i + 1) % 500 == 0:
                print(
                    f"已处理 {i + 1}/{len(symbols)}，"
                    f"成功 {success} 跳过 {skipped} 失败 {failed}"
                )

    finally:
        bs.logout()

    print(f"回填完成 — 成功: {success} | 跳过: {skipped} | 失败: {failed}")


def main() -> None:
    parser = argparse.ArgumentParser(description="全市场日K回填（方案一：交易日历预检查）")
    parser.add_argument(
        "--start",
        default=_DEFAULT_START,
        help=f"无本地数据股票的回填起始日期（默认 {_DEFAULT_START}）",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="跳过交易日历预检查，强制逐股拉取",
    )
    args = parser.parse_args()

    engine = DataEngine()

    # 1) 单次调用获取最近交易日
    import baostock as bs
    lg = bs.login()
    if lg.error_code != "0":
        print(f"baostock 登录失败: {lg.error_msg}")
        sys.exit(1)
    try:
        latest = _latest_trade_date(bs)
    finally:
        bs.logout()

    if not latest:
        print("获取最近交易日失败，回退到 today 逻辑")
        latest = date.today().strftime("%Y-%m-%d")

    # 2) 全局预检查：库内最新日期已到最近交易日 → 0 次逐股调用直接退出
    db_max = engine.get_db_info()["max_date"]
    if db_max and db_max >= latest and not args.force:
        print(
            f"数据已是最新：库内最新 {db_max} = 最近交易日 {latest}，"
            "跳过全量回填（0 次逐股调用）"
        )
        return

    print(
        f"最近交易日: {latest} | 库内最新: {db_max or '空'}"
        f"{'（--force 已跳过预检查）' if args.force else ''}"
    )

    # 3) 全市场股票列表（1 次调用）
    symbols = engine.get_all_symbols()
    if not symbols:
        print("未获取到股票列表，终止")
        sys.exit(1)

    backfill(engine, symbols, args.start, latest)


if __name__ == "__main__":
    main()
