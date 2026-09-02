"""策略集：向量化评估实现。

设计：
- 一次性在预加载全表上计算全部指标列（compute_indicators / prepare），
  所有 shift/rolling 均按 symbol 分组、只回看历史，因此对任一日截断后取值
  与旧版「逐股票循环 + 每日重复滚动」结果完全一致。
- 按交易日 evaluate_day(ind_df, day) 评估：取每只股票截至当日的最后一根K线，
  用预计算指标做向量化判定，毫秒级完成一天。
- 相比旧实现：消除了逐股票 Python 循环与每日重复滚动，单日/范围分析大幅提速，
  且线程模式（单/多线程）几乎不再影响耗时。

统计 self.stats 兼容旧格式：
  total      已分析的股票数量
  matched    符合策略的数量
  reasons    不符合原因分布 {原因: 数量}
  samples    各原因抽样代码 {原因: [代码...]}
"""

import pandas as pd

from engine import DataEngine


# ── 指标一次性计算（向量化，按 symbol 分组、仅回看历史） ──

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """在传入的全表上就地追加全部策略所需的指标列。

    注意：rolling/shift 必须用 min_periods=窗口大小，与旧版逐股 rolling 的默认
    语义一致，才能精确复现「历史K线不足(窗口不完整)」等判定。
    """
    g = df.groupby("symbol", sort=False)

    # TurtleTrade：20日新高（排除当日，即前20日最高）
    df["high_p1"] = g["high"].shift(1)
    df["high20"] = g["high_p1"].rolling(20, min_periods=20).max().reset_index(level=0, drop=True)

    # MaVolume：均线 + 均量（含前一日均线供金叉判定）
    df["ma5"] = g["close"].rolling(5, min_periods=5).mean().reset_index(level=0, drop=True)
    df["ma20"] = g["close"].rolling(20, min_periods=20).mean().reset_index(level=0, drop=True)
    df["vol_ma20"] = g["volume"].rolling(20, min_periods=20).mean().reset_index(level=0, drop=True)
    df["ma5_p1"] = g["ma5"].shift(1)
    df["ma20_p1"] = g["ma20"].shift(1)

    # HighTightFlag：40日/10日高低点 + 前20日均量（不含当日）
    df["high40"] = g["high"].rolling(40, min_periods=40).max().reset_index(level=0, drop=True)
    df["low40"] = g["low"].rolling(40, min_periods=40).min().reset_index(level=0, drop=True)
    df["high10"] = g["high"].rolling(10, min_periods=10).max().reset_index(level=0, drop=True)
    df["low10"] = g["low"].rolling(10, min_periods=10).min().reset_index(level=0, drop=True)
    df["vol_p1"] = g["volume"].shift(1)
    df["vol_ma20_p"] = g["vol_p1"].rolling(20, min_periods=20).mean().reset_index(level=0, drop=True)

    # LimitUpShakeout：前1/前2根K线
    df["close_p1"] = g["close"].shift(1)
    df["close_p2"] = g["close"].shift(2)

    # UptrendLimitDown：60日线（含前一日均线）
    df["ma60"] = g["close"].rolling(60, min_periods=60).mean().reset_index(level=0, drop=True)
    df["ma60_p1"] = g["ma60"].shift(1)

    # RpsBreakout：120日涨幅 + 120日最高（min_periods=60 与旧版一致）
    df["close_p120"] = g["close"].shift(120)
    df["pct120"] = (df["close"] - df["close_p120"]) / df["close_p120"]
    df["roll_high120"] = g["high"].rolling(120, min_periods=60).max().reset_index(level=0, drop=True)

    return df


def prepare(df: pd.DataFrame) -> pd.DataFrame:
    """按 symbol,date 排序并一次性计算指标列，返回可直接 evaluate_day 的 DataFrame。"""
    if df is None or df.empty:
        return df
    df = df.sort_values(["symbol", "date"]).reset_index(drop=True)
    return compute_indicators(df)


def _day_last(ind_df: pd.DataFrame, day: str) -> pd.DataFrame:
    """返回该日每只股票的最后一根K线（date<=day）及累计历史条数，索引为 symbol。"""
    sub = ind_df[ind_df["date"] <= day]
    last = sub.groupby("symbol", sort=False).tail(1).set_index("symbol")
    cnt = sub.groupby("symbol", sort=False).size()
    last["cnt"] = cnt
    return last


# ── 各策略：输入 per-symbol 的 last 行，输出 (matched: bool Series, reasons: str Series) ──

def _eval_turtle(row: pd.DataFrame):
    reasons = pd.Series("", index=row.index, dtype=object)
    short = row["cnt"] < 21
    reasons[short] = "历史K线不足(需>20根)"
    ok = ~short
    win = ok & row["high20"].isna()
    reasons[win] = "历史K线不足(20日窗口不完整)"
    ok = ok & ~win

    breakout = row["close"] > row["high20"]
    liquid = row["turnover"] > 100_000_000
    is_yang = row["close"] > row["open"]
    is_up = row["close"] > row["close_p1"]
    matched = ok & breakout & liquid & is_yang & is_up

    m = ok & ~breakout
    reasons[m] = "未突破20日新高"
    m = ok & breakout & ~liquid
    reasons[m] = "成交额不足1亿"
    m = ok & breakout & liquid & ~(is_yang & is_up)
    reasons[m] = "防诱多不过(非阳线/未真涨)"
    return matched, reasons


def _eval_ma_volume(row: pd.DataFrame):
    reasons = pd.Series("", index=row.index, dtype=object)
    short = row["cnt"] < 20
    reasons[short] = "历史K线不足(需>19根)"
    ok = ~short

    golden = (row["ma5_p1"] < row["ma20_p1"]) & (row["ma5"] > row["ma20"])
    surge = row["volume"] > row["vol_ma20"] * 1.5
    matched = ok & golden & surge

    m = ok & ~golden
    reasons[m] = "均线未形成金叉"
    m = ok & golden & ~surge
    reasons[m] = "量能未达20日均量1.5倍"
    return matched, reasons


def _eval_flag(row: pd.DataFrame):
    reasons = pd.Series("", index=row.index, dtype=object)
    short = row["cnt"] < 40
    reasons[short] = "历史K线不足(需>39根)"
    ok = ~short

    zero = ok & ((row["low40"] == 0) | (row["low10"] == 0))
    reasons[zero] = "数据异常(最低价为0)"
    ok = ok & ~zero

    momentum = ok & (row["high40"] / row["low40"] > 1.6)
    consolidation = ok & (row["high10"] / row["low10"] < 1.15)
    high_level = ok & (row["low10"] >= row["high40"] * 0.8)
    shrink = ok & (row["volume"] < row["vol_ma20_p"] * 0.6)
    matched = momentum & consolidation & high_level & shrink

    m = ok & ~momentum
    reasons[m] = "40日涨幅未达60%"
    m = ok & momentum & ~consolidation
    reasons[m] = "10日振幅未收敛(<15%)"
    m = ok & momentum & consolidation & ~high_level
    reasons[m] = "非高位抗跌(回撤过深)"
    m = ok & momentum & consolidation & high_level & ~shrink
    reasons[m] = "未缩量"
    return matched, reasons


def _eval_shakeout(row: pd.DataFrame):
    reasons = pd.Series("", index=row.index, dtype=object)
    short = row["cnt"] < 3
    reasons[short] = "历史K线不足(需≥3根)"
    ok = ~short

    limit_up = row["close_p1"] >= row["close_p2"] * 1.095
    bearish = row["close"] < row["open"]
    surge = row["volume"] > row["vol_p1"] * 2.0
    support = row["low"] >= row["close_p1"]
    matched = ok & limit_up & bearish & surge & support

    m = ok & ~limit_up
    reasons[m] = "昨日未涨停"
    m = ok & limit_up & ~bearish
    reasons[m] = "今日非收阴"
    m = ok & limit_up & bearish & ~surge
    reasons[m] = "今日未放量(>昨日2倍)"
    m = ok & limit_up & bearish & surge & ~support
    reasons[m] = "今日最低跌破昨收"
    return matched, reasons


def _eval_uptrend(row: pd.DataFrame):
    reasons = pd.Series("", index=row.index, dtype=object)
    short = row["cnt"] < 60
    reasons[short] = "历史K线不足(需≥60根)"
    ok = ~short

    nan = ok & (row["ma20_p1"].isna() | row["ma60_p1"].isna() | row["vol_ma20"].isna())
    reasons[nan] = "历史K线不足(均线未成形)"
    ok = ok & ~nan

    uptrend = row["ma20_p1"] > row["ma60_p1"]
    limit_down = row["close"] <= row["close_p1"] * 0.905
    surge = row["volume"] > row["vol_ma20"] * 2.0
    matched = ok & uptrend & limit_down & surge

    m = ok & ~uptrend
    reasons[m] = "非上升趋势(20日线≤60日线)"
    m = ok & uptrend & ~limit_down
    reasons[m] = "今日未跌停(未跌逾9.5%)"
    m = ok & uptrend & limit_down & ~surge
    reasons[m] = "未放量(未达20日均量2倍)"
    return matched, reasons


def _eval_rps(ind_df: pd.DataFrame, day: str):
    """RPS 需全市场横向排位，只取当日确有行情记录的股票（与旧版语义一致）。"""
    day_df = ind_df[ind_df["date"] == day]
    total = int(day_df["symbol"].nunique())

    insuff = day_df[pd.isna(day_df["pct120"])]
    ranked = day_df.dropna(subset=["pct120"]).copy()
    ranked["rps"] = ranked["pct120"].rank(pct=True) * 100
    below = ranked[ranked["rps"] < 90]
    strong = ranked[ranked["rps"] >= 90]

    # 旧版 strong.merge(roll_high) 为 inner join → 只保留 roll_high120 非空的行
    joined = strong[strong["roll_high120"].notna()]
    selected = joined[joined["close"] >= joined["roll_high120"] * 0.90]

    reasons: dict = {}
    samples: dict = {}
    if len(insuff):
        reasons["历史K线不足(无120日涨幅)"] = int(len(insuff))
        samples["历史K线不足(无120日涨幅)"] = [insuff.iloc[0]["symbol"]]
    if len(below):
        reasons["RPS相对强度未达前10%"] = int(len(below))
        samples["RPS相对强度未达前10%"] = [below.iloc[0]["symbol"]]
    not_break = joined[joined["close"] < joined["roll_high120"] * 0.90]
    if len(not_break):
        reasons["未接近120日新高(收盘价<最高*0.9)"] = int(len(not_break))
        samples["未接近120日新高(收盘价<最高*0.9)"] = [not_break.iloc[0]["symbol"]]

    stats = {"total": total, "matched": len(selected), "reasons": reasons, "samples": samples}
    return selected["symbol"].tolist(), stats


def _make_stats(row: pd.DataFrame, matched: pd.Series, reasons: pd.Series) -> dict:
    """汇总统计：total/matched/reasons/samples（samples 每原因最多抽样5只）。"""
    reason_counts: dict = {}
    samples: dict = {}
    for r in sorted(set(reasons[reasons != ""])):
        mask = reasons == r
        reason_counts[r] = int(mask.sum())
        samples[r] = row.index[mask].tolist()[:5]
    return {
        "total": int(len(row)),
        "matched": int(matched.sum()),
        "reasons": reason_counts,
        "samples": samples,
    }


# ── 策略元数据 & 按日评估入口 ──

STRATEGY_META = [
    {"key": "TurtleTradeStrategy", "name": "TurtleTrade 海龟突破",
     "desc": "20日新高 + 成交额>1亿 + 阳线防诱多", "fn": _eval_turtle},
    {"key": "MaVolumeStrategy", "name": "MaVolume 均线放量",
     "desc": "5日均线上穿20日均线金叉 + 成交量>20日均量1.5倍", "fn": _eval_ma_volume},
    {"key": "HighTightFlagStrategy", "name": "HighTightFlag 高窄旗形",
     "desc": "40日涨幅>60% + 10日振幅<15% + 近10日高位 + 缩量", "fn": _eval_flag},
    {"key": "LimitUpShakeoutStrategy", "name": "LimitUpShakeout 涨停洗盘",
     "desc": "昨日涨停 + 今日收阴放量 + 不破昨收", "fn": _eval_shakeout},
    {"key": "UptrendLimitDownStrategy", "name": "UptrendLimitDown 上升跌停",
     "desc": "20日线>60日线(上升趋势) + 放量跌停", "fn": _eval_uptrend},
    {"key": "RpsBreakoutStrategy", "name": "RpsBreakout RPS突破",
     "desc": "120日相对强度前10% + 收盘价>=120日最高*0.90", "fn": _eval_rps},
]


def evaluate_day(ind_df: pd.DataFrame, day: str, keys: list[str] | None = None) -> dict:
    """对某交易日运行选中策略。

    ind_df 为 prepare() 后的全表（须覆盖 day 之前足够历史）。
    Returns: {key: {name, desc, count, symbols, stats}}
    """
    if ind_df is None or ind_df.empty:
        return {}
    last = _day_last(ind_df, day)
    results: dict = {}
    for meta in STRATEGY_META:
        key = meta["key"]
        if keys and key not in keys:
            continue
        try:
            if meta["fn"] is _eval_rps:
                selected, stats = _eval_rps(ind_df, day)
            else:
                matched, reasons = meta["fn"](last)
                selected = last.index[matched].tolist()
                stats = _make_stats(last, matched, reasons)
            results[key] = {
                "name": meta["name"], "desc": meta["desc"],
                "count": len(selected), "symbols": selected, "stats": stats,
            }
        except Exception as exc:  # noqa: BLE001
            results[key] = {
                "name": meta["name"], "desc": meta["desc"],
                "count": 0, "symbols": [], "error": str(exc),
            }
    return results


def build_strategies(engine: DataEngine | None = None, threads: int = 1, preload_df: pd.DataFrame | None = None) -> dict:
    """兼容层：返回 {key: 元数据}，供 /api/strategies 展示（评估已向量化，无需实例）。"""
    return {m["key"]: m for m in STRATEGY_META}
