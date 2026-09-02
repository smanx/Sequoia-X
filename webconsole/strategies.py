"""策略集：复制改造自原项目 sequoia_x/strategy/*.py。

核心改动：
1. 所有策略的 run(as_of_date=None) 支持指定任意交易日回算。
2. 每个策略在运行时记录统计 self.stats：
     total      已分析的股票数量（参与判定的条数）
     matched    符合策略的数量
     reasons    不符合原因分布 {原因: 数量}
     samples    各原因抽样代码 {原因: [代码...]}
   供前端在每个策略 Tab 中展示"分析了多少 / 符合不合格 / 不符原因"。

说明：原项目 PrivatePlacementStrategy 基于外部公告，无法对历史日期回算，故不包含。
"""

import pandas as pd

from engine import DataEngine


class BaseStrategy:
    """策略基类：提供统一统计容器与计数 API。"""

    webhook_key: str = "default"
    name: str = "策略"
    desc: str = ""

    def __init__(self, engine: DataEngine) -> None:
        self.engine = engine
        self.stats: dict = {}

    def _begin(self) -> None:
        """每次 run 开始时重置统计。"""
        self.stats = {"total": 0, "matched": 0, "reasons": {}, "samples": {}}

    def _note(self, reason: str, symbol: str, max_samples: int = 5) -> None:
        """记录一条不符原因及其抽样代码。"""
        self.stats["reasons"][reason] = self.stats["reasons"].get(reason, 0) + 1
        bucket = self.stats["samples"].setdefault(reason, [])
        if len(bucket) < max_samples:
            bucket.append(symbol)

    def run(self, as_of_date: str | None = None) -> list[str]:  # pragma: no cover
        raise NotImplementedError


# ── 海龟突破 ──
class TurtleTradeStrategy(BaseStrategy):
    """海龟突破：20日新高突破 + 成交额过亿 + 实体阳线防诱多。"""

    webhook_key = "turtle"
    name = "TurtleTrade 海龟突破"
    desc = "20日新高 + 成交额>1亿 + 阳线防诱多"
    _MIN_BARS = 21

    def run(self, as_of_date: str | None = None) -> list[str]:
        self._begin()
        symbols = self.engine.get_local_symbols()
        candidates: list[str] = []

        for symbol in symbols:
            self.stats["total"] += 1
            try:
                df = self.engine.get_ohlcv(symbol, as_of_date)
                if len(df) < self._MIN_BARS:
                    self._note("历史K线不足(需>20根)", symbol)
                    continue

                df["high_20"] = df["high"].shift(1).rolling(20).max()
                last = df.iloc[-1]
                prev = df.iloc[-2]

                if pd.isna(last["high_20"]):
                    self._note("历史K线不足(20日窗口不完整)", symbol)
                    continue

                breakout = last["close"] > last["high_20"]
                liquid = last["turnover"] > 100_000_000
                is_yang = last["close"] > last["open"]
                is_up = last["close"] > prev["close"]

                if breakout and liquid and is_yang and is_up:
                    candidates.append(symbol)
                elif not breakout:
                    self._note("未突破20日新高", symbol)
                elif not liquid:
                    self._note("成交额不足1亿", symbol)
                else:
                    self._note("防诱多不过(非阳线/未真涨)", symbol)
            except Exception:
                self._note("计算异常", symbol)

        self.stats["matched"] = len(candidates)
        return candidates


# ── 均线放量 ──
class MaVolumeStrategy(BaseStrategy):
    """均线+放量：5日均线上穿20日均线（金叉）且放量确认。"""

    webhook_key = "ma_volume"
    name = "MaVolume 均线放量"
    desc = "5日均线上穿20日均线金叉 + 成交量>20日均量1.5倍"
    _MIN_BARS = 20

    def run(self, as_of_date: str | None = None) -> list[str]:
        self._begin()
        symbols = self.engine.get_local_symbols()
        selected: list[str] = []

        for symbol in symbols:
            self.stats["total"] += 1
            try:
                df = self.engine.get_ohlcv(symbol, as_of_date)
                if len(df) < self._MIN_BARS:
                    self._note("历史K线不足(需>19根)", symbol)
                    continue

                df["ma5"] = df["close"].rolling(5).mean()
                df["ma20"] = df["close"].rolling(20).mean()
                df["vol_ma20"] = df["volume"].rolling(20).mean()

                last = df.iloc[-1]
                prev = df.iloc[-2]

                golden_cross = (
                    prev["ma5"] < prev["ma20"]
                    and last["ma5"] > last["ma20"]
                )
                volume_surge = last["volume"] > last["vol_ma20"] * 1.5

                if golden_cross and volume_surge:
                    selected.append(symbol)
                elif not golden_cross:
                    self._note("均线未形成金叉", symbol)
                else:
                    self._note("量能未达20日均量1.5倍", symbol)
            except Exception:
                self._note("计算异常", symbol)

        self.stats["matched"] = len(selected)
        return selected


# ── 高窄旗形 ──
class HighTightFlagStrategy(BaseStrategy):
    """高而窄的旗形整理：强动量后极度收敛缩量。"""

    webhook_key = "flag"
    name = "HighTightFlag 高窄旗形"
    desc = "40日涨幅>60% + 10日振幅<15% + 近10日高位 + 缩量"
    _MIN_BARS = 40

    def run(self, as_of_date: str | None = None) -> list[str]:
        self._begin()
        symbols = self.engine.get_local_symbols()
        selected: list[str] = []

        for symbol in symbols:
            self.stats["total"] += 1
            try:
                df = self.engine.get_ohlcv(symbol, as_of_date)
                if len(df) < self._MIN_BARS:
                    self._note("历史K线不足(需>39根)", symbol)
                    continue

                tail40 = df.tail(40)
                tail10 = df.tail(10)

                high40 = tail40["high"].max()
                low40 = tail40["low"].min()
                high10 = tail10["high"].max()
                low10 = tail10["low"].min()

                if low40 == 0 or low10 == 0:
                    self._note("数据异常(最低价为0)", symbol)
                    continue

                momentum = high40 / low40 > 1.6
                consolidation = high10 / low10 < 1.15
                high_level = low10 >= high40 * 0.8
                vol_ma20 = df["volume"].iloc[-21:-1].mean()
                shrink = df["volume"].iloc[-1] < vol_ma20 * 0.6

                if momentum and consolidation and high_level and shrink:
                    selected.append(symbol)
                elif not momentum:
                    self._note("40日涨幅未达60%", symbol)
                elif not consolidation:
                    self._note("10日振幅未收敛(<15%)", symbol)
                elif not high_level:
                    self._note("非高位抗跌(回撤过深)", symbol)
                else:
                    self._note("未缩量", symbol)
            except Exception:
                self._note("计算异常", symbol)

        self.stats["matched"] = len(selected)
        return selected


# ── 涨停洗盘 ──
class LimitUpShakeoutStrategy(BaseStrategy):
    """涨停洗盘：昨日涨停后今日放量收阴但不破昨收。"""

    webhook_key = "shakeout"
    name = "LimitUpShakeout 涨停洗盘"
    desc = "昨日涨停 + 今日收阴放量 + 不破昨收"
    _MIN_BARS = 3

    def run(self, as_of_date: str | None = None) -> list[str]:
        self._begin()
        symbols = self.engine.get_local_symbols()
        selected: list[str] = []

        for symbol in symbols:
            self.stats["total"] += 1
            try:
                df = self.engine.get_ohlcv(symbol, as_of_date)
                if len(df) < self._MIN_BARS:
                    self._note("历史K线不足(需≥3根)", symbol)
                    continue

                prev2 = df.iloc[-3]
                prev1 = df.iloc[-2]
                today = df.iloc[-1]

                limit_up_yesterday = prev1["close"] >= prev2["close"] * 1.095
                bearish_today = today["close"] < today["open"]
                volume_surge = today["volume"] > prev1["volume"] * 2.0
                support_hold = today["low"] >= prev1["close"]

                if limit_up_yesterday and bearish_today and volume_surge and support_hold:
                    selected.append(symbol)
                elif not limit_up_yesterday:
                    self._note("昨日未涨停", symbol)
                elif not bearish_today:
                    self._note("今日非收阴", symbol)
                elif not volume_surge:
                    self._note("今日未放量(>昨日2倍)", symbol)
                else:
                    self._note("今日最低跌破昨收", symbol)
            except Exception:
                self._note("计算异常", symbol)

        self.stats["matched"] = len(selected)
        return selected


# ── 上升趋势跌停 ──
class UptrendLimitDownStrategy(BaseStrategy):
    """上升趋势跌停：趋势中放量跌停，捕捉错杀机会。"""

    webhook_key = "limit_down"
    name = "UptrendLimitDown 上升跌停"
    desc = "20日线>60日线(上升趋势) + 放量跌停"
    _MIN_BARS = 60

    def run(self, as_of_date: str | None = None) -> list[str]:
        self._begin()
        symbols = self.engine.get_local_symbols()
        selected: list[str] = []

        for symbol in symbols:
            self.stats["total"] += 1
            try:
                df = self.engine.get_ohlcv(symbol, as_of_date)
                if len(df) < self._MIN_BARS:
                    self._note("历史K线不足(需≥60根)", symbol)
                    continue

                df["ma20"] = df["close"].rolling(20).mean()
                df["ma60"] = df["close"].rolling(60).mean()
                df["vol_ma20"] = df["volume"].rolling(20).mean()

                prev = df.iloc[-2]
                today = df.iloc[-1]

                if pd.isna(prev["ma20"]) or pd.isna(prev["ma60"]) or pd.isna(today["vol_ma20"]):
                    self._note("历史K线不足(均线未成形)", symbol)
                    continue

                uptrend = prev["ma20"] > prev["ma60"]
                limit_down = today["close"] <= prev["close"] * 0.905
                volume_surge = today["volume"] > today["vol_ma20"] * 2.0

                if uptrend and limit_down and volume_surge:
                    selected.append(symbol)
                elif not uptrend:
                    self._note("非上升趋势(20日线≤60日线)", symbol)
                elif not limit_down:
                    self._note("今日未跌停(未跌逾9.5%)", symbol)
                else:
                    self._note("未放量(未达20日均量2倍)", symbol)
            except Exception:
                self._note("计算异常", symbol)

        self.stats["matched"] = len(selected)
        return selected


# ── RPS 突破 ──
class RpsBreakoutStrategy(BaseStrategy):
    """RPS 极强动量突破：120日相对强度 >= 90 分位且接近突破新高。"""

    webhook_key = "rps"
    name = "RpsBreakout RPS突破"
    desc = "120日相对强度前10% + 收盘价>=120日最高*0.90"
    rps_period = 120
    rps_threshold = 90

    def run(self, as_of_date: str | None = None) -> list[str]:
        self._begin()
        df = self.engine.get_ohlcv_all(as_of_date)
        if df.empty:
            self.stats["total"] = 0
            return []

        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values(["symbol", "date"])

        df["close_shift"] = df.groupby("symbol")["close"].shift(self.rps_period)
        df["pct_change"] = (df["close"] - df["close_shift"]) / df["close_shift"]

        latest_date = df["date"].max()
        latest_df = df[df["date"] == latest_date].copy()

        # 统计：当日参与评估的股票数
        self.stats["total"] = int(latest_df["symbol"].nunique())

        # 数据不足：当日无足够120日历史
        insufficient = latest_df[pd.isna(latest_df["pct_change"])]
        if len(insufficient):
            self._note("历史K线不足(无120日涨幅)", insufficient.iloc[0]["symbol"])

        # RPS 未达阈值
        ranked = latest_df.dropna(subset=["pct_change"]).copy()
        ranked["rps"] = ranked["pct_change"].rank(pct=True) * 100
        below_threshold = ranked[ranked["rps"] < self.rps_threshold]
        if len(below_threshold):
            self._note("RPS相对强度未达前10%", below_threshold.iloc[0]["symbol"])
            # 即使达到RPS也须再判突破
        strong_stocks = ranked[ranked["rps"] >= self.rps_threshold].copy()

        roll_high = df.groupby("symbol")["high"].rolling(
            window=self.rps_period, min_periods=self.rps_period // 2
        ).max().reset_index(level=0, drop=True)
        df["roll_high"] = roll_high
        latest_roll_high = df[df["date"] == latest_date][["symbol", "roll_high"]]
        joined = strong_stocks.merge(latest_roll_high, on="symbol")

        not_break = joined[joined["close"] < joined["roll_high"] * 0.90]
        if len(not_break):
            self._note("未接近120日新高(收盘价<最高*0.9)", not_break.iloc[0]["symbol"])

        selected = strong_stocks.merge(latest_roll_high, on="symbol")
        selected = selected[selected["close"] >= selected["roll_high"] * 0.90]

        self.stats["matched"] = len(selected)
        return selected["symbol"].tolist()


def build_strategies(engine: DataEngine) -> dict[str, BaseStrategy]:
    """实例化全部策略，返回 {策略键: 策略实例}。"""
    classes = [
        MaVolumeStrategy,
        TurtleTradeStrategy,
        HighTightFlagStrategy,
        LimitUpShakeoutStrategy,
        UptrendLimitDownStrategy,
        RpsBreakoutStrategy,
    ]
    return {type(c(engine)).__name__: c(engine) for c in classes}