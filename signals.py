import asyncio
import logging
import time
from dataclasses import dataclass

from binance_client import binance, KlineData
from config import config

logger = logging.getLogger(__name__)


@dataclass
class Signal:
    symbol: str
    signal_type: str  # pump / dump
    price_change_pct: float
    current_price: float
    volume: float
    quote_volume: float
    volume_ratio: float  # current volume / average volume
    timeframe_minutes: int


class SignalDetector:
    def __init__(self):
        self._last_signal: dict[str, float] = {}  # symbol -> timestamp
        self._volume_cache: dict[str, float] = {}  # symbol -> avg quote volume
        self._volume_cache_time: float = 0

    def _is_spam(self, symbol: str) -> bool:
        now = time.time()
        last = self._last_signal.get(symbol, 0)
        if now - last < config.ANTISPAM_COOLDOWN:
            return True
        return False

    def _mark_sent(self, symbol: str):
        self._last_signal[symbol] = time.time()

    async def _update_volume_cache(self, symbols: list[str]):
        now = time.time()
        if now - self._volume_cache_time < 300 and self._volume_cache:
            return

        semaphore = asyncio.Semaphore(30)

        async def fetch_avg(sym: str):
            async with semaphore:
                kl = await binance.get_klines(sym, "1h", limit=24)
                if kl:
                    avg = sum(k.quote_volume for k in kl) / len(kl)
                    return sym, avg
                return sym, 0.0

        tasks = [fetch_avg(s) for s in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, tuple):
                self._volume_cache[r[0]] = r[1]
        self._volume_cache_time = now
        logger.info(f"Updated volume cache for {len(self._volume_cache)} symbols")

    @staticmethod
    def _timeframe_to_binance(tf: int) -> str:
        return f"{tf}m"

    def _format_signal_msg(self, sig: Signal) -> str:
        if sig.signal_type == "pump":
            emoji = "\U0001f680"
            arrow = "+"
        else:
            emoji = "\U0001f4c9"
            arrow = ""

        vol_text = (
            f"x{sig.volume_ratio:.1f} от среднего"
            if sig.volume_ratio >= 1.5
            else "нормальный"
        )

        lines = [
            f"{emoji} <b>{sig.symbol}</b>",
            f"Изменение: <b>{arrow}{sig.price_change_pct:.2f}%</b> за {sig.timeframe_minutes} мин",
            f"Цена: {sig.current_price:.6g}",
            f"Объём: {vol_text}",
            f"Тип: <b>{sig.signal_type.upper()}</b>",
        ]
        return "\n".join(lines)

    async def scan(
        self,
        user_settings: list[dict],
    ) -> list[tuple[int, str, Signal]]:
        """
        Scan all symbols, return list of (user_id, message_text, Signal) to send.
        """
        symbols = await binance.get_usdt_symbols()
        if not symbols:
            return []

        # Determine unique timeframes needed
        tf_needed = {s["timeframe_minutes"] for s in user_settings}
        interval_map: dict[str, list[int]] = {}
        for tf in tf_needed:
            interval = self._timeframe_to_binance(tf)
            interval_map.setdefault(interval, []).append(tf)

        # Fetch klines for each interval
        klines_by_interval: dict[str, dict[str, list[KlineData]]] = {}
        for interval in interval_map:
            klines_by_interval[interval] = await binance.get_batch_klines(
                symbols, interval, limit=2
            )

        # Update volume cache if volume filter is used
        volume_filter_users = [s for s in user_settings if s["volume_filter_enabled"]]
        if volume_filter_users:
            await self._update_volume_cache(symbols)

        # Detect signals
        detected_signals: dict[str, Signal] = {}  # key: f"{symbol}_{tf}"

        for interval, tfs in interval_map.items():
            batch = klines_by_interval.get(interval, {})
            for sym, klines in batch.items():
                if len(klines) < 2:
                    continue
                prev, curr = klines[0], klines[1]
                if prev.open_price == 0:
                    continue

                change_pct = ((curr.close_price - prev.open_price) / prev.open_price) * 100
                if abs(change_pct) < 0.1:
                    continue

                avg_vol = self._volume_cache.get(sym, 0)
                vol_ratio = (curr.quote_volume / avg_vol) if avg_vol > 0 else 0

                for tf in tfs:
                    key = f"{sym}_{tf}"
                    sig_type = "pump" if change_pct > 0 else "dump"
                    detected_signals[key] = Signal(
                        symbol=sym,
                        signal_type=sig_type,
                        price_change_pct=change_pct,
                        current_price=curr.close_price,
                        volume=curr.volume,
                        quote_volume=curr.quote_volume,
                        volume_ratio=vol_ratio,
                        timeframe_minutes=tf,
                    )

        # Match signals to users
        messages: list[tuple[int, str, Signal]] = []
        for user in user_settings:
            tf = user["timeframe_minutes"]
            pct_threshold = user["price_change_pct"]
            sig_type_filter = user["signal_type"]  # pump / dump / both
            vol_enabled = user["volume_filter_enabled"]
            vol_mult = user["volume_multiplier"]
            uid = user["user_id"]

            for key, sig in detected_signals.items():
                if sig.timeframe_minutes != tf:
                    continue
                if abs(sig.price_change_pct) < pct_threshold:
                    continue
                if sig_type_filter == "pump" and sig.signal_type != "pump":
                    continue
                if sig_type_filter == "dump" and sig.signal_type != "dump":
                    continue
                if vol_enabled and sig.volume_ratio < vol_mult:
                    continue
                if self._is_spam(sig.symbol):
                    continue

                self._mark_sent(sig.symbol)
                msg = self._format_signal_msg(sig)
                messages.append((uid, msg, sig))

        return messages


detector = SignalDetector()
