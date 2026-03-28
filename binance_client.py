import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime

import aiohttp

from config import config

logger = logging.getLogger(__name__)


@dataclass
class KlineData:
    symbol: str
    open_time: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float
    quote_volume: float
    trades_count: int


class BinanceClient:
    def __init__(self):
        self.base_url = config.BINANCE_FUTURES_REST
        self.session: aiohttp.ClientSession | None = None
        self._symbols: list[str] = []
        self._symbols_loaded = False

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    async def get_usdt_symbols(self) -> list[str]:
        if self._symbols_loaded and self._symbols:
            return self._symbols

        session = await self._get_session()
        url = f"{self.base_url}/fapi/v1/exchangeInfo"
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                data = await resp.json()
                self._symbols = [
                    s["symbol"]
                    for s in data["symbols"]
                    if s["quoteAsset"] == "USDT"
                    and s["status"] == "TRADING"
                    and s["contractType"] == "PERPETUAL"
                ]
                self._symbols_loaded = True
                logger.info(f"Loaded {len(self._symbols)} USDT perpetual symbols")
                return self._symbols
        except Exception as e:
            logger.error(f"Error fetching symbols: {e}")
            return self._symbols if self._symbols else []

    async def get_klines(self, symbol: str, interval: str, limit: int = 2) -> list[KlineData]:
        session = await self._get_session()
        url = f"{self.base_url}/fapi/v1/klines"
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        try:
            async with session.get(url, params=params,
                                   timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()
                result = []
                for k in data:
                    result.append(KlineData(
                        symbol=symbol,
                        open_time=datetime.fromtimestamp(k[0] / 1000),
                        open_price=float(k[1]),
                        high_price=float(k[2]),
                        low_price=float(k[3]),
                        close_price=float(k[4]),
                        volume=float(k[5]),
                        quote_volume=float(k[7]),
                        trades_count=int(k[8]),
                    ))
                return result
        except Exception as e:
            logger.debug(f"Error fetching klines for {symbol}: {e}")
            return []

    async def get_batch_klines(
        self, symbols: list[str], interval: str, limit: int = 2
    ) -> dict[str, list[KlineData]]:
        semaphore = asyncio.Semaphore(30)

        async def fetch(sym: str) -> tuple[str, list[KlineData]]:
            async with semaphore:
                kl = await self.get_klines(sym, interval, limit)
                return sym, kl

        tasks = [fetch(s) for s in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        out: dict[str, list[KlineData]] = {}
        for r in results:
            if isinstance(r, tuple):
                out[r[0]] = r[1]
        return out


binance = BinanceClient()
