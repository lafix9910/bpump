import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


@dataclass
class Config:
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
    DB_PATH: str = "pump_dump_bot.db"

    # Binance
    BINANCE_FUTURES_REST: str = "https://fapi.binance.com"
    BINANCE_FUTURES_WS: str = "wss://fstream.binance.com/ws"

    # Default user settings
    DEFAULT_PRICE_CHANGE_PCT: float = 3.0
    DEFAULT_TIMEFRAME_MINUTES: int = 5
    DEFAULT_VOLUME_FILTER_ENABLED: bool = False
    DEFAULT_VOLUME_MULTIPLIER: float = 2.0
    DEFAULT_SIGNAL_TYPE: str = "both"  # pump / dump / both

    # Scan interval (seconds)
    SCAN_INTERVAL: int = 5

    # Anti-spam: how many seconds between repeated signals for same symbol
    ANTISPAM_COOLDOWN: int = 300  # 5 minutes

    # Available timeframes for settings
    TIMEFRAMES: list[int] = field(default_factory=lambda: [1, 3, 5, 15, 30])

    def __post_init__(self):
        if self.BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
            raise ValueError(
                "BOT_TOKEN не задан. Установите переменную окружения BOT_TOKEN или "
                "отредактируйте config.py"
            )


config = Config()
