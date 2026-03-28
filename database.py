import aiosqlite
from config import config


class Database:
    def __init__(self, db_path: str = config.DB_PATH):
        self.db_path = db_path
        self.db: aiosqlite.Connection | None = None

    async def connect(self):
        self.db = await aiosqlite.connect(self.db_path)
        self.db.row_factory = aiosqlite.Row
        await self._create_tables()

    async def close(self):
        if self.db:
            await self.db.close()

    async def _create_tables(self):
        await self.db.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                is_active INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS user_settings (
                user_id INTEGER PRIMARY KEY,
                price_change_pct REAL DEFAULT 3.0,
                timeframe_minutes INTEGER DEFAULT 5,
                volume_filter_enabled INTEGER DEFAULT 0,
                volume_multiplier REAL DEFAULT 2.0,
                signal_type TEXT DEFAULT 'both',
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            );
            CREATE TABLE IF NOT EXISTS signal_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                signal_type TEXT NOT NULL,
                price_change_pct REAL,
                volume_multiplier REAL,
                timeframe_minutes INTEGER,
                sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        await self.db.commit()

    async def register_user(self, user_id: int, username: str | None, first_name: str | None):
        await self.db.execute(
            """INSERT OR IGNORE INTO users (user_id, username, first_name)
               VALUES (?, ?, ?)""",
            (user_id, username, first_name),
        )
        await self.db.execute(
            """INSERT OR IGNORE INTO user_settings (user_id) VALUES (?)""",
            (user_id,),
        )
        await self.db.commit()

    async def get_user(self, user_id: int) -> dict | None:
        cursor = await self.db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        row = await cursor.fetchone()
        return dict(row) if row else None

    async def get_settings(self, user_id: int) -> dict | None:
        cursor = await self.db.execute(
            "SELECT * FROM user_settings WHERE user_id = ?", (user_id,)
        )
        row = await cursor.fetchone()
        return dict(row) if row else None

    async def update_settings(self, user_id: int, **kwargs):
        allowed = {
            "price_change_pct", "timeframe_minutes",
            "volume_filter_enabled", "volume_multiplier", "signal_type",
        }
        fields = {k: v for k, v in kwargs.items() if k in allowed}
        if not fields:
            return
        set_clause = ", ".join(f"{k} = ?" for k in fields)
        values = list(fields.values()) + [user_id]
        await self.db.execute(
            f"UPDATE user_settings SET {set_clause} WHERE user_id = ?", values
        )
        await self.db.commit()

    async def set_active(self, user_id: int, active: bool):
        await self.db.execute(
            "UPDATE users SET is_active = ? WHERE user_id = ?",
            (1 if active else 0, user_id),
        )
        await self.db.commit()

    async def get_active_users(self) -> list[dict]:
        cursor = await self.db.execute(
            """SELECT u.user_id, s.* FROM users u
               JOIN user_settings s ON u.user_id = s.user_id
               WHERE u.is_active = 1"""
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def log_signal(self, symbol: str, signal_type: str,
                         price_change_pct: float, volume_multiplier: float,
                         timeframe_minutes: int):
        await self.db.execute(
            """INSERT INTO signal_log
               (symbol, signal_type, price_change_pct, volume_multiplier, timeframe_minutes)
               VALUES (?, ?, ?, ?, ?)""",
            (symbol, signal_type, price_change_pct, volume_multiplier, timeframe_minutes),
        )
        await self.db.commit()


db = Database()
