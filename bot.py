import asyncio
import logging
import sys

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from binance_client import binance
from config import config
from database import db
from signals import detector

# ─── Logging ───────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bot.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ─── Bot ───────────────────────────────────────────────────────────────
bot = Bot(token=config.BOT_TOKEN)
dp = Dispatcher()
router = Router()
dp.include_router(router)

# ─── Helpers ───────────────────────────────────────────────────────────

SIGNAL_TYPES = ["both", "pump", "dump"]
TIMEFRAMES = config.TIMEFRAMES


def main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="\u2699\ufe0f Настройки", callback_data="menu:settings")],
        [InlineKeyboardButton(text="\U0001f514 Включить сигналы", callback_data="menu:start")],
        [InlineKeyboardButton(text="\U0001f6d1 Выключить сигналы", callback_data="menu:stop")],
        [InlineKeyboardButton(text="\U0001f4ca Статус", callback_data="menu:status")],
    ])


def settings_kb(settings: dict) -> InlineKeyboardMarkup:
    pct = settings["price_change_pct"]
    tf = settings["timeframe_minutes"]
    vol = "\u2705" if settings["volume_filter_enabled"] else "\u274c"
    vol_m = settings["volume_multiplier"]
    st = settings["signal_type"]

    st_labels = {"both": "\U0001f504 Оба", "pump": "\U0001f680 Только рост", "dump": "\U0001f4c9 Только падение"}

    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"\U0001f4c8 Порог: {pct}%", callback_data="set:pct"
        )],
        [InlineKeyboardButton(
            text=f"\u23f1\ufe0f Таймфрейм: {tf} мин", callback_data="set:tf"
        )],
        [InlineKeyboardButton(
            text=f"\U0001f4ca Фильтр объёма: {vol} (x{vol_m})", callback_data="set:vol"
        )],
        [InlineKeyboardButton(
            text=f"\U0001f503 Тип: {st_labels.get(st, st)}", callback_data="set:type"
        )],
        [InlineKeyboardButton(text="\u2190 Назад", callback_data="menu:main")],
    ])


def pct_kb() -> InlineKeyboardMarkup:
    values = [1.0, 2.0, 3.0, 5.0, 7.0, 10.0]
    rows = []
    row = []
    for i, v in enumerate(values):
        row.append(InlineKeyboardButton(text=f"{v}%", callback_data=f"pct:{v}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text="\u2190 Назад", callback_data="menu:settings")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def tf_kb() -> InlineKeyboardMarkup:
    rows = []
    row = []
    for i, tf in enumerate(TIMEFRAMES):
        row.append(InlineKeyboardButton(text=f"{tf} мин", callback_data=f"tf:{tf}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text="\u2190 Назад", callback_data="menu:settings")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def vol_kb() -> InlineKeyboardMarkup:
    mults = [1.5, 2.0, 3.0, 5.0]
    rows = [
        [InlineKeyboardButton(text="\u2705 Включить", callback_data="vol:on"),
         InlineKeyboardButton(text="\u274c Выключить", callback_data="vol:off")],
    ]
    row = []
    for m in mults:
        row.append(InlineKeyboardButton(text=f"x{m}", callback_data=f"volm:{m}"))
    rows.append(row)
    rows.append([InlineKeyboardButton(text="\u2190 Назад", callback_data="menu:settings")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def type_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="\U0001f504 Оба (pump + dump)", callback_data="stype:both")],
        [InlineKeyboardButton(text="\U0001f680 Только рост (pump)", callback_data="stype:pump")],
        [InlineKeyboardButton(text="\U0001f4c9 Только падение (dump)", callback_data="stype:dump")],
        [InlineKeyboardButton(text="\u2190 Назад", callback_data="menu:settings")],
    ])


async def ensure_user(message_or_callback) -> int:
    if isinstance(message_or_callback, Message):
        u = message_or_callback.from_user
    else:
        u = message_or_callback.from_user
    assert u is not None
    await db.register_user(u.id, u.username, u.first_name)
    return u.id


# ─── Command handlers ──────────────────────────────────────────────────

@router.message(CommandStart())
async def cmd_start(message: Message):
    await ensure_user(message)
    text = (
        "\U0001f4ca <b>Pump & Dump Monitor Bot</b>\n\n"
        "Бот отслеживает все USDT-фьючерсные пары на Binance "
        "и отправляет сигналы при сильных движениях цены.\n\n"
        "\U0001f4cc <b>Команды:</b>\n"
        "/settings \u2014 настройки\n"
        "/status \u2014 текущие настройки\n"
        "/start_signals \u2014 включить сигналы\n"
        "/stop_signals \u2014 выключить сигналы\n"
    )
    await message.answer(text, parse_mode="HTML", reply_markup=main_menu_kb())


@router.message(Command("settings"))
async def cmd_settings(message: Message):
    uid = await ensure_user(message)
    settings = await db.get_settings(uid)
    await message.answer(
        "\u2699\ufe0f <b>Настройки</b>\nВыберите параметр для изменения:",
        parse_mode="HTML",
        reply_markup=settings_kb(settings),
    )


@router.message(Command("status"))
async def cmd_status(message: Message):
    uid = await ensure_user(message)
    user = await db.get_user(uid)
    settings = await db.get_settings(uid)
    active = "\u2705 Активен" if user["is_active"] else "\u274c Неактивен"
    st_labels = {"both": "\U0001f504 Оба", "pump": "\U0001f680 Только рост", "dump": "\U0001f4c9 Только падение"}
    vol = "\u2705" if settings["volume_filter_enabled"] else "\u274c"
    text = (
        f"\U0001f4ca <b>Статус</b>\n\n"
        f"Статус: {active}\n"
        f"Порог изменения: {settings['price_change_pct']}%\n"
        f"Таймфрейм: {settings['timeframe_minutes']} мин\n"
        f"Фильтр объёма: {vol} (x{settings['volume_multiplier']})\n"
        f"Тип сигналов: {st_labels.get(settings['signal_type'], settings['signal_type'])}"
    )
    await message.answer(text, parse_mode="HTML", reply_markup=main_menu_kb())


@router.message(Command("start_signals"))
async def cmd_start_signals(message: Message):
    uid = await ensure_user(message)
    await db.set_active(uid, True)
    await message.answer("\U0001f514 Сигналы <b>включены</b>!", parse_mode="HTML")


@router.message(Command("stop_signals"))
async def cmd_stop_signals(message: Message):
    uid = await ensure_user(message)
    await db.set_active(uid, False)
    await message.answer("\U0001f6d1 Сигналы <b>выключены</b>!", parse_mode="HTML")


# ─── Callback handlers ────────────────────────────────────────────────

@router.callback_query(F.data == "menu:main")
async def cb_main(cb: CallbackQuery):
    await cb.message.edit_text(
        "\U0001f4ca <b>Pump & Dump Monitor Bot</b>\n\nВыберите действие:",
        parse_mode="HTML",
        reply_markup=main_menu_kb(),
    )
    await cb.answer()


@router.callback_query(F.data == "menu:settings")
async def cb_settings(cb: CallbackQuery):
    settings = await db.get_settings(cb.from_user.id)
    await cb.message.edit_text(
        "\u2699\ufe0f <b>Настройки</b>\nВыберите параметр:",
        parse_mode="HTML",
        reply_markup=settings_kb(settings),
    )
    await cb.answer()


@router.callback_query(F.data == "menu:status")
async def cb_status(cb: CallbackQuery):
    user = await db.get_user(cb.from_user.id)
    settings = await db.get_settings(cb.from_user.id)
    active = "\u2705 Активен" if user["is_active"] else "\u274c Неактивен"
    st_labels = {"both": "\U0001f504 Оба", "pump": "\U0001f680 Только рост", "dump": "\U0001f4c9 Только падение"}
    vol = "\u2705" if settings["volume_filter_enabled"] else "\u274c"
    text = (
        f"\U0001f4ca <b>Статус</b>\n\n"
        f"Статус: {active}\n"
        f"Порог изменения: {settings['price_change_pct']}%\n"
        f"Таймфрейм: {settings['timeframe_minutes']} мин\n"
        f"Фильтр объёма: {vol} (x{settings['volume_multiplier']})\n"
        f"Тип сигналов: {st_labels.get(settings['signal_type'], settings['signal_type'])}"
    )
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=main_menu_kb())
    await cb.answer()


@router.callback_query(F.data == "menu:start")
async def cb_start(cb: CallbackQuery):
    await db.set_active(cb.from_user.id, True)
    await cb.answer("\u2705 Сигналы включены!", show_alert=True)


@router.callback_query(F.data == "menu:stop")
async def cb_stop(cb: CallbackQuery):
    await db.set_active(cb.from_user.id, False)
    await cb.answer("\u274c Сигналы выключены!", show_alert=True)


# ── Percentage threshold ───────────────────────────────────────────────

@router.callback_query(F.data == "set:pct")
async def cb_set_pct(cb: CallbackQuery):
    await cb.message.edit_text(
        "\U0001f4c8 Выберите порог изменения цены (%):",
        reply_markup=pct_kb(),
    )
    await cb.answer()


@router.callback_query(F.data.startswith("pct:"))
async def cb_pct_value(cb: CallbackQuery):
    val = float(cb.data.split(":")[1])
    await db.update_settings(cb.from_user.id, price_change_pct=val)
    settings = await db.get_settings(cb.from_user.id)
    await cb.message.edit_text(
        f"\u2705 Порог установлен: <b>{val}%</b>",
        parse_mode="HTML",
        reply_markup=settings_kb(settings),
    )
    await cb.answer()


# ── Timeframe ──────────────────────────────────────────────────────────

@router.callback_query(F.data == "set:tf")
async def cb_set_tf(cb: CallbackQuery):
    await cb.message.edit_text(
        "\u23f1\ufe0f Выберите таймфрейм:",
        reply_markup=tf_kb(),
    )
    await cb.answer()


@router.callback_query(F.data.startswith("tf:"))
async def cb_tf_value(cb: CallbackQuery):
    val = int(cb.data.split(":")[1])
    await db.update_settings(cb.from_user.id, timeframe_minutes=val)
    settings = await db.get_settings(cb.from_user.id)
    await cb.message.edit_text(
        f"\u2705 Таймфрейм: <b>{val} мин</b>",
        parse_mode="HTML",
        reply_markup=settings_kb(settings),
    )
    await cb.answer()


# ── Volume filter ──────────────────────────────────────────────────────

@router.callback_query(F.data == "set:vol")
async def cb_set_vol(cb: CallbackQuery):
    await cb.message.edit_text(
        "\U0001f4ca Настройка фильтра объёма:",
        reply_markup=vol_kb(),
    )
    await cb.answer()


@router.callback_query(F.data == "vol:on")
async def cb_vol_on(cb: CallbackQuery):
    await db.update_settings(cb.from_user.id, volume_filter_enabled=1)
    settings = await db.get_settings(cb.from_user.id)
    await cb.message.edit_text(
        "\u2705 Фильтр объёма включён",
        reply_markup=settings_kb(settings),
    )
    await cb.answer()


@router.callback_query(F.data == "vol:off")
async def cb_vol_off(cb: CallbackQuery):
    await db.update_settings(cb.from_user.id, volume_filter_enabled=0)
    settings = await db.get_settings(cb.from_user.id)
    await cb.message.edit_text(
        "\u274c Фильтр объёма выключен",
        reply_markup=settings_kb(settings),
    )
    await cb.answer()


@router.callback_query(F.data.startswith("volm:"))
async def cb_vol_mult(cb: CallbackQuery):
    val = float(cb.data.split(":")[1])
    await db.update_settings(cb.from_user.id, volume_multiplier=val)
    settings = await db.get_settings(cb.from_user.id)
    await cb.message.edit_text(
        f"\u2705 Множитель объёма: <b>x{val}</b>",
        parse_mode="HTML",
        reply_markup=settings_kb(settings),
    )
    await cb.answer()


# ── Signal type ────────────────────────────────────────────────────────

@router.callback_query(F.data == "set:type")
async def cb_set_type(cb: CallbackQuery):
    await cb.message.edit_text(
        "\U0001f503 Выберите тип сигналов:",
        reply_markup=type_kb(),
    )
    await cb.answer()


@router.callback_query(F.data.startswith("stype:"))
async def cb_stype(cb: CallbackQuery):
    val = cb.data.split(":")[1]
    await db.update_settings(cb.from_user.id, signal_type=val)
    settings = await db.get_settings(cb.from_user.id)
    labels = {"both": "\U0001f504 Оба", "pump": "\U0001f680 Только рост", "dump": "\U0001f4c9 Только падение"}
    await cb.message.edit_text(
        f"\u2705 Тип: <b>{labels.get(val, val)}</b>",
        parse_mode="HTML",
        reply_markup=settings_kb(settings),
    )
    await cb.answer()


# ─── Scanning loop ─────────────────────────────────────────────────────

async def scanning_loop():
    logger.info("Scanning loop started")
    while True:
        try:
            users = await db.get_active_users()
            if users:
                messages = await detector.scan(users)
                for uid, text, sig in messages:
                    try:
                        await bot.send_message(uid, text, parse_mode="HTML")
                        await db.log_signal(
                            symbol=sig.symbol,
                            signal_type=sig.signal_type,
                            price_change_pct=sig.price_change_pct,
                            volume_multiplier=sig.volume_ratio,
                            timeframe_minutes=sig.timeframe_minutes,
                        )
                    except Exception as e:
                        logger.error(f"Failed to send to {uid}: {e}")
        except Exception as e:
            logger.error(f"Scanning error: {e}", exc_info=True)
        await asyncio.sleep(config.SCAN_INTERVAL)


# ─── Main ──────────────────────────────────────────────────────────────

async def on_startup():
    await db.connect()
    logger.info("Database connected")
    # Preload symbols
    await binance.get_usdt_symbols()
    logger.info("Binance symbols loaded")


async def on_shutdown():
    await binance.close()
    await db.close()
    logger.info("Shutdown complete")


async def main():
    await on_startup()
    try:
        loop_task = asyncio.create_task(scanning_loop())
        logger.info("Starting bot polling...")
        await dp.start_polling(bot)
    finally:
        loop_task.cancel()
        await on_shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
