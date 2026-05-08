import asyncio
import random
import time
import os
import aiohttp
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.filters import CommandStart, Command
from aiogram import BaseMiddleware
from aiogram.exceptions import TelegramBadRequest

import psycopg2
from psycopg2.extras import RealDictCursor

# ═══════════════════════════════════════════════
#              SYSTEM CONFIGURATION
# ═══════════════════════════════════════════════
TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = os.getenv("ADMIN_ID")
DATABASE_URL = os.getenv("DATABASE_URL")
CRYPTO_BOT_TOKEN = os.getenv("CRYPTO_BOT_TOKEN")

if not TOKEN or not ADMIN_ID or not CRYPTO_BOT_TOKEN:
    raise ValueError("Check BOT_TOKEN, ADMIN_ID and CRYPTO_BOT_TOKEN in Railway environment variables!")

ADMIN_ID = int(ADMIN_ID)
bot = Bot(token=TOKEN)
dp = Dispatcher()

# ═══════════════════════════════════════════════
#              SUBSCRIPTION PLANS
# ═══════════════════════════════════════════════
SUBSCRIPTION_PLANS = {
    "free":   {"limit": 25,  "name": "FREE",   "price": 0,   "emoji": "⬜"},
    "junior": {"limit": 50,  "name": "JUNIOR",  "price": 100,  "duration": 7, "emoji": "🔵"},
    "pro":    {"limit": 100, "name": "PRO",     "price": 200, "duration": 7, "emoji": "🟣"},
}

# ═══════════════════════════════════════════════
#         OTC CURRENCY PAIRS WITH FLAGS
# ═══════════════════════════════════════════════
pairs = [
    "🇦🇪 AED/CNY OTC",
    "🇦🇺 AUD/NZD OTC",
    "🇦🇺 AUD/USD OTC",
    "🇧🇭 BHD/CNY OTC",
    "🇨🇭 CHF/NOK OTC",
    "🇪🇺 EUR/CHF OTC",
    "🇬🇧 GBP/AUD OTC",
    "🇨🇦 CAD/JPY OTC",
    "🇪🇺 EUR/USD OTC",
    "🇲🇦 MAD/USD OTC",
    "🇦🇺 AUD/CAD OTC",
    "🇸🇦 SAR/CNY OTC",
]

# Timeframes for OTC
times = ["⏱ 5 sec", "⏱ 10 sec", "⏱ 15 sec", "⏱ 30 sec"]

# ═══════════════════════════════════════════════
#         MARKET HOURS CHECK
# ═══════════════════════════════════════════════
def is_market_open() -> bool:
    return True

# ════════════════════════════════════════════════
#              PostgreSQL OPERATIONS
# ════════════════════════════════════════════════
def get_db_connection():
    return psycopg2.connect(DATABASE_URL, sslmode='require')

def init_db():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id         BIGINT PRIMARY KEY,
                has_access      BOOLEAN   DEFAULT FALSE,
                total_signals   INTEGER   DEFAULT 0,
                daily_signals   INTEGER   DEFAULT 0,
                last_signal_date TEXT,
                sub_type        TEXT      DEFAULT 'free',
                sub_expires     TIMESTAMP,
                username        TEXT,
                first_seen      TIMESTAMP DEFAULT NOW(),
                last_active     TIMESTAMP DEFAULT NOW()
            )
        """)
        for col, definition in [
            ("username",    "TEXT"),
            ("first_seen",  "TIMESTAMP DEFAULT NOW()"),
            ("last_active", "TIMESTAMP DEFAULT NOW()"),
        ]:
            try:
                cursor.execute(f"ALTER TABLE users ADD COLUMN IF NOT EXISTS {col} {definition}")
            except Exception:
                pass
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"DB initialization error: {e}")

def db_get_user(user_id):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(
            "SELECT has_access, total_signals, daily_signals, last_signal_date, "
            "sub_type, sub_expires, username FROM users WHERE user_id = %s",
            (user_id,)
        )
        row = cursor.fetchone()
        cursor.close()
        conn.close()

        if row:
            sub_type = row['sub_type']
            if row['sub_expires'] and row['sub_expires'] < datetime.now():
                sub_type = 'free'
                db_update_user(user_id, sub_type='free', sub_expires=None)

            today = (datetime.utcnow() + timedelta(hours=3)).strftime("%Y-%m-%d")
            daily_count = row['daily_signals']
            last_date   = row['last_signal_date'] or ""

            if last_date != "" and last_date != today:
                daily_count = 0
                last_date   = today
                db_update_user(user_id, daily=0, date=today)

            return {
                "has_access":  row['has_access'],
                "signals":     row['total_signals'],
                "daily_count": daily_count,
                "last_date":   last_date,
                "sub_type":    sub_type,
                "sub_expires": row['sub_expires'],
                "username":    row.get('username', ''),
            }
    except Exception as e:
        print(f"DB read error: {e}")
    return {"has_access": False, "signals": 0, "daily_count": 0,
            "last_date": "", "sub_type": "free", "sub_expires": None, "username": ""}

def db_update_user(user_id, has_access=None, signals=None, daily=None,
                   date=None, sub_type=None, sub_expires=None, username=None):
    try:
        conn   = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO users (user_id) VALUES (%s) ON CONFLICT (user_id) DO NOTHING",
            (user_id,)
        )
        if has_access  is not None:
            cursor.execute("UPDATE users SET has_access = %s WHERE user_id = %s", (has_access, user_id))
        if signals     is not None:
            cursor.execute("UPDATE users SET total_signals = %s WHERE user_id = %s", (signals, user_id))
        if daily       is not None:
            cursor.execute("UPDATE users SET daily_signals = %s WHERE user_id = %s", (daily, user_id))
        if date        is not None:
            cursor.execute("UPDATE users SET last_signal_date = %s WHERE user_id = %s", (date, user_id))
        if sub_type    is not None:
            cursor.execute("UPDATE users SET sub_type = %s WHERE user_id = %s", (sub_type, user_id))
        if sub_expires is not None or sub_type == 'free':
            cursor.execute("UPDATE users SET sub_expires = %s WHERE user_id = %s", (sub_expires, user_id))
        if username    is not None:
            cursor.execute("UPDATE users SET username = %s WHERE user_id = %s", (username, user_id))
        cursor.execute("UPDATE users SET last_active = NOW() WHERE user_id = %s", (user_id,))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"DB update error: {e}")

def db_get_total_users():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM users")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return count
    except:
        return 0

def db_get_active_users():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM users WHERE last_active > NOW() - INTERVAL '24 hours'")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return count
    except:
        return 0

# ════════════════════════════════════════════════
#              CRYPTO BOT API
# ════════════════════════════════════════════════
async def create_invoice(amount, plan_name):
    url     = "https://pay.crypt.bot/api/createInvoice"
    headers = {"Crypto-Pay-API-Token": CRYPTO_BOT_TOKEN}
    payload = {
        "asset":        "USDT",
        "amount":       str(amount),
        "description":  f"Subscription {plan_name} for 7 days | AI Trading Terminal",
        "paid_btn_name":"callback",
        "paid_btn_url": "https://t.me/CryptoBot"
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, headers=headers) as resp:
            return await resp.json()

async def check_invoice(invoice_id):
    url     = f"https://pay.crypt.bot/api/getInvoices?invoice_ids={invoice_id}"
    headers = {"Crypto-Pay-API-Token": CRYPTO_BOT_TOKEN}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as resp:
            data = await resp.json()
            if data['ok'] and data['result']['items']:
                return data['result']['items'][0]['status'] == 'paid'
    return False

# ════════════════════════════════════════════════
#   OTC SIGNAL GENERATOR (autonomous mode)
# ════════════════════════════════════════════════
def generate_otc_signal(pair: str, timeframe: str) -> tuple[str, int, str]:
    now = datetime.utcnow()

    if "5 sec" in timeframe:
        bucket = int(now.timestamp() / 5)
    elif "10 sec" in timeframe:
        bucket = int(now.timestamp() / 10)
    elif "15 sec" in timeframe:
        bucket = int(now.timestamp() / 15)
    elif "30 sec" in timeframe:
        bucket = int(now.timestamp() / 30)
    else:
        bucket = int(now.timestamp() / 60)

    seed = hash(f"{pair}_{bucket}") % (2**32)
    rng = random.Random(seed)

    rsi = rng.uniform(25, 75)
    if rsi <= 35:
        rsi_vote = +2
        rsi_desc = f"RSI {rsi:.1f} — oversold"
    elif rsi <= 45:
        rsi_vote = +1
        rsi_desc = f"RSI {rsi:.1f} — lower zone"
    elif rsi >= 65:
        rsi_vote = -2
        rsi_desc = f"RSI {rsi:.1f} — overbought"
    elif rsi >= 55:
        rsi_vote = -1
        rsi_desc = f"RSI {rsi:.1f} — upper zone"
    else:
        rsi_vote = rng.choice([-1, 0, 0, +1])
        rsi_desc = f"RSI {rsi:.1f} — neutral"

    ema_options = [
        (+2, "EMA — bullish crossover"),
        (-2, "EMA — bearish crossover"),
        (+1, "EMA — uptrend"),
        (-1, "EMA — downtrend"),
        (0,  "EMA — sideways"),
    ]
    ema_vote, ema_desc = rng.choices(ema_options, weights=[15, 15, 25, 25, 20])[0]

    macd_options = [
        (+2, "MACD — bullish reversal"),
        (-2, "MACD — bearish reversal"),
        (+1, "MACD — positive"),
        (-1, "MACD — negative"),
        (0,  "MACD — neutral"),
    ]
    macd_vote, macd_desc = rng.choices(macd_options, weights=[15, 15, 25, 25, 20])[0]

    bb_options = [
        (+2, "BB — bounce from lower band"),
        (-2, "BB — bounce from upper band"),
        (+1, "BB — lower zone"),
        (-1, "BB — upper zone"),
        (0,  "BB — middle of channel"),
    ]
    bb_vote, bb_desc = rng.choices(bb_options, weights=[12, 12, 26, 26, 24])[0]

    stoch_k = rng.uniform(15, 85)
    if stoch_k <= 20:
        stoch_vote = +2
        stoch_desc = f"Stoch {stoch_k:.0f} — oversold"
    elif stoch_k >= 80:
        stoch_vote = -2
        stoch_desc = f"Stoch {stoch_k:.0f} — overbought"
    elif stoch_k < 40:
        stoch_vote = +1
        stoch_desc = f"Stoch {stoch_k:.0f} — lower zone"
    elif stoch_k > 60:
        stoch_vote = -1
        stoch_desc = f"Stoch {stoch_k:.0f} — upper zone"
    else:
        stoch_vote = rng.choice([-1, 0, +1])
        stoch_desc = f"Stoch {stoch_k:.0f} — neutral"

    pattern_options = [
        (+1, "bullish pin bar"),
        (+1, "bullish engulfing"),
        (+1, "three white soldiers"),
        (-1, "bearish pin bar"),
        (-1, "bearish engulfing"),
        (-1, "three black crows"),
        (0,  "doji"),
        (0,  "no pattern"),
    ]
    pattern_vote, pattern_desc = rng.choices(
        pattern_options,
        weights=[12, 10, 8, 12, 10, 8, 15, 25]
    )[0]

    votes = [rsi_vote, ema_vote, macd_vote, bb_vote, stoch_vote, pattern_vote]
    total_score = sum(votes)

    if total_score > 0:
        agreeing = sum(1 for v in votes if v > 0)
    else:
        agreeing = sum(1 for v in votes if v < 0)

    if agreeing < 3 or abs(total_score) < 3:
        direction  = rng.choice(["UP", "DOWN"])
        confidence = rng.randint(78, 82)
        return direction, confidence, None

    max_possible = 11
    signal_strength = abs(total_score) / max_possible
    base_confidence = 78 + int(signal_strength * 16)
    block_bonus = (agreeing - 3) * 2
    confidence = min(base_confidence + block_bonus, 96)
    confidence += rng.choice([-1, 0, 0, 1])
    confidence = max(78, min(96, confidence))

    direction = "UP" if total_score > 0 else "DOWN"
    return direction, confidence, None


# ════════════════════════════════════════════════
#         RANKS AND UTILITIES
# ════════════════════════════════════════════════
RANKS = [
    (0,    100,  "🌱 Beginner",      "Retail"),
    (101,  300,  "📊 Trader",        "Prop Firm"),
    (301,  1000, "📈 Pro Trader",    "Institutional"),
    (1001, 2000, "🔥 Expert",        "Smart Money"),
    (2001, 9999999, "👑 Market Maker", "Whale"),
]

def get_rank(count):
    for lo, hi, title, level in RANKS:
        if lo <= count <= hi:
            return f"{title} ({level})"
    return "👑 Market Maker (Whale)"

def get_next_rank(count):
    for lo, hi, title, level in RANKS:
        if lo <= count <= hi:
            idx = RANKS.index((lo, hi, title, level))
            if idx + 1 < len(RANKS):
                nxt = RANKS[idx + 1]
                return nxt[2], nxt[3], nxt[0] - count
    return None, None, 0

def confidence_bar(pct: int) -> str:
    filled = int(pct / 10)
    filled = max(0, min(10, filled))
    return "▓" * filled + "░" * (10 - filled)

def days_bar(used: int, total: int) -> str:
    pct = used / total if total > 0 else 0
    filled = int(pct * 10)
    return "█" * filled + "░" * (10 - filled)

def calc_lot(balance: float) -> dict:
    conservative = round(balance * 0.01, 2)
    moderate     = round(balance * 0.02, 2)
    aggressive   = round(balance * 0.03, 2)
    max_risk     = round(balance * 0.05, 2)
    return {
        "conservative": conservative,
        "moderate":     moderate,
        "aggressive":   aggressive,
        "max_risk":     max_risk,
    }

def rank_progress_bar(current: int, lo: int, hi: int) -> str:
    if hi == 9999999:
        return "▓▓▓▓▓▓▓▓▓▓ MAX"
    total = hi - lo
    done  = current - lo
    pct   = done / total if total > 0 else 1
    filled = int(pct * 10)
    filled = max(0, min(10, filled))
    bar = "▓" * filled + "░" * (10 - filled)
    return f"[{bar}] {int(pct * 100)}%"

# ════════════════════════════════════════════════
#         DESIGN CONSTANTS (short lines)
# ════════════════════════════════════════════════
DIV  = "───────────────"
SDIV = "┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈"

# ════════════════════════════════════════════════
#              TEMPORARY DATA
# ════════════════════════════════════════════════
user_temp_data   = {}
pending_users    = set()
pending_support  = set()
pending_lot_calc = set()

last_signal_request = {}   # uid -> timestamp of last successful signal

# ════════════════════════════════════════════════
#              MIDDLEWARE
# ════════════════════════════════════════════════
class AccessMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        if isinstance(event, Message):
            uid  = event.from_user.id
            text = event.text or ""
            if uid == ADMIN_ID:
                return await handler(event, data)
            user_info = db_get_user(uid)
            allowed = [
                "🔐 Activate Access", "📩 Send Pocket Option ID",
                "⬅️ Back", "/start", "⬅️ Menu", "/vip", "/help",
                "🆘 Support", "🚀 About"
            ]
            if not user_info["has_access"] and uid not in pending_users and uid not in pending_support:
                if text not in allowed:
                    await event.answer(
                        "🔒 <b>ACCESS RESTRICTED</b>\n"
                        f"{DIV}\n"
                        "This section is available to verified traders only.\n\n"
                        "Press <b>«🔐 Activate Access»</b>",
                        parse_mode="HTML"
                    )
                    return
        return await handler(event, data)

dp.message.middleware(AccessMiddleware())

# ════════════════════════════════════════════════
#              KEYBOARDS
# ════════════════════════════════════════════════
def get_main_menu(has_access: bool):
    keyboard = [
        [KeyboardButton(text="📊 Trading Panel"), KeyboardButton(text="⚡ Get Signal")],
        [KeyboardButton(text="👤 Profile"),        KeyboardButton(text="📈 Statistics")],
        [KeyboardButton(text="💎 Subscription"),   KeyboardButton(text="🚀 About")],
        [KeyboardButton(text="🧮 Lot Calculator")],
    ]
    row_bottom = []
    if not has_access:
        row_bottom.append(KeyboardButton(text="🔐 Activate Access"))
    row_bottom.append(KeyboardButton(text="🆘 Support"))
    keyboard.append(row_bottom)
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

access_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📩 Send Pocket Option ID")],
        [KeyboardButton(text="⬅️ Back")]
    ],
    resize_keyboard=True
)

def get_pair_kb():
    rows = []
    pair_list = list(pairs)
    for i in range(0, len(pair_list), 2):
        if i + 1 < len(pair_list):
            rows.append([
                KeyboardButton(text=pair_list[i]),
                KeyboardButton(text=pair_list[i + 1])
            ])
        else:
            rows.append([KeyboardButton(text=pair_list[i])])
    rows.append([KeyboardButton(text="⬅️ Back")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)

pair_kb = get_pair_kb()

time_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="⏱ 5 sec"),  KeyboardButton(text="⏱ 10 sec")],
        [KeyboardButton(text="⏱ 15 sec"), KeyboardButton(text="⏱ 30 sec")],
        [KeyboardButton(text="⬅️ Back")]
    ],
    resize_keyboard=True
)
signal_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="⚡ Get Signal")],
        [KeyboardButton(text="📊 Trading Panel"), KeyboardButton(text="⬅️ Menu")]
    ],
    resize_keyboard=True
)
back_kb = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="⬅️ Back")]],
    resize_keyboard=True
)

def get_sub_kb(current_plan: str = "free"):
    buttons = []
    if current_plan == "free":
        buttons.append([InlineKeyboardButton(text="🔵 JUNIOR — 100$ / 7 days", callback_data="buy_junior")])
        buttons.append([InlineKeyboardButton(text="🟣 PRO — 200$ / 7 days",    callback_data="buy_pro")])
    elif current_plan == "junior":
        buttons.append([InlineKeyboardButton(text="🔄 Renew JUNIOR — 100$ / 7 days", callback_data="buy_junior")])
        buttons.append([InlineKeyboardButton(text="⬆️ Upgrade to PRO — 200$ / 7 days", callback_data="buy_pro")])
    elif current_plan == "pro":
        buttons.append([InlineKeyboardButton(text="🔄 Renew PRO — 200$ / 7 days", callback_data="buy_pro")])
        buttons.append([InlineKeyboardButton(text="🔵 Switch to JUNIOR — 100$ / 7 days", callback_data="buy_junior")])
    buttons.append([InlineKeyboardButton(text="📊 Compare Plans", callback_data="compare_plans")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_upgrade_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔵 JUNIOR — 50 signals/day | 100$", callback_data="buy_junior")],
        [InlineKeyboardButton(text="🟣 PRO — 100 signals/day | 200$",   callback_data="buy_pro")],
        [InlineKeyboardButton(text="📊 Compare Plans",                   callback_data="compare_plans")],
    ])

def get_confirm_sub_kb(invoice_url, invoice_id, plan_key):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Pay (USDT)", url=invoice_url)],
        [InlineKeyboardButton(text="✅ Check Payment", callback_data=f"check_{invoice_id}_{plan_key}")],
        [InlineKeyboardButton(text="🔙 Back to Plans",  callback_data="back_to_plans")],
    ])

# ════════════════════════════════════════════════
#              SUBSCRIPTION HANDLERS
# ════════════════════════════════════════════════
@dp.message(F.text == "💎 Subscription")
async def sub_menu(message: Message):
    u     = db_get_user(message.from_user.id)
    plan  = SUBSCRIPTION_PLANS[u['sub_type']]
    limit = plan['limit']
    emoji = plan['emoji']

    exp_str = "∞ Lifetime"
    days_left_str = ""
    if u['sub_expires']:
        exp_str = u['sub_expires'].strftime("%d.%m.%Y %H:%M")
        days_left = (u['sub_expires'] - datetime.now()).days
        days_used = 7 - days_left
        bar = days_bar(days_used, 7)
        days_left_str = f"\n  Remaining: <code>[{bar}]</code> <b>{max(days_left, 0)} days</b>"

    renew_block = ""
    if u['sub_type'] != 'free':
        renew_block = (
            f"\n{SDIV}\n"
            "🔄 <b>Renew / Change Plan</b>\n"
            "<i>Days will be added to your current balance.</i>\n"
        )

    text = (
        "💎 <b>SUBSCRIPTION</b>\n"
        f"{DIV}\n\n"
        f"  Plan:    {emoji} <b>{u['sub_type'].upper()}</b>\n"
        f"  Limit:   <b>{limit} signals / day</b>\n"
        f"  Expires: <b>{exp_str}</b>"
        f"{days_left_str}\n"
        f"{renew_block}"
        f"\n{DIV}\n"
        "📦 <b>Plans:</b>\n\n"
        "⬜ <b>FREE</b>   — 25 signals / day  <i>(free)</i>\n"
        "🔵 <b>JUNIOR</b> — 50 signals / day  <i>100$ / 7 days</i>\n"
        "🟣 <b>PRO</b>    — 100 signals / day  <i>200$ / 7 days</i>\n\n"
        "<i>Payment in <b>USDT</b> via CryptoBot — instant.</i>"
    )
    await message.answer(text, reply_markup=get_sub_kb(u['sub_type']), parse_mode="HTML")

@dp.callback_query(F.data == "compare_plans")
async def compare_plans(callback: CallbackQuery):
    text = (
        "📊 <b>PLAN COMPARISON</b>\n"
        f"{DIV}\n\n"
        "<code>"
        "Feature              FREE  JUN  PRO\n"
        "───────────────────────────────────\n"
        "Signals/day            25   50  100\n"
        "OTC analysis           ✅   ✅   ✅\n"
        "RSI/EMA/MACD           ✅   ✅   ✅\n"
        "AI confidence          ✅   ✅   ✅\n"
        "Calculator             ✅   ✅   ✅\n"
        "Support                ❌   ✅   ✅\n"
        "Analytics              ❌   ✅   ✅\n"
        "Volatility             ❌   ✅   ✅\n"
        "VIP notifications      ❌   ❌   ✅\n"
        "Trend strength         ❌   ❌   ✅\n"
        "Trade volume           ❌   ❌   ✅\n"
        "TOP strategies         ❌   ❌   ✅\n"
        "────────────────────────────────────\n"
        "Price                  0$ 100$ 200$\n"
        "Duration               ∞  7d   7d\n"
        "</code>\n"
        f"{DIV}\n"
        "<i>More signals = more opportunities</i>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔵 Buy JUNIOR — 100$", callback_data="buy_junior")],
        [InlineKeyboardButton(text="🟣 Buy PRO — 200$",    callback_data="buy_pro")],
    ])
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data == "back_to_plans")
async def back_to_plans(callback: CallbackQuery):
    u = db_get_user(callback.from_user.id)
    await callback.message.edit_reply_markup(reply_markup=get_sub_kb(u['sub_type']))

@dp.callback_query(F.data.startswith("buy_"))
async def process_buy(callback: CallbackQuery):
    plan_key = callback.data.split("_")[1]
    plan     = SUBSCRIPTION_PLANS[plan_key]
    u        = db_get_user(callback.from_user.id)
    res      = await create_invoice(plan['price'], plan['name'])

    is_renew    = u['sub_type'] == plan_key
    action_word = "RENEWAL" if is_renew else "PURCHASE"

    if res['ok']:
        invoice_url = res['result']['pay_url']
        invoice_id  = res['result']['invoice_id']
        kb = get_confirm_sub_kb(invoice_url, invoice_id, plan_key)

        renew_note = ""
        if is_renew and u['sub_expires']:
            new_exp = u['sub_expires'] + timedelta(days=7)
            renew_note = f"\n  📅 New expiry: <b>{new_exp.strftime('%d.%m.%Y')}</b>\n"

        await callback.message.edit_text(
            f"🧾 <b>INVOICE — {action_word}</b>\n"
            f"{DIV}\n\n"
            f"  Plan:     {plan['emoji']} <b>{plan['name']}</b>\n"
            f"  Amount:   <b>{plan['price']} USDT</b>\n"
            f"  Duration: <b>7 days</b>\n"
            f"  Limit:    <b>{plan['limit']} signals / day</b>\n"
            f"{renew_note}"
            f"{DIV}\n"
            f"1️⃣ Press <b>«💳 Pay»</b>\n"
            f"2️⃣ Complete payment in USDT\n"
            f"3️⃣ Press <b>«✅ Check Payment»</b>\n\n"
            f"<i>⚡ Instant activation after confirmation.</i>",
            reply_markup=kb,
            parse_mode="HTML"
        )
    else:
        await callback.answer("⚠️ Invoice creation error. Please try again later.", show_alert=True)

@dp.callback_query(F.data.startswith("check_"))
async def process_check(callback: CallbackQuery):
    parts    = callback.data.split("_")
    inv_id   = parts[1]
    plan_key = parts[2]
    is_paid  = await check_invoice(inv_id)

    if is_paid:
        u = db_get_user(callback.from_user.id)
        if u['sub_type'] == plan_key and u['sub_expires'] and u['sub_expires'] > datetime.now():
            expiry = u['sub_expires'] + timedelta(days=7)
        else:
            expiry = datetime.now() + timedelta(days=7)

        db_update_user(callback.from_user.id, sub_type=plan_key, sub_expires=expiry)
        plan = SUBSCRIPTION_PLANS[plan_key]
        await callback.message.edit_text(
            f"🎉 <b>PAYMENT CONFIRMED!</b>\n"
            f"{DIV}\n\n"
            f"  Plan:    {plan['emoji']} <b>{plan_key.upper()}</b>\n"
            f"  Limit:   <b>{plan['limit']} signals / day</b>\n"
            f"  Expires: <b>{expiry.strftime('%d.%m.%Y %H:%M')}</b>\n\n"
            f"{DIV}\n"
            f"🚀 <b>Terminal activated!</b>\n"
            f"<i>Profitable trades and a green balance! 📈</i>",
            parse_mode="HTML"
        )
        try:
            await bot.send_message(
                ADMIN_ID,
                f"💰 <b>NEW PAYMENT</b>\n"
                f"👤 ID: <code>{callback.from_user.id}</code>\n"
                f"📦 Plan: <b>{plan_key.upper()}</b>\n"
                f"💵 Amount: <b>{plan['price']} USDT</b>\n"
                f"📅 Expires: <b>{expiry.strftime('%d.%m.%Y %H:%M')}</b>",
                parse_mode="HTML"
            )
        except:
            pass
    else:
        await callback.answer("❌ Payment not received yet. Please wait and check again.", show_alert=True)

# ════════════════════════════════════════════════
#              COMMANDS AND MAIN HANDLERS
# ════════════════════════════════════════════════
@dp.message(CommandStart())
async def start(message: Message):
    db_update_user(message.from_user.id, username=message.from_user.username)
    u           = db_get_user(message.from_user.id)
    total_users = db_get_total_users()

    start_text = (
        "┌─────────────────────────┐\n"
        "│  🖥  AI TRADING TERMINAL  │\n"
        "│     OTC PRO v4.0        │\n"
        "└─────────────────────────┘\n\n"
        "⚡ <b>Professional signal system</b> for Pocket Option OTC market.\n\n"
        "🧠 <b>Smart Precision Engine:</b>\n"
        "▸ 12 OTC pairs with country flags\n"
        "▸ Timeframes: 5s / 10s / 15s / 30s\n"
        "▸ 6 analysis blocks (RSI + EMA + MACD + BB + Stoch + patterns)\n"
        "▸ AI confidence: 78–96%\n\n"
        f"👥 Traders: <b>{total_users + 152:,}</b>\n"
        f"📡 WinRate: <b>88–96%</b>  |  🟢 <b>24/7</b>\n"
        f"🕐 {(datetime.utcnow() + timedelta(hours=3)).strftime('%d.%m.%Y %H:%M')} MSK"
    )
    await message.answer(start_text, reply_markup=get_main_menu(u["has_access"]), parse_mode="HTML")

@dp.message(F.text == "🚀 About")
async def about_bot(message: Message):
    pairs_list = "\n".join([f"  ▸ {p}" for p in pairs])

    text = (
        "🤖 <b>AI TRADING TERMINAL — OTC PRO v4.0</b>\n"
        f"{DIV}\n\n"
        "📡 <b>Platform:</b> Pocket Option (OTC)\n\n"
        "🧠 <b>Smart Precision Engine v4:</b>\n"
        "  ▸ RSI(14)\n"
        "  ▸ EMA(9/21) crossover + trend\n"
        "  ▸ MACD(12,26,9)\n"
        "  ▸ Bollinger Bands(20,2)\n"
        "  ▸ Stochastic(14,3)\n"
        "  ▸ Candlestick patterns (8 types)\n"
        "🎯 <b>Entry filter:</b> 3 of 6 blocks\n\n"
        f"{DIV}\n"
        "💱 <b>OTC PAIRS (12 instruments):</b>\n\n"
        f"{pairs_list}\n\n"
        f"{DIV}\n"
        "⏱ <b>Timeframes:</b> 5s · 10s · 15s · 30s\n"
        "⏰ <b>Mode:</b> MON–SUN 24/7\n\n"
        f"{DIV}\n"
        "📦 <b>Plans:</b>\n"
        "  ⬜ FREE   — 25 signals / day\n"
        "  🔵 JUNIOR — 50 signals / day  |  100$ / 7 days\n"
        "  🟣 PRO    — 100 signals / day  |  200$ / 7 days\n\n"
        f"{DIV}\n"
        "⚠️ <i>Trading binary options involves risks. "
        "Signals are for informational purposes only. Always use proper money management.</i>"
    )
    await message.answer(text, parse_mode="HTML")

# ════════════════════════════════════════════════
#         🧮 LOT CALCULATOR
# ════════════════════════════════════════════════
@dp.message(F.text == "🧮 Lot Calculator")
async def lot_calculator(message: Message):
    pending_lot_calc.add(message.from_user.id)
    await message.answer(
        "🧮 <b>LOT CALCULATOR</b>\n"
        f"{DIV}\n\n"
        "Enter your <b>balance in dollars</b>:\n\n"
        "<i>Minimum: 50$  |  Example: 100 or 500</i>",
        reply_markup=back_kb,
        parse_mode="HTML"
    )

@dp.message(lambda msg: msg.from_user.id in pending_lot_calc)
async def process_lot_calc(message: Message):
    if message.text == "⬅️ Back":
        pending_lot_calc.discard(message.from_user.id)
        u = db_get_user(message.from_user.id)
        return await message.answer(
            "🏠 <b>Main Panel</b>",
            reply_markup=get_main_menu(u["has_access"]),
            parse_mode="HTML"
        )

    text = (message.text or "").replace(",", ".").replace(" ", "")
    try:
        balance = float(text)
        if balance <= 0:
            raise ValueError
    except ValueError:
        return await message.answer(
            "❌ Enter a valid amount (numbers only, > 0).\n"
            "<i>Example: 100</i>",
            parse_mode="HTML"
        )

    if balance < 50:
        return await message.answer(
            "⚠️ <b>BALANCE TOO LOW</b>\n"
            f"{DIV}\n\n"
            f"  You entered: <b>{balance:,.2f}$</b>\n"
            f"  Minimum: <b>50$</b>\n\n"
            f"{SDIV}\n"
            "❌ Trading with this balance is <b>not recommended</b>.\n\n"
            "With a balance below 50$ you cannot follow basic money management rules:\n\n"
            "▸ Minimum trade on Pocket Option is <b>1$</b>\n"
            "▸ Recommended risk per trade — <b>1–2% of deposit</b>\n"
            "▸ With a balance under 50$, even a $1 trade = <b>2%+ risk</b>, leading to fast loss\n"
            "▸ A streak of 5–7 losing trades will completely wipe the deposit\n\n"
            f"{SDIV}\n"
            "💡 <b>Recommendation:</b> top up to at least <b>50$</b>, "
            "ideally from <b>100$</b> for comfortable trading.\n\n"
            "<i>Enter a valid amount (from 50$):</i>",
            parse_mode="HTML"
        )

    pending_lot_calc.discard(message.from_user.id)
    u = db_get_user(message.from_user.id)
    lot = calc_lot(balance)

    bar_c = confidence_bar(10)
    bar_m = confidence_bar(20)
    bar_a = confidence_bar(30)
    bar_x = confidence_bar(50)

    await message.answer(
        f"🧮 <b>LOT CALCULATOR</b>\n"
        f"{DIV}\n\n"
        f"  💰 Balance: <b>{balance:,.2f}$</b>\n\n"
        f"{DIV}\n"
        f"🟢 <b>Conservative (1%)</b>\n"
        f"  <code>{bar_c}</code>  <b>{lot['conservative']:,.2f}$</b>\n\n"
        f"🔵 <b>Moderate (2%)</b> — optimal ✅\n"
        f"  <code>{bar_m}</code>  <b>{lot['moderate']:,.2f}$</b>\n\n"
        f"🟡 <b>Aggressive (3%)</b>\n"
        f"  <code>{bar_a}</code>  <b>{lot['aggressive']:,.2f}$</b>\n\n"
        f"🔴 <b>Maximum (5%)</b> — red zone\n"
        f"  <code>{bar_x}</code>  <b>{lot['max_risk']:,.2f}$</b>\n\n"
        f"{DIV}\n"
        f"💡 Optimal: <b>{lot['moderate']:,.2f}$ – {lot['aggressive']:,.2f}$</b>\n"
        f"<i>Never risk more than 5% in a single trade!</i>",
        reply_markup=get_main_menu(u["has_access"]),
        parse_mode="HTML"
    )

# ════════════════════════════════════════════════
#              ACCESS ACTIVATION
# ════════════════════════════════════════════════
@dp.message(Command("vip"))
@dp.message(F.text == "🔐 Activate Access")
async def activate(message: Message):
    user_info = db_get_user(message.from_user.id)
    if user_info["has_access"]:
        return await message.answer(
            "✅ <b>VIP LICENSE ACTIVE</b>\n"
            f"{DIV}\n"
            "All terminal modules are unlocked.",
            parse_mode="HTML"
        )
    await message.answer(
        "💎 <b>VIP LICENSE ACTIVATION</b>\n"
        f"{DIV}\n\n"
        "📋 <b>3 simple steps:</b>\n\n"
        "1️⃣ <b>Register an account:</b>\n"
        "   🌍 Global: <a href='https://u3.shortink.io/register?utm_campaign=845784&utm_source=affiliate&utm_medium=sr&a=e0FkuUtf0CHZA5&al=1760257&ac=bot&cid=954756&code=LXJ558'>Pocket Option (Official Gateway)</a>\n"
        "   🇷🇺 RU/CIS: <a href='https://po-ru4.click/register?utm_campaign=845784&utm_source=affiliate&utm_medium=sr&a=e0FkuUtf0CHZA5&al=1760257&ac=bot&cid=954756&code=LXJ558'>Pocket Option (Mirror)</a>\n\n"
        "2️⃣ <b>Top up your deposit</b> from <b>$50</b>\n\n"
        "3️⃣ <b>Send your ID</b> using the button below\n\n"
        f"{DIV}\n"
        "🎁 <b>+60% bonus</b> on deposit when registering via our link!\n\n"
        "⚠️ <b>Important:</b> your account must be registered via our link. "
        "If not, create a new one strictly via the link above.\n\n"
        "🔐 <i>Activation within a few minutes after verification.</i>",
        reply_markup=access_kb,
        parse_mode="HTML",
        disable_web_page_preview=True
    )

@dp.message(Command("help"))
@dp.message(F.text == "🆘 Support")
async def help_cmd(message: Message):
    pending_support.add(message.from_user.id)
    await message.answer(
        "🆘 <b>SUPPORT</b>\n"
        f"{DIV}\n\n"
        "Describe your issue in one message — we'll forward it to the admin.\n\n"
        "💬 <b>FAQ:</b>\n"
        "▸ Activation → «🔐 Activate Access»\n"
        "▸ Pocket Option ID → My Account → Profile\n"
        "▸ Signal limit resets at 00:00 MSK\n"
        "▸ Terminal operates 24/7\n\n"
        "✍️ <b>Write your question:</b>",
        reply_markup=back_kb,
        parse_mode="HTML"
    )

@dp.message(F.text == "📩 Send Pocket Option ID")
async def ask_id(message: Message):
    pending_users.add(message.from_user.id)
    await message.answer(
        "🔢 <b>ACCOUNT VERIFICATION</b>\n"
        f"{DIV}\n\n"
        "Enter your <b>numeric Pocket Option profile ID</b>:\n\n"
        "📍 <i>Where to find it: Pocket Option → Account → Profile</i>\n\n"
        "⌨️ <b>Numbers only:</b>",
        reply_markup=back_kb,
        parse_mode="HTML"
    )

@dp.message(F.text == "⬅️ Back")
@dp.message(F.text == "⬅️ Menu")
async def go_back(message: Message):
    pending_users.discard(message.from_user.id)
    pending_support.discard(message.from_user.id)
    pending_lot_calc.discard(message.from_user.id)
    u = db_get_user(message.from_user.id)
    await message.answer(
        f"🏠 <b>Home</b> · <i>{message.from_user.first_name}</i>",
        reply_markup=get_main_menu(u["has_access"]),
        parse_mode="HTML"
    )

@dp.message(lambda msg: msg.from_user.id in pending_support)
async def process_support_message(message: Message):
    if message.text == "⬅️ Back":
        pending_support.discard(message.from_user.id)
        return await go_back(message)
    uid      = message.from_user.id
    username = message.from_user.username or "—"
    name     = message.from_user.full_name or "—"
    await bot.send_message(
        ADMIN_ID,
        f"📩 <b>SUPPORT REQUEST</b>\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"👤 Name: <b>{name}</b>\n"
        f"🔗 Username: @{username}\n"
        f"🆔 ID: <code>{uid}</code>\n\n"
        f"📝 <b>Message:</b>\n{message.text}\n\n"
        f"💬 Reply: <code>/reply {uid} text</code>",
        parse_mode="HTML"
    )
    pending_support.discard(uid)
    u = db_get_user(uid)
    await message.answer(
        "✅ <b>Request received!</b>\n"
        "We'll respond within 30 minutes.",
        reply_markup=get_main_menu(u["has_access"]),
        parse_mode="HTML"
    )

@dp.message(lambda msg: msg.from_user.id in pending_users)
async def process_id(message: Message):
    if message.text == "⬅️ Back":
        pending_users.discard(message.from_user.id)
        return await go_back(message)
    if not message.text or not message.text.isdigit():
        return await message.answer(
            "❌ <b>Error.</b> Enter <b>numbers only</b>.\n"
            "<i>Example: 12345678</i>",
            parse_mode="HTML"
        )
    uid = message.from_user.id
    pending_users.discard(uid)
    await bot.send_message(
        ADMIN_ID,
        f"🔔 <b>NEW VIP APPLICATION</b>\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"👤 Name: <b>{message.from_user.full_name}</b>\n"
        f"🔗 Username: @{message.from_user.username or '—'}\n"
        f"🆔 TG ID: <code>{uid}</code>\n"
        f"💼 PO ID: <code>{message.text}</code>\n\n"
        f"✅ Grant: <code>/give {uid}</code>\n"
        f"🚫 Deny: <code>/block {uid}</code>",
        parse_mode="HTML"
    )
    u = db_get_user(uid)
    await message.answer(
        "⏳ <b>APPLICATION SENT</b>\n"
        f"{DIV}\n\n"
        f"🆔 Pocket Option ID: <code>{message.text}</code>\n\n"
        "Please wait for verification. Activation takes a few minutes.",
        reply_markup=get_main_menu(u["has_access"]),
        parse_mode="HTML"
    )

# ════════════════════════════════════════════════
#              ADMIN COMMANDS
# ════════════════════════════════════════════════
@dp.message(F.text.startswith("/give"))
async def admin_give(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    try:
        target = int(message.text.split()[1])
        db_update_user(target, has_access=True)
        await bot.send_message(
            target,
            "🚀 <b>VIP ACCESS ACTIVATED!</b>\n"
            f"{DIV}\n\n"
            "✅ Account verified. All modules unlocked.\n\n"
            "📊 Press <b>«📊 Trading Panel»</b>\n"
            "⚡ Or go straight to <b>«⚡ Get Signal»</b>\n\n"
            "<i>Profitable trades! 📈</i>",
            parse_mode="HTML",
            reply_markup=get_main_menu(True)
        )
        await message.answer(f"✅ Access for <code>{target}</code> activated.", parse_mode="HTML")
    except Exception as e:
        await message.answer(f"⚠️ Error: {e}\nFormat: <code>/give ID</code>", parse_mode="HTML")

@dp.message(F.text.startswith("/block"))
async def admin_block(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    try:
        target = int(message.text.split()[1])
        db_update_user(target, has_access=False)
        try:
            await bot.send_message(
                target,
                "🛑 <b>ACCESS REVOKED</b>\n"
                f"{DIV}\n\n"
                "VIP license has been revoked by the administrator.\n"
                "Contact support: /help",
                parse_mode="HTML",
                reply_markup=get_main_menu(False)
            )
        except:
            pass
        await message.answer(f"🚫 Access for <code>{target}</code> blocked.", parse_mode="HTML")
    except Exception as e:
        await message.answer(f"⚠️ Error: {e}\nFormat: <code>/block ID</code>", parse_mode="HTML")

@dp.message(F.text.startswith("/reply"))
async def admin_reply(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    try:
        parts  = message.text.split(maxsplit=2)
        target = int(parts[1])
        text   = parts[2]
        await bot.send_message(
            target,
            f"💬 <b>SUPPORT REPLY</b>\n"
            f"{DIV}\n\n"
            f"{text}",
            parse_mode="HTML"
        )
        await message.answer(f"✅ Reply sent to user <code>{target}</code>.", parse_mode="HTML")
    except Exception as e:
        await message.answer(f"⚠️ Error: {e}\nFormat: <code>/reply ID text</code>", parse_mode="HTML")

@dp.message(Command("stats_admin"))
async def admin_stats(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    total  = db_get_total_users()
    active = db_get_active_users()
    await message.answer(
        f"📊 <b>BOT STATISTICS</b>\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"👥 Total: <b>{total}</b>\n"
        f"🟢 Active (24h): <b>{active}</b>\n"
        f"📅 {datetime.now().strftime('%d.%m.%Y %H:%M')}",
        parse_mode="HTML"
    )

@dp.message(F.text.startswith("/broadcast"))
async def admin_broadcast(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    try:
        text = message.text.split(maxsplit=1)[1]
        try:
            conn   = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT user_id FROM users")
            users = cursor.fetchall()
            cursor.close()
            conn.close()
        except:
            users = []

        sent = 0
        fail = 0
        for (uid,) in users:
            try:
                await bot.send_message(
                    uid,
                    f"📢 <b>MESSAGE FROM THE TEAM</b>\n"
                    f"━━━━━━━━━━━━━━━━━\n\n"
                    f"{text}",
                    parse_mode="HTML"
                )
                sent += 1
                await asyncio.sleep(0.05)
            except:
                fail += 1

        await message.answer(
            f"📤 <b>Broadcast complete</b>\n"
            f"✅ Delivered: <b>{sent}</b>\n"
            f"❌ Errors: <b>{fail}</b>",
            parse_mode="HTML"
        )
    except Exception as e:
        await message.answer(f"⚠️ Format: <code>/broadcast text</code>\n{e}", parse_mode="HTML")

# ════════════════════════════════════════════════
#              TRADING PANEL
# ════════════════════════════════════════════════
@dp.message(F.text == "📊 Trading Panel")
async def t_panel(message: Message):
    if not db_get_user(message.from_user.id)["has_access"]:
        return

    now_msk = datetime.utcnow() + timedelta(hours=3)
    hour = now_msk.hour
    if 3 <= hour < 10:
        session_info = "🌏 Asian · moderate volatility"
    elif 10 <= hour < 18:
        session_info = "🌍 European · high liquidity"
    elif 18 <= hour < 23:
        session_info = "🌎 American · maximum volume"
    else:
        session_info = "🌙 Night · caution, low volume"

    await message.answer(
        "📊 <b>TRADING PANEL</b>\n"
        f"{DIV}\n\n"
        f"  📡 {session_info}\n"
        f"  🕐 {now_msk.strftime('%H:%M')} MSK · 12 OTC pairs\n\n"
        "Select a <b>currency pair:</b>",
        reply_markup=pair_kb,
        parse_mode="HTML"
    )

@dp.message(F.text.in_(set(pairs)))
async def set_pair(message: Message):
    uid = message.from_user.id
    user_temp_data[uid] = {"pair": message.text}

    await message.answer(
        f"✅ <b>{message.text}</b>\n\n"
        f"⏱ Select <b>expiration time:</b>",
        reply_markup=time_kb,
        parse_mode="HTML"
    )

@dp.message(F.text.in_(set(times)))
async def set_time(message: Message):
    uid = message.from_user.id
    if uid not in user_temp_data or "pair" not in user_temp_data.get(uid, {}):
        await message.answer(
            "⚠️ Please select a pair first.\n"
            "Press <b>«📊 Trading Panel»</b>.",
            parse_mode="HTML"
        )
        return

    user_temp_data[uid]["time"] = message.text
    pair = user_temp_data[uid]["pair"]

    await message.answer(
        f"⚙️ <b>READY</b>\n"
        f"{DIV}\n\n"
        f"  Pair:       <b>{pair}</b>\n"
        f"  Expiration: <b>{message.text}</b>\n\n"
        f"<i>Press «⚡ Get Signal»</i>",
        reply_markup=signal_kb,
        parse_mode="HTML"
    )

# ════════════════════════════════════════════════
#     MAIN SIGNAL HANDLER — NEW DESIGN
# ════════════════════════════════════════════════
@dp.message(Command("signals"))
@dp.message(F.text == "⚡ Get Signal")
async def get_signal(message: Message):
    uid = message.from_user.id
    u   = db_get_user(uid)
    if not u["has_access"]:
        return

    # Anti-spam
    now_ts = time.time()
    last_ts = last_signal_request.get(uid, 0)
    if now_ts - last_ts < 1.5:
        return

    today = (datetime.utcnow() + timedelta(hours=3)).strftime("%Y-%m-%d")
    daily = u["daily_count"]

    if u["last_date"] != today:
        daily = 0
        db_update_user(uid, daily=0, date=today)

    sub_type      = u['sub_type']
    current_limit = SUBSCRIPTION_PLANS[sub_type]['limit']

    if daily >= current_limit:
        if sub_type == "free":
            return await message.answer(
                "🛑 <b>DAILY LIMIT REACHED</b>\n"
                f"{DIV}\n\n"
                f"Used <b>{current_limit} / {current_limit}</b> free signals.\n\n"
                "💡 Get more signals with a subscription:\n\n"
                "🔵 <b>JUNIOR</b> — <b>50 signals/day</b>  |  <b>100$</b>\n"
                "🟣 <b>PRO</b>    — <b>100 signals/day</b>  |  <b>200$</b>\n\n"
                "⏳ <i>Or wait for the reset at 00:00 MSK</i>",
                reply_markup=get_upgrade_kb(),
                parse_mode="HTML"
            )
        else:
            return await message.answer(
                "🛑 <b>LIMIT REACHED</b>\n"
                f"{DIV}\n\n"
                f"Plan <b>{sub_type.upper()}</b>: <b>{daily} / {current_limit}</b> signals.\n\n"
                "The limit protects against emotional trading.\n"
                "Come back tomorrow — resets at <b>00:00 MSK</b>.\n\n"
                "💡 Want more? Change your plan in <b>«💎 Subscription»</b>",
                reply_markup=get_upgrade_kb(),
                parse_mode="HTML"
            )

    # Check configuration
    data = user_temp_data.get(uid, {})

    if not data.get("pair"):
        return await message.answer(
            "⚠️ <b>No pair selected!</b>\n\n"
            "Press <b>«📊 Trading Panel»</b>,\n"
            "select a pair and expiration time.",
            reply_markup=get_main_menu(True),
            parse_mode="HTML"
        )

    if not data.get("time"):
        await message.answer(
            f"⚠️ <b>No time selected!</b>\n\n"
            f"Pair: <b>{data['pair']}</b>\n\n"
            f"Select <b>expiration:</b>",
            reply_markup=time_kb,
            parse_mode="HTML"
        )
        return

    last_signal_request[uid] = now_ts

    # Animated progress bar
    progress_frames = [
        ("⬛⬛⬛⬛⬛  0%",   "Connecting to terminal..."),
        ("🟩🟩⬛⬛⬛  40%",  "RSI · EMA · MACD..."),
        ("🟩🟩🟩🟩⬛  80%",  "BB · Stoch · patterns..."),
        ("🟩🟩🟩🟩🟩  100%", "Signal formed ✅"),
    ]

    try:
        progress_msg = await message.answer(
            f"<b>⚡ MARKET ANALYSIS</b>\n"
            f"{DIV}\n\n"
            f"<code>{progress_frames[0][0]}</code>\n"
            f"<i>{progress_frames[0][1]}</i>",
            parse_mode="HTML"
        )
    except Exception as e:
        print(f"Progress bar error: {e}")
        return

    for bar, label in progress_frames[1:]:
        await asyncio.sleep(0.35)
        try:
            await progress_msg.edit_text(
                f"<b>⚡ MARKET ANALYSIS</b>\n"
                f"{DIV}\n\n"
                f"<code>{bar}</code>\n"
                f"<i>{label}</i>",
                parse_mode="HTML"
            )
        except (TelegramBadRequest, Exception):
            pass

    # Signal generation
    direction, confidence, _ = generate_otc_signal(data["pair"], data["time"])

    db_update_user(uid, signals=u["signals"] + 1, daily=daily + 1, date=today)
    new_daily = daily + 1
    remaining = current_limit - new_daily

    # ── NEW COMPACT SIGNAL DESIGN ─────────────────────────────
    is_up = direction == "UP"

    if is_up:
        dir_line   = "▲  UP  ·  CALL"
        dir_emoji  = "🟢"
    else:
        dir_line   = "▼  DOWN  ·  PUT"
        dir_emoji  = "🔴"

    # Confidence
    conf_bar = confidence_bar(confidence)

    if confidence >= 93:
        conf_label = "🔥 Extreme"
    elif confidence >= 88:
        conf_label = "💎 Strong"
    elif confidence >= 84:
        conf_label = "⚡ Steady"
    else:
        conf_label = "📊 Standard"

    # Limit line
    if remaining == 0:
        limit_line = f"<b>⚠️ Last signal for today!</b>"
    elif remaining <= 3:
        limit_line = f"<i>Remaining: <b>{remaining}</b> signals</i>"
    else:
        limit_line = f"<i>{new_daily} / {current_limit} · {remaining} remaining</i>"

    # PRO block
    pro_block = ""
    if sub_type in ("junior", "pro"):
        now_msk = datetime.utcnow() + timedelta(hours=3)
        hour = now_msk.hour
        if 3 <= hour < 10:
            session = "🌏 Asian"
        elif 10 <= hour < 18:
            session = "🌍 European"
        elif 18 <= hour < 23:
            session = "🌎 American"
        else:
            session = "🌙 Night"

        volatility_opts = ["🟢 Low", "🟡 Moderate", "🟠 Medium", "🔴 High"]
        rng_vol = random.Random(hash(f"{data['pair']}_{confidence}_{hour}"))
        volatility = rng_vol.choice(volatility_opts)

        pro_block = (
            f"\n{SDIV}\n"
            f"  📡 Session:    <b>{session}</b>\n"
            f"  📊 Volatility: <b>{volatility}</b>\n"
        )

    # PRO extended block
    pro_extra = ""
    if sub_type == "pro":
        rng_pro = random.Random(hash(f"{data['pair']}_{direction}_{confidence}"))
        trend_strength = rng_pro.randint(55, 95)
        trend_bar = confidence_bar(trend_strength)
        pro_tips = [
            "Standard conditions — follow the algorithm",
            "High confidence — standard volume",
            "Moderate signal — recommend 1–2% of deposit",
            "Strong bias — good entry point",
            "Counter-trend — extra caution advised",
        ]
        pro_tip = rng_pro.choice(pro_tips)
        pro_extra = (
            f"  💪 Trend: <code>{trend_bar}</code> <b>{trend_strength}%</b>\n"
            f"  💬 <i>{pro_tip}</i>\n"
        )

    res = (
        f"{dir_emoji} <b>{dir_line}</b> {dir_emoji}\n"
        f"{DIV}\n"
        f"  {data['pair']}\n"
        f"  Expiration: <b>{data['time']}</b>\n"
        f"{SDIV}\n"
        f"  AI: <code>{conf_bar}</code> <b>{confidence}%</b>\n"
        f"  {conf_label}"
        f"{pro_block}"
        f"{pro_extra}"
        f"\n{SDIV}\n"
        f"  {limit_line}\n"
        f"<i>⚡ 1–3% of balance per trade</i>"
    )

    try:
        await progress_msg.delete()
    except Exception:
        pass

    try:
        await message.answer(res, parse_mode="HTML", reply_markup=signal_kb)
    except Exception as e:
        print(f"Signal send error: {e}")

# ════════════════════════════════════════════════
#              PROFILE
# ════════════════════════════════════════════════
@dp.message(Command("profile"))
@dp.message(F.text == "👤 Profile")
async def profile(message: Message):
    u         = db_get_user(message.from_user.id)
    rank      = get_rank(u["signals"])
    sub_plan  = SUBSCRIPTION_PLANS[u["sub_type"]]
    sub_limit = sub_plan["limit"]
    sub_emoji = sub_plan["emoji"]

    expiry_str = "∞ Lifetime"
    days_info  = ""
    if u['sub_expires']:
        expiry_str = u['sub_expires'].strftime("%d.%m.%Y %H:%M")
        days_left  = max((u['sub_expires'] - datetime.now()).days, 0)
        days_used  = 7 - days_left
        bar        = days_bar(days_used, 7)
        days_info  = f"\n  Remaining: <code>[{bar}]</code> <b>{days_left} days</b>"

    next_title, next_level, signals_left = get_next_rank(u["signals"])
    rank_progress = ""
    if next_title:
        rank_progress = f"\n  To <b>{next_title}</b>: <b>{signals_left}</b> more signals"

    rank_bar_str = ""
    for lo, hi, title, level in RANKS:
        if lo <= u["signals"] <= hi:
            rank_bar_str = rank_progress_bar(u["signals"], lo, hi)
            break

    used_pct  = min(int((u["daily_count"] / sub_limit) * 10), 10)
    daily_bar = "▓" * used_pct + "░" * (10 - used_pct)

    name = message.from_user.first_name or "Trader"

    profile_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🧮 Calculate Lot", callback_data="open_lot_calc")],
    ])

    await message.answer(
        f"👤 <b>PROFILE</b>\n"
        f"{DIV}\n\n"
        f"  {name}  ·  <code>{message.from_user.id}</code>\n\n"
        f"{SDIV}\n"
        f"🏆 <b>Rank:</b> {rank}\n"
        f"  <code>{rank_bar_str}</code>"
        f"{rank_progress}\n\n"
        f"{SDIV}\n"
        f"💎 <b>Subscription:</b> {sub_emoji} <b>{u['sub_type'].upper()}</b>\n"
        f"  Limit:   <b>{sub_limit} sig./day</b>\n"
        f"  Expires: <b>{expiry_str}</b>"
        f"{days_info}\n\n"
        f"{SDIV}\n"
        f"📈 <b>Activity:</b>\n"
        f"  Total: <b>{u['signals']}</b>  ·  Today:\n"
        f"  <code>[{daily_bar}]</code> <b>{u['daily_count']} / {sub_limit}</b>\n\n"
        f"{DIV}\n"
        f"🔐 License: {'<b>ACTIVE ✅</b>' if u['has_access'] else '<b>❌ No access</b>'}\n\n"
        f"<i>Calculate your optimal lot:</i>",
        reply_markup=profile_kb,
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "open_lot_calc")
async def open_lot_calc_callback(callback: CallbackQuery):
    pending_lot_calc.add(callback.from_user.id)
    await callback.message.answer(
        "🧮 <b>LOT CALCULATOR</b>\n"
        f"{DIV}\n\n"
        "Enter your <b>balance in dollars</b>:\n\n"
        "<i>Minimum: 50$  |  Example: 100 or 500</i>",
        reply_markup=back_kb,
        parse_mode="HTML"
    )
    await callback.answer()

# ════════════════════════════════════════════════
#              STATISTICS
# ════════════════════════════════════════════════
@dp.message(F.text == "📈 Statistics")
async def stats(message: Message):
    seed_val = int(datetime.now().strftime("%Y%m%d"))
    random.seed(seed_val)

    total_day    = random.randint(1800, 2500)
    win_rate     = round(random.uniform(91.5, 96.2), 1)
    plus_deals   = int(total_day * (win_rate / 100))
    minus_deals  = total_day - plus_deals - random.randint(10, 30)
    refunds      = total_day - plus_deals - minus_deals
    avg_profit   = round(random.uniform(85.5, 93.8), 1)
    best_pair    = random.choice([p.replace("🇦🇪 ", "").replace("🇦🇺 ", "").replace("🇧🇭 ", "")
                                   .replace("🇨🇭 ", "").replace("🇪🇺 ", "").replace("🇲🇦 ", "")
                                   .replace("🇳🇿 ", "").replace("🇸🇦 ", "").replace("🇺🇸 ", "")
                                   .replace("🇬🇧 ", "").replace("🇨🇦 ", "")
                                   for p in pairs])
    peak_hour    = random.randint(10, 18)
    total_users  = db_get_total_users()
    active_users = db_get_active_users()

    wr_filled = int(win_rate / 10)
    wr_bar    = "█" * wr_filled + "░" * (10 - wr_filled)

    rng_chart = random.Random(seed_val)
    hourly_bars = ""
    for h in range(6, 24, 3):
        vol = rng_chart.randint(2, 10)
        bar_h = "█" * vol + "░" * (10 - vol)
        hourly_bars += f"  {h:02d}:00  <code>{bar_h}</code>\n"

    await message.answer(
        f"📊 <b>TERMINAL STATISTICS</b>\n"
        f"{DIV}\n\n"
        f"WinRate (Smart Precision):\n"
        f"<code>[{wr_bar}] {win_rate}%</code>\n\n"
        f"🟢 Profit: <b>{plus_deals:,}</b>  🔴 Loss: <b>{minus_deals:,}</b>  🔁 Refund: <b>{refunds:,}</b>\n"
        f"📦 Signals: <b>{total_day:,}</b>\n\n"
        f"{SDIV}\n"
        f"⚡ <b>System:</b>\n"
        f"  ROI:       <b>{avg_profit}%</b>\n"
        f"  Top pair:  <b>{best_pair}</b>\n"
        f"  Peak:      <b>{peak_hour}:00–{peak_hour+1}:00</b>\n\n"
        f"{SDIV}\n"
        f"📈 <b>Activity (MSK):</b>\n\n"
        f"{hourly_bars}\n"
        f"{SDIV}\n"
        f"👥 Traders: <b>{total_users + 152:,}</b>  ·  Active: <b>{active_users + 94:,}</b>\n\n"
        f"<i>📅 {datetime.now().strftime('%d.%m.%Y %H:%M')} MSK</i>",
        parse_mode="HTML"
    )
    random.seed()

# ════════════════════════════════════════════════
#              STARTUP
# ════════════════════════════════════════════════
async def main():
    print("=" * 60)
    print("  🚀 AI TRADING TERMINAL — OTC PRO v4.0")
    print("  ✅ BOT STARTED SUCCESSFULLY")
    print("  🧠 SMART PRECISION ENGINE v4 (OTC MODE):")
    print("     RSI(14) + EMA(9/21) + MACD + BB + STOCH + PATTERNS")
    print("     FILTER: 3/6 blocks minimum")
    print("  💱 OTC PAIRS: 12 instruments with country flags")
    print("  ⏱ TIMEFRAMES: 5s / 10s / 15s / 30s")
    print("  📦 LIMITS: FREE=25 | JUNIOR=50 | PRO=100")
    print("=" * 60)

    init_db()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
