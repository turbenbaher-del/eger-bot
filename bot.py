import os
import json
import math
import re
import logging
import asyncio
import aiohttp
import feedparser
from datetime import datetime, time as dtime
import pytz

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    filters, ContextTypes, ConversationHandler, CallbackQueryHandler
)
import firebase_admin
from firebase_admin import credentials, firestore, storage, messaging as fcm

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ── Firebase ──
_svc = os.environ.get("FIREBASE_SERVICE_ACCOUNT", "")
cred = credentials.Certificate(json.loads(_svc))
firebase_admin.initialize_app(cred, {"storageBucket": "eger-ai.firebasestorage.app"})
db = firestore.client()

TOKEN    = os.environ["BOT_TOKEN"]
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))
MOSCOW_TZ = pytz.timezone("Europe/Moscow")

DEFAULT_LAT, DEFAULT_LON = 47.2357, 39.7015  # Ростов-на-Дону

# ── Рыболовные магазины ──
SHOPS = [
    {"name":"Рыболов",            "city":"Ростов-на-Дону","addr":"ул. Нариманова, 56",        "phone":"+7 (863) 269-44-00","lat":47.2201,"lon":39.7234},
    {"name":"Охота и Рыбалка",    "city":"Ростов-на-Дону","addr":"Большая Садовая, 62",       "phone":"+7 (863) 240-88-00","lat":47.2357,"lon":39.7134},
    {"name":"Мир Рыбака",         "city":"Ростов-на-Дону","addr":"ул. Красноармейская, 87",   "phone":"+7 (863) 261-00-55","lat":47.2289,"lon":39.7289},
    {"name":"Планета Рыбака",     "city":"Ростов-на-Дону","addr":"ул. Стачки, 198",           "phone":"+7 (863) 285-77-00","lat":47.2456,"lon":39.6812},
    {"name":"Фишермен",           "city":"Ростов-на-Дону","addr":"ул. Зорге, 52",             "phone":"+7 (863) 275-33-00","lat":47.2567,"lon":39.7623},
    {"name":"Рыболов-Аксай",      "city":"Аксай",          "addr":"ул. Ленина, 45",           "phone":"+7 (86350) 5-12-33","lat":47.2681,"lon":39.8699},
    {"name":"Крючок",             "city":"Батайск",        "addr":"ул. Свердлова, 23",        "phone":"+7 (86354) 6-44-00","lat":47.1456,"lon":39.7456},
    {"name":"Рыболовный Мир",     "city":"Азов",           "addr":"ул. Московская, 78",       "phone":"+7 (86342) 4-88-00","lat":47.1023,"lon":39.4123},
    {"name":"Морской Рыболов",    "city":"Азов",           "addr":"ул. Береговая, 5",         "phone":"+7 (86342) 3-55-00","lat":47.0934,"lon":39.4234},
    {"name":"Рыбак Таганрог",     "city":"Таганрог",       "addr":"ул. Чехова, 100",          "phone":"+7 (8634) 31-44-00","lat":47.2089,"lon":38.9234},
    {"name":"Цимлянские Снасти",  "city":"Цимлянск",       "addr":"ул. Строителей, 8",        "phone":"+7 (86391) 2-33-00","lat":47.6421,"lon":42.0954},
    {"name":"Рыболов Волгодонск", "city":"Волгодонск",     "addr":"ул. Рабочая, 88",          "phone":"+7 (8639) 22-44-00","lat":47.5134,"lon":42.1523},
]

# ── Точки рыбалки ──
SPOTS = [
    {"name":"Вторая дамба (Аксай)",       "fish":"лещ, сазан, карп",    "lat":47.2681,"lon":39.8699},
    {"name":"Гниловская переправа",        "fish":"судак, жерех",         "lat":47.1734,"lon":39.7012},
    {"name":"Мёртвый Донец (Ливенцовка)", "fish":"щука, карась",         "lat":47.2198,"lon":39.6234},
    {"name":"Камышовая заводь (Батайск)", "fish":"карась, карп",         "lat":47.1456,"lon":39.7456},
    {"name":"Перекат ниже Аксая",         "fish":"сазан, жерех",         "lat":47.2456,"lon":39.9012},
    {"name":"Цимлянское вдхр.",           "fish":"лещ, судак, сом",      "lat":47.6421,"lon":42.0954},
    {"name":"Манычское вдхр.",            "fish":"амур, карп, толстолоб","lat":46.7012,"lon":41.7234},
    {"name":"Устье Дона (Азов)",          "fish":"тарань, бычок",        "lat":47.1023,"lon":39.4123},
    {"name":"Быстрый перекат (Аксай)",    "fish":"жерех, чехонь",        "lat":47.2612,"lon":39.8812},
    {"name":"Глубокая яма у Аксая",       "fish":"сом, судак",           "lat":47.2534,"lon":39.8923},
]

FISH_KEYWORDS = [
    "поймал","поймала","улов","клюёт","клюет","клёв","клев",
    "щука","судак","лещ","карась","сазан","амур","окунь",
    "карп","толстолоб","берш","жерех","налим","сом",
    "рыбалка","рыбачил","спиннинг","фидер","поплавок",
    "дон","аксай","донец","цимла","манычское",
    "кг","кило","граммов","штук","хвостов",
]

RSS_CHANNELS = ["rybolov_don","fishing_rostov61","don_rybalka"]

REPORT_TITLE, REPORT_LOCATION, REPORT_FISH, REPORT_PHOTO, REPORT_TEXT = range(5)


# ── Утилиты ──

def haversine(lat1, lon1, lat2, lon2) -> float:
    R = 6371
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return R * 2 * math.asin(math.sqrt(a))


def nearest(items, lat, lon, n=3):
    return sorted(items, key=lambda s: haversine(lat, lon, s["lat"], s["lon"]))[:n]


def calc_bite_score(pressure_mmhg: float, month: int) -> int:
    score = 7
    if 758 <= pressure_mmhg <= 768:
        score += 2
    elif pressure_mmhg > 768:
        score += 1
    elif pressure_mmhg < 748:
        score -= 3
    elif pressure_mmhg < 755:
        score -= 1
    if month in (2, 3, 4):   # март–май
        score += 1
    elif month in (6, 7):    # июль–август, жара
        score -= 1
    return max(1, min(10, score))


async def fetch_weather(lat: float, lon: float) -> dict:
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        "&current=temperature_2m,wind_speed_10m,surface_pressure,precipitation,weathercode"
        "&wind_speed_unit=ms&timezone=Europe%2FMoscow"
    )
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
                data = await r.json()
        c = data["current"]
        pressure_hpa = c.get("surface_pressure", 1013)
        pressure_mmhg = round(pressure_hpa * 0.750064)
        temp = round(c.get("temperature_2m", 18))
        wind = round(c.get("wind_speed_10m", 3))
        precip = c.get("precipitation", 0)
        water_temp = max(4, temp - 3)
        return {
            "temp": temp, "wind": wind,
            "pressure": pressure_mmhg,
            "precip": precip,
            "water": water_temp,
            "ok": True
        }
    except Exception as e:
        logger.warning(f"Weather fetch failed: {e}")
        return {"temp": 18, "wind": 3, "pressure": 763, "precip": 0, "water": 15, "ok": False}


def pressure_trend(p: int) -> str:
    if p >= 768: return "↑ высокое"
    if p <= 748: return "↓ низкое"
    if 758 <= p <= 768: return "→ норма"
    return "↓ ниже нормы"


def build_forecast(w: dict, location_name: str, lat: float, lon: float) -> str:
    month = datetime.now().month - 1  # 0-indexed
    score = calc_bite_score(w["pressure"], month)
    score_label = (
        "🔴 Слабый" if score < 4 else
        "🟡 Средний" if score < 6 else
        "🟢 Хороший" if score < 8 else
        "⭐ Отличный!"
    )

    if score >= 7:
        fish_tip = "✅ Лещ и сазан — активны, фидер у бровки\n✅ Карась — у камышей, утро–вечер\n✅ Щука и судак — активны весь день"
        time_tip = "🌅 Лучшее время: 5:00–8:00 и 18:00–21:00"
    elif score >= 5:
        fish_tip = "✅ Лещ — средняя активность, опарыш\n⚠️ Карп и сазан — капризят\n✅ Хищник (щука, окунь) — берёт"
        time_tip = "🌅 Лучшее время: раннее утро 5:00–7:00"
    else:
        fish_tip = "⚠️ Мирная рыба (лещ, карась) — пассивна\n✅ Хищник (щука, судак) — активнее в ненастье\n⚠️ Ловить сложно — рыба стоит у дна"
        time_tip = "🕐 Лучшее время: ближе к ночи, сумерки"

    precip_str = f"🌧 Осадки: {w['precip']} мм\n" if w['precip'] > 0.5 else ""

    # Ближайшие точки
    close_spots = nearest(SPOTS, lat, lon, 2)
    spots_str = "\n".join(
        f"📍 {s['name']} — {s['fish']}\nhttps://maps.google.com/?q={s['lat']},{s['lon']}"
        for s in close_spots
    )

    # Ближайшие магазины
    close_shops = nearest(SHOPS, lat, lon, 2)
    shops_str = "\n".join(
        f"🏪 {s['name']}, {s['city']}\n📞 {s['phone']}"
        for s in close_shops
    )

    now_str = datetime.now(MOSCOW_TZ).strftime("%d.%m.%Y %H:%M")

    return (
        f"🎣 *Прогноз клёва — {location_name}*\n"
        f"📅 {now_str} МСК\n\n"
        f"🌡 Воздух: +{w['temp']}°C · Вода: ~+{w['water']}°C\n"
        f"💨 Ветер: {w['wind']} м/с\n"
        f"📊 Давление: {w['pressure']} мм рт.ст. {pressure_trend(w['pressure'])}\n"
        f"{precip_str}\n"
        f"*Клёв: {score}/10 — {score_label}*\n\n"
        f"{fish_tip}\n"
        f"{time_tip}\n\n"
        f"*Ближайшие точки ловли:*\n{spots_str}\n\n"
        f"*Магазины рядом:*\n{shops_str}"
    )


# ── AI ответы ──
def get_reply(text: str) -> str:
    l = text.lower()
    if "амур" in l or "камыш" in l:
        return "Белый амур жирует у камышовых крепей — вода прогрелась. Бери варёную кукурузу, молодую траву. Тишина обязательна — пугливый. Вторая дамба или Манычское вдхр."
    if "щук" in l:
        return "Щука стоит у затопленных кустов и коряжника. Воблер-суспендер 7–9 см или джерк. Утром активна у берега. Мёртвый Донец (Ливенцовка) — топовое место."
    if "лещ" in l or "опарыш" in l:
        return "Лещ на 4–6 м у бровки старого русла. Фидер, кормушка 60–80 г, опарыш 2–3 шт + кориандровая прикормка. Клёв 5:00–8:00 и 17:00–20:00. Вторая дамба — лучшее место."
    if "карась" in l:
        return "Карась в камышовых заводях — тихая вода с илом. Поплавочная 5 м, мотыль или красный червь. Камышовая заводь Батайска, ранним утром 6:00–9:00."
    if "сазан" in l or "бойл" in l:
        return "Сазан выходит при стабильном давлении. Перекаты ниже Аксая — его место. Карповая снасть, бойлы 15–18 мм или варёный горох. С вечера — ночью жор."
    if "судак" in l or "джиг" in l:
        return "Судак на свале — где глубина с 3 до 8 м. Джиг 20–28 г, твистер белый или «машинное масло». Ступенчатая проводка у дна. Гниловская переправа — точка 🎯"
    if "сом" in l:
        return "Сом — ночной хищник, жор с 22:00 до 3:00. Глубокая яма под обрывом. Крупный червь пучком или живец 200+ г. Яма у Аксая — там сом есть точно."
    if "прогноз" in l or "клёв" in l or "клев" in l:
        return "Напиши /forecast — пришлю актуальный прогноз клёва на основе реальной погоды. Или поделись геолокацией для точного прогноза по твоему месту."
    if "магазин" in l or "снасти" in l or "купить" in l:
        return "В Ростове много хороших магазинов! Отправь /shops — покажу ближайшие к тебе с телефонами."
    replies = [
        "Спроси конкретнее — про какую рыбу или место? Отвечу по-егерски: точно и по делу.",
        "На Дону сейчас самое время! Напиши что интересует: точки, снасти, прогноз клёва — всё расскажу.",
        "Лучший клёв — на рассвете, с 5 до 8. Куда направляешься? Подскажу лучшую точку.",
    ]
    import random
    return random.choice(replies)


# ── /start ──
async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db.collection("bot_users").document(str(user.id)).set({
        "name": user.full_name,
        "username": user.username or "",
        "chat_id": user.id,
        "subscribed": True,
        "joined": firestore.SERVER_TIMESTAMP,
    }, merge=True)

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🌤 Прогноз клёва", callback_data="forecast")],
        [InlineKeyboardButton("📝 Добавить отчёт", callback_data="report_start"),
         InlineKeyboardButton("🔔 Уведомления", callback_data="subscribe")],
        [InlineKeyboardButton("🏪 Магазины рядом", callback_data="shops"),
         InlineKeyboardButton("📍 Точки ловли", callback_data="spots")],
        [InlineKeyboardButton("🌐 Открыть сайт", url="https://turbenbaher-del.github.io/eger-ai/")],
    ])
    await update.message.reply_text(
        f"Здорово, {user.first_name}! 🎣\n\n"
        "Я — *Егерь ИИ*, знаю каждый омут Дона и Ростовской области.\n\n"
        "Просто напиши вопрос о рыбалке — отвечу.\n"
        "Или поделись 📍 геолокацией — дам прогноз и магазины прямо рядом с тобой!",
        parse_mode="Markdown",
        reply_markup=kb
    )


# ── /forecast — запрашивает геолокацию ──
async def forecast_ask_location(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.message or (update.callback_query.message if update.callback_query else None)
    if update.callback_query:
        await update.callback_query.answer()

    location_kb = ReplyKeyboardMarkup([
        [KeyboardButton("📍 Отправить мою геолокацию", request_location=True)],
        [KeyboardButton("🎣 Прогноз для Ростова (без геолокации)")],
    ], resize_keyboard=True, one_time_keyboard=True)

    await msg.reply_text(
        "📍 Поделись геолокацией — дам *персональный прогноз* для твоего места:\n"
        "• точная погода в твоём районе\n"
        "• ближайшие точки ловли\n"
        "• магазины рядом с тобой",
        parse_mode="Markdown",
        reply_markup=location_kb
    )


# ── Обработка геолокации ──
async def handle_location(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    loc = update.message.location
    lat, lon = loc.latitude, loc.longitude

    # Сохраняем геолокацию пользователя
    db.collection("bot_users").document(str(update.effective_user.id)).set(
        {"last_lat": lat, "last_lon": lon}, merge=True
    )

    await update.message.reply_text(
        "📡 Получил геолокацию, загружаю погоду...",
        reply_markup=ReplyKeyboardRemove()
    )

    w = await fetch_weather(lat, lon)
    location_name = f"{round(lat,4)}, {round(lon,4)}"

    # Пробуем определить ближайший город
    cities = [
        {"name":"Ростов-на-Дону","lat":47.2357,"lon":39.7015},
        {"name":"Аксай",         "lat":47.2681,"lon":39.8699},
        {"name":"Батайск",       "lat":47.1456,"lon":39.7456},
        {"name":"Азов",          "lat":47.1023,"lon":39.4123},
        {"name":"Таганрог",      "lat":47.2089,"lon":38.9234},
        {"name":"Новочеркасск",  "lat":47.4189,"lon":40.0934},
        {"name":"Волгодонск",    "lat":47.5134,"lon":42.1523},
        {"name":"Цимлянск",      "lat":47.6421,"lon":42.0954},
    ]
    nearest_city = min(cities, key=lambda c: haversine(lat, lon, c["lat"], c["lon"]))
    if haversine(lat, lon, nearest_city["lat"], nearest_city["lon"]) < 30:
        location_name = f"р-н {nearest_city['name']}"

    text = build_forecast(w, location_name, lat, lon)
    await update.message.reply_text(text, parse_mode="Markdown", disable_web_page_preview=True)


# ── Прогноз для Ростова (без геолокации) ──
async def forecast_rostov(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "⏳ Загружаю актуальную погоду для Ростова...",
        reply_markup=ReplyKeyboardRemove()
    )
    w = await fetch_weather(DEFAULT_LAT, DEFAULT_LON)
    text = build_forecast(w, "Ростов-на-Дону", DEFAULT_LAT, DEFAULT_LON)
    await update.message.reply_text(text, parse_mode="Markdown", disable_web_page_preview=True)


# ── /shops — ближайшие магазины ──
async def cmd_shops(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.message or (update.callback_query.message if update.callback_query else None)
    if update.callback_query:
        await update.callback_query.answer()

    # Проверяем есть ли сохранённая геолокация
    user_id = (update.effective_user or update.callback_query.from_user).id
    doc = db.collection("bot_users").document(str(user_id)).get()
    data = doc.to_dict() if doc.exists else {}
    lat = data.get("last_lat", DEFAULT_LAT)
    lon = data.get("last_lon", DEFAULT_LON)
    location_name = "Ростов-на-Дону" if (lat == DEFAULT_LAT and lon == DEFAULT_LON) else "твоим местом"

    close = nearest(SHOPS, lat, lon, 5)
    lines = []
    for i, s in enumerate(close, 1):
        dist = haversine(lat, lon, s["lat"], s["lon"])
        dist_str = f"{dist:.0f} км" if dist >= 1 else "рядом"
        lines.append(
            f"{i}. *{s['name']}* — {s['city']}\n"
            f"   📍 {s['addr']} ({dist_str})\n"
            f"   📞 {s['phone']}"
        )

    tip = "ℹ️ _Для точного расстояния поделись геолокацией через /forecast_" if lat == DEFAULT_LAT else ""

    await msg.reply_text(
        f"🏪 *Рыболовные магазины рядом с {location_name}:*\n\n"
        + "\n\n".join(lines)
        + (f"\n\n{tip}" if tip else ""),
        parse_mode="Markdown"
    )


# ── /spots — ближайшие точки ловли ──
async def cmd_spots(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.message or (update.callback_query.message if update.callback_query else None)
    if update.callback_query:
        await update.callback_query.answer()

    user_id = (update.effective_user or update.callback_query.from_user).id
    doc = db.collection("bot_users").document(str(user_id)).get()
    data = doc.to_dict() if doc.exists else {}
    lat = data.get("last_lat", DEFAULT_LAT)
    lon = data.get("last_lon", DEFAULT_LON)

    close = nearest(SPOTS, lat, lon, 4)
    lines = []
    for s in close:
        dist = haversine(lat, lon, s["lat"], s["lon"])
        dist_str = f"{dist:.0f} км"
        lines.append(
            f"📍 *{s['name']}* ({dist_str})\n"
            f"🐟 {s['fish']}\n"
            f"🗺 https://maps.google.com/?q={s['lat']},{s['lon']}"
        )

    await msg.reply_text(
        "🎣 *Ближайшие точки ловли:*\n\n"
        + "\n\n".join(lines),
        parse_mode="Markdown",
        disable_web_page_preview=True
    )


# ── Текстовые сообщения ──
async def handle_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text

    # Кнопка с клавиатуры
    if text == "🎣 Прогноз для Ростова (без геолокации)":
        await forecast_rostov(update, ctx)
        return

    reply = get_reply(text)
    await update.message.reply_text(f"🎣 {reply}")


# ── Кнопки (InlineKeyboard) ──
async def handle_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    data = update.callback_query.data
    if data == "forecast":
        await forecast_ask_location(update, ctx)
    elif data == "subscribe":
        await subscribe_toggle(update, ctx)
    elif data == "shops":
        await cmd_shops(update, ctx)
    elif data == "spots":
        await cmd_spots(update, ctx)


# ── Подписка ──
async def subscribe_toggle(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        await update.callback_query.answer()
        user = update.callback_query.from_user
        msg = update.callback_query.message
    else:
        user = update.effective_user
        msg = update.message

    ref = db.collection("bot_users").document(str(user.id))
    doc = ref.get()
    current = doc.to_dict().get("subscribed", True) if doc.exists else True
    ref.set({"subscribed": not current}, merge=True)
    status = "включены ✅" if not current else "отключены ❌"
    await msg.reply_text(f"🔔 Уведомления о клёве {status}")


# ── Отчёт ──
async def report_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        await update.callback_query.answer()
        msg = update.callback_query.message
    else:
        msg = update.message
    await msg.reply_text(
        "📝 *Новый отчёт об улове*\n\nШаг 1/5 — Введи заголовок:\n_(например: «Отличный улов на Дамбе-2»)_",
        parse_mode="Markdown"
    )
    return REPORT_TITLE


async def report_title(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["report"] = {"title": update.message.text}
    await update.message.reply_text(
        "📍 Шаг 2/5 — Место рыбалки:\n_(например: «Вторая дамба, Аксайский р-н»)_\n\nИли /skip чтобы пропустить",
        parse_mode="Markdown"
    )
    return REPORT_LOCATION


async def report_location(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["report"]["location"] = "" if update.message.text == "/skip" else update.message.text
    await update.message.reply_text(
        "🐟 Шаг 3/5 — Что поймал?\n_(например: «Лещ 1.2 кг, карась 5 штук»)_\n\nИли /skip чтобы пропустить",
        parse_mode="Markdown"
    )
    return REPORT_FISH


async def report_fish(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["report"]["fish"] = "" if update.message.text == "/skip" else update.message.text
    await update.message.reply_text(
        "📸 Шаг 4/5 — Отправь фото улова:\n\nИли /skip чтобы пропустить",
        parse_mode="Markdown"
    )
    return REPORT_PHOTO


async def report_photo(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.message.text == "/skip":
        ctx.user_data["report"]["photo_url"] = ""
    else:
        photo = update.message.photo[-1]
        file = await ctx.bot.get_file(photo.file_id)
        bucket = storage.bucket()
        blob = bucket.blob(f"reports/{update.effective_user.id}_{photo.file_id}.jpg")
        photo_bytes = await file.download_as_bytearray()
        blob.upload_from_string(bytes(photo_bytes), content_type="image/jpeg")
        blob.make_public()
        ctx.user_data["report"]["photo_url"] = blob.public_url
    await update.message.reply_text(
        "✍️ Шаг 5/5 — Расскажи подробнее:\n_(снасти, насадка, время клёва, советы)_",
        parse_mode="Markdown"
    )
    return REPORT_TEXT


async def report_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    report = ctx.user_data.get("report", {})
    report["body"] = update.message.text
    report["displayName"] = user.full_name
    report["uid"] = f"tg_{user.id}"
    report["timestamp"] = firestore.SERVER_TIMESTAMP
    report["createdAt"] = firestore.SERVER_TIMESTAMP
    report["source"] = "telegram"
    db.collection("reports").add(report)
    await update.message.reply_text(
        "✅ *Отчёт опубликован на сайте!*\n\n"
        f"🏆 {report.get('title','')}\n"
        f"📍 {report.get('location','—')}\n"
        f"🐟 {report.get('fish','—')}\n\n"
        "Смотри в разделе «Отчёты» 👉 https://turbenbaher-del.github.io/eger-ai/",
        parse_mode="Markdown"
    )
    ctx.user_data.clear()
    return ConversationHandler.END


async def report_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.clear()
    await update.message.reply_text("❌ Отчёт отменён.")
    return ConversationHandler.END


# ── Мониторинг чатов ──
def is_fishing_report(text: str) -> bool:
    if not text or len(text) < 30:
        return False
    return sum(1 for kw in FISH_KEYWORDS if kw in text.lower()) >= 2


async def handle_group_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.message or update.channel_post
    if not msg or not msg.text:
        return
    chat = msg.chat
    monitored = db.collection("monitored_chats").document(str(chat.id)).get()
    if not monitored.exists:
        return
    if not is_fishing_report(msg.text):
        return
    sender = msg.from_user
    name = sender.full_name if sender else chat.title or "Чат"
    db.collection("reports").add({
        "title": msg.text[:80].split("\n")[0] or "Отчёт из чата",
        "body": msg.text[:1000],
        "displayName": f"{name} (из {chat.title or 'чата'})",
        "uid": f"tg_chat_{chat.id}",
        "source": "telegram_chat",
        "chat_title": chat.title or "",
        "timestamp": firestore.SERVER_TIMESTAMP,
        "createdAt": firestore.SERVER_TIMESTAMP,
    })


# ── Админ команды ──
async def cmd_addchat(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type == "private":
        await update.message.reply_text("❌ Используй в групповом чате")
        return
    chat = update.effective_chat
    db.collection("monitored_chats").document(str(chat.id)).set({
        "title": chat.title, "username": chat.username or "",
        "added_by": update.effective_user.id,
        "timestamp": firestore.SERVER_TIMESTAMP,
    })
    await update.message.reply_text(f"✅ Чат *{chat.title}* добавлен в мониторинг!", parse_mode="Markdown")


async def cmd_removechat(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    db.collection("monitored_chats").document(str(chat.id)).delete()
    await update.message.reply_text(f"❌ Чат *{chat.title}* удалён.", parse_mode="Markdown")


async def cmd_listchats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID and ADMIN_ID != 0:
        return
    chats = db.collection("monitored_chats").stream()
    lines = [f"• {c.to_dict().get('title','?')}" for c in chats]
    await update.message.reply_text(
        "📋 *Мониторируемые чаты:*\n" + ("\n".join(lines) if lines else "_(пусто)_"),
        parse_mode="Markdown"
    )


# ── RSS ──
async def monitor_rss(app: Application):
    seen_ids: set = set()
    while True:
        for channel in RSS_CHANNELS:
            try:
                feed = feedparser.parse(f"https://rsshub.app/telegram/channel/{channel}")
                for entry in feed.entries[:5]:
                    eid = entry.get("id", entry.get("link", ""))
                    if eid in seen_ids:
                        continue
                    seen_ids.add(eid)
                    text = entry.get("summary", entry.get("title", ""))
                    if len(text) < 20:
                        continue
                    db.collection("news_telegram").add({
                        "channel": channel,
                        "title": entry.get("title", "")[:100],
                        "text": text[:800],
                        "link": entry.get("link", ""),
                        "published": entry.get("published", ""),
                        "timestamp": firestore.SERVER_TIMESTAMP,
                    })
            except Exception as e:
                logger.warning(f"RSS {channel}: {e}")
        await asyncio.sleep(3600)


# ── Утренние уведомления ──
async def send_morning_notifications(ctx: ContextTypes.DEFAULT_TYPE):
    w = await fetch_weather(DEFAULT_LAT, DEFAULT_LON)
    users = db.collection("bot_users").where("subscribed", "==", True).stream()
    for u in users:
        data = u.to_dict()
        try:
            lat = data.get("last_lat", DEFAULT_LAT)
            lon = data.get("last_lon", DEFAULT_LON)
            # Загружаем персональную погоду если есть сохранённая геолокация
            if lat != DEFAULT_LAT or lon != DEFAULT_LON:
                w_user = await fetch_weather(lat, lon)
                location = "твоего района"
            else:
                w_user = w
                location = "Ростова-на-Дону"
            forecast_text = build_forecast(w_user, location, lat, lon)
            await ctx.bot.send_message(
                chat_id=data["chat_id"],
                text=f"🌅 *Доброе утро, рыбак!*\n\n{forecast_text}",
                parse_mode="Markdown",
                disable_web_page_preview=True
            )
        except Exception as e:
            logger.warning(f"Notify failed {data.get('chat_id')}: {e}")


# ── FCM push ──
def _do_send_fcm(title: str, body: str, url: str):
    try:
        tokens = [d.id for d in db.collection("fcm_tokens").stream()]
        if not tokens:
            return
        for i in range(0, len(tokens), 500):
            batch = tokens[i:i+500]
            resp = fcm.send_multicast(fcm.MulticastMessage(
                notification=fcm.Notification(title=title, body=body),
                data={"url": url},
                tokens=batch,
            ))
            for j, r in enumerate(resp.responses):
                if not r.success:
                    try: db.collection("fcm_tokens").document(batch[j]).delete()
                    except Exception: pass
            logger.info(f"FCM: {resp.success_count}/{len(batch)} delivered")
    except Exception as e:
        logger.error(f"FCM send error: {e}")


def _classify_tag(title: str) -> str:
    tl = title.lower()
    if any(w in tl for w in ["запрет", "штраф"]): return "Запрет"
    if any(w in tl for w in ["клёв", "клев", "улов"]): return "Клёв"
    if "нерест" in tl: return "Нерест"
    if "соревнован" in tl: return "Соревнования"
    if any(w in tl for w in ["уровень", "паводок", "вода", "гидро"]): return "Гидрология"
    return "Новости"


def _save_news_items(items: list):
    """Save new items to Firestore news collection (sync, called via executor)."""
    try:
        batch = db.batch()
        for item in items:
            ref = db.collection("news").document(item["id"])
            batch.set(ref, {
                "title": item["title"],
                "text": item["desc"],
                "link": item["link"],
                "source": "Google Новости",
                "tag": _classify_tag(item["title"]),
                "timestamp": firestore.SERVER_TIMESTAMP,
            })
        batch.commit()
        logger.info(f"Saved {len(items)} news items to Firestore")
    except Exception as e:
        logger.error(f"Firestore news save error: {e}")


async def monitor_web_news(app: Application):
    """Poll Google News RSS every 10 min, save to Firestore and send FCM push."""
    seen_ids: set = set()
    loop = asyncio.get_event_loop()
    feeds = [
        "https://news.google.com/rss/search?q=рыбалка+Ростов+Дон&hl=ru&gl=RU&ceid=RU:ru",
        "https://news.google.com/rss/search?q=рыболовство+запрет+нерест&hl=ru&gl=RU&ceid=RU:ru",
    ]

    # Warm-up: fetch RSS, save to Firestore immediately, seed seen_ids
    for feed_url in feeds:
        try:
            feed = await loop.run_in_executor(None, feedparser.parse, feed_url)
            to_save = []
            for entry in feed.entries[:8]:
                eid = entry.get("link") or entry.get("id") or entry.get("title", "")
                title = entry.get("title", "")
                if eid and title and eid not in seen_ids:
                    seen_ids.add(eid)
                    summary = re.sub(r'<[^>]+>', '', entry.get("summary", ""))[:220]
                    pub = entry.get("published", "")
                    to_save.append({"id": re.sub(r'[^\w]', '_', eid)[:100], "title": title, "link": entry.get("link", ""), "desc": summary})
            if to_save:
                await loop.run_in_executor(None, _save_news_items, to_save)
        except Exception as e:
            logger.warning(f"Warm-up error: {e}")
    logger.info(f"Web news warm-up: {len(seen_ids)} IDs saved to Firestore")

    while True:
        await asyncio.sleep(10 * 60)
        new_items = []
        for feed_url in feeds:
            try:
                feed = await loop.run_in_executor(None, feedparser.parse, feed_url)
                for entry in feed.entries[:8]:
                    eid = entry.get("link") or entry.get("id") or entry.get("title", "")
                    title = entry.get("title", "")
                    if eid and eid not in seen_ids and title:
                        seen_ids.add(eid)
                        summary = re.sub(r'<[^>]+>', '', entry.get("summary", ""))[:220]
                        pub = entry.get("published", "")
                        try:
                            from email.utils import parsedate_to_datetime
                            date_str = parsedate_to_datetime(pub).strftime("%-d %B")
                        except Exception:
                            date_str = ""
                        new_items.append({"id": re.sub(r'[^\w]', '_', eid)[:100], "title": title, "link": entry.get("link", ""), "desc": summary, "date": date_str})
            except Exception as e:
                logger.warning(f"Web news poll error: {e}")

        if new_items:
            # Save to Firestore (site reads via onSnapshot)
            await loop.run_in_executor(None, _save_news_items, new_items)
            # Send FCM push
            if len(new_items) == 1:
                n = new_items[0]
                push_title = f"🎣 {n['title']}"
                push_body  = n['desc'][:120]
                push_url   = n['link'] or "https://turbenbaher-del.github.io/eger-ai/"
            else:
                push_title = f"🎣 Рыбалка: {len(new_items)} новых новостей"
                push_body  = " · ".join(i['title'] for i in new_items[:3])[:120]
                push_url   = "https://turbenbaher-del.github.io/eger-ai/"
            logger.info(f"Web news: {len(new_items)} new → Firestore + FCM")
            await loop.run_in_executor(None, _do_send_fcm, push_title, push_body, push_url)


# ── Main ──
def main():
    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("forecast", forecast_ask_location))
    app.add_handler(CommandHandler("shops", cmd_shops))
    app.add_handler(CommandHandler("spots", cmd_spots))
    app.add_handler(CommandHandler("subscribe", subscribe_toggle))
    app.add_handler(CommandHandler("unsubscribe", subscribe_toggle))
    app.add_handler(CommandHandler("addchat", cmd_addchat))
    app.add_handler(CommandHandler("removechat", cmd_removechat))
    app.add_handler(CommandHandler("listchats", cmd_listchats))

    app.add_handler(MessageHandler(filters.LOCATION, handle_location))

    app.add_handler(MessageHandler(
        filters.TEXT & (filters.ChatType.GROUPS | filters.ChatType.CHANNEL),
        handle_group_message
    ))

    report_conv = ConversationHandler(
        entry_points=[
            CommandHandler("report", report_start),
            CallbackQueryHandler(report_start, pattern="^report_start$"),
        ],
        states={
            REPORT_TITLE:    [MessageHandler(filters.TEXT & ~filters.COMMAND, report_title)],
            REPORT_LOCATION: [MessageHandler(filters.TEXT, report_location)],
            REPORT_FISH:     [MessageHandler(filters.TEXT, report_fish)],
            REPORT_PHOTO:    [MessageHandler(filters.PHOTO | filters.TEXT, report_photo)],
            REPORT_TEXT:     [MessageHandler(filters.TEXT & ~filters.COMMAND, report_text)],
        },
        fallbacks=[CommandHandler("cancel", report_cancel)],
    )
    app.add_handler(report_conv)

    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    app.job_queue.run_daily(
        send_morning_notifications,
        time=dtime(hour=3, minute=0, tzinfo=pytz.utc),
        name="morning_forecast"
    )

    loop = asyncio.get_event_loop()
    loop.create_task(monitor_rss(app))
    loop.create_task(monitor_web_news(app))

    logger.info("Егерь-бот запущен!")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
