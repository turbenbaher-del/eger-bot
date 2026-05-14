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
CATCH_FISH, CATCH_WEIGHT, CATCH_LOCATION, CATCH_PHOTO = range(5, 9)


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
    import random
    l = text.lower()
    month = datetime.now(MOSCOW_TZ).month
    season = "весна" if month in (3,4,5) else "лето" if month in (6,7,8) else "осень" if month in (9,10,11) else "зима"

    # ── ЛЕЩ ──
    if any(w in l for w in ["лещ","подлещик","лещик"]):
        if season == "весна":
            return "🐟 *Лещ весной* — самый жор перед нерестом (март–апрель). Фидер 3.6–4.2 м, кормушка 80–120 г. Леска 0.20 мм, крючок №8–10. Насадка: опарыш 3–4 шт или пучок мотыля. Прикормка с кориандром и анисом. Глубина 5–7 м у бровки русла. Лучшее место — Вторая дамба (Аксай). Клёв 5:00–8:30 и 17:00–20:00. После нереста (май) — снова жрёт, насадка червь."
        if season == "лето":
            return "🐟 *Лещ летом* — ночник. Жор с 22:00 до 4:00. Фидер на глубоких ямах 6–9 м. Кормушка 100–120 г (сильное течение). Опарыш + кукуруза на крючке. Прикормка с ароматизатором клубники. Цимлянское вдхр. — стабильно хорошо всё лето."
        if season == "осень":
            return "🐟 *Лещ осенью* (сентябрь–октябрь) — активно жирует перед зимой. Уходит на свалы 7–10 м. Фидер дальний заброс, кормушка 100 г. Насадка: червь + опарыш бутерброд. Клёв стабильный весь день. Вторая дамба и Цимла — лучшие точки."
        return "🐟 *Лещ на Дону*: фидер 3.6–4.2 м, кормушка 60–120 г в зависимости от течения. Глубина 4–8 м у бровки. Насадка — опарыш, мотыль, червь, кукуруза. Прикормка с кориандром обязательна. Лучшие точки: Вторая дамба (Аксай), Цимлянское вдхр. Клёв: 5:00–9:00 и вечер."

    # ── СУДАК / БЕРШ ──
    if any(w in l for w in ["судак","клыкастый","берш","судачок"]):
        if season in ("осень","зима"):
            return "🎣 *Судак осенью/зимой* — пик активности! Глубина 5–12 м у свала. Джиг 20–35 г, поролоновая рыбка или виброхвост 7–10 см. Цвет: белый, жёлтый, «машинное масло». Ступенчатая проводка у дна. Лучшие места: Гниловская переправа, глубокая яма у Аксая. Клёв активен с 7:00 до 11:00 и 16:00–21:00. Фрикцион не затягивай — сильный рывок!"
        return "🎣 *Судак на Дону*: джиг-спиннинг, 15–28 г в зависимости от глубины. Леска плетня 0.10–0.14 мм + флюрокарбон-поводок 0.25 мм (судак видит леску!). Твистер 5–8 см, белый или перламутр. Ищи свал с 3 до 8 м — это его стол. Гниловская переправа и яма у Аксая — топовые точки. Лучший клёв вечером 18:00–23:00."

    # ── САЗАН / КАРП ──
    if any(w in l for w in ["сазан","карп","бойл","макух","макуха","карпов"]):
        return "🐠 *Сазан/Карп на Дону*: карповая снасть, удилище 3.6–3.9 м тест 3–3.5 lb. Леска 0.28–0.35 мм. Крючок №2–6 под бойли или corn. Насадка: бойлы 15–20 мм (клубника, слива, кукуруза), варёная кукуруза, горох, тесто с макухой. Прикормка: конопля + кукуруза + пшеница. Глубина 3–6 м у тростника и поворотов реки. Лучшее место — Перекат ниже Аксая и Камышовая заводь. Клёв: с заката до 4:00 ночи и ранним утром 5:00–7:00. При стабильном давлении 758–768 мм — сазан обязательно выходит."

    # ── ЩУКА ──
    if any(w in l for w in ["щук","щучк","щучий"]):
        if season == "весна":
            return "🐊 *Щука весной* — преднерестовый жор в марте! Самая крупная щука берёт. Воблер-суспендер 9–12 см или джерк-бейт. Ищи у затопленных кустов и в заводях. После нереста (апрель) — отходит и не берёт 2–3 недели. Мёртвый Донец (Ливенцовка) — заповедное место."
        return "🐊 *Щука на Дону*: спиннинг средний 10–28 г. Воблер-суспендер 7–12 см, джерк, крупный твистер на офсетнике. Цвет: натуральный (карась, плотва), яркий в мутной воде. Ищи структуру — коряжник, затопленные кусты, граница камыша. Всегда бросай параллельно берегу. Мёртвый Донец — лучшая щучья точка. Клёв: рассвет 5:00–8:30, облачные дни — весь день."

    # ── СОМ ──
    if "сом" in l:
        return "🐋 *Сом на Дону* — ночной охотник. Активен с конца мая по сентябрь, пик июль–август. Тяжёлая снасть: удилище с тестом 100–200 г, леска 0.50–0.80 мм или шнур 50+ lb. Крючок №1–3/0, насадка: пучок крупных червей, живец 150–300 г, лягушка, рак. Техника квок (knock деревянным квоком по воде) — сом реагирует. Глубокие ямы 5–12 м под обрывистым берегом. Яма у Аксая и Цимлянское вдхр. — там сом есть точно. Клёв: 22:00–4:00."

    # ── ЖЕРЕХ ──
    if any(w in l for w in ["жерех","жерешок","asp"]):
        return "💨 *Жерех* — самая спортивная рыба Дона! Поверхностный хищник, бьёт малька на мелководных перекатах. Лёгкий спиннинг 5–18 г, дальний заброс 50–80 м. Приманки: поппер, стик-бейт, блесна-кастмастер 14–20 г. Ищи «котёл» — всплески на поверхности — и бросай на 5 м выше по течению. Быстрая проводка! Гниловская переправа и перекат у Аксая — его рай. Клёв: рассвет 5:00–8:00 строго."

    # ── КАРАСЬ ──
    if any(w in l for w in ["карась","карасик"]):
        return "🐡 *Карась* — поплавочная 4–5 м, леска 0.12–0.14 мм, крючок №14–18, поплавок 2–4 г. Насадка: мотыль, опарыш, хлеб, консервированная кукуруза, навозный червь. Прикормка: пшённая каша + панировочные сухари. Тихая вода с илом — заводи, старицы, заросшие берега. Камышовая заводь (Батайск) — лучшее место. Клёв: рассвет 6:00–9:00 и вечер 17:00–19:00. Очень осторожная рыба — тишина!"

    # ── НАЛИМ ──
    if "налим" in l:
        return "❄️ *Налим* — зимняя и холодная рыба, активна при температуре воды 1–8°C (ноябрь–март). Ночная донная рыбалка. Снасть: донка или зимняя удочка с крупным крючком №1–2/0. Насадка: пучок червей, печень, мелкая рыбка. Глубокие ямы с холодным течением. В тёплой воде не берёт совсем."

    # ── ЧЕХОНЬ / ТАРАНЬ ──
    if any(w in l for w in ["чехонь","тарань","таранка","плотва"]):
        return "🐟 *Чехонь/Тарань* — быстрая поверхностная рыба. Лёгкий спиннинг или матч-удочка, тонкая леска 0.12 мм. Мелкий твистер, нимфа, мушка. Или поплавочная в толще воды, насадка опарыш. Перекаты с течением, весна и лето. Тарань особенно активна в апреле–мае у устья Дона в р-не Азова."

    # ── ОКУНЬ ──
    if any(w in l for w in ["окунь","полосатый","матросик"]):
        return "🎯 *Окунь* — стайная рыба, ищи котёл. Ультралёгкий спиннинг (UL), приманки 2–5 см: микротвистер, мелкий воблер, блесна-вертушка №0–2. Леска 0.10–0.12 мм. Заросли травы, коряжник на мелководье 1–3 м. Активен утром и вечером, особенно в пасмурную погоду."

    # ── БЕЛЫЙ АМУР ──
    if any(w in l for w in ["амур","белый амур"]):
        return "🌿 *Белый амур* — растительноядная рыба, самая пугливая на Дону. Карповая снасть, насадка: пучок молодой травы, варёная кукуруза, хлебный мякиш, ряска. Тишина критична — от малейшего шума уходит. Мелководье 1.5–3 м у камышей. Вторая дамба и Манычское вдхр. — его территория. Клёв: жаркий полдень когда вода прогрелась выше +22°C."

    # ── СЕЗОННЫЕ СОВЕТЫ ──
    if any(w in l for w in ["сейчас","сегодня","когда ловить","что ловить","какая рыба"]):
        tips = {
            "весна": "🌱 Весна на Дону — самое интересное время! Март: жерех, щука, судак просыпаются — жор перед нерестом. Апрель–май: НЕРЕСТОВЫЙ ЗАПРЕТ на большинство видов. Ловить можно: щуку, жереха (до запрета), карася. После снятия запрета (середина мая) — лещ, сазан клюют как сумасшедшие!",
            "лето": "☀️ Лето на Дону: июнь — сазан, карп по ночам; жерех утром на перекатах. Июль–август — сомовий жор ночью. Белый амур в жару у камышей в полдень. Лещ — только ночью в жару. Рассвет (5:00–7:00) — лучшее время для большинства рыб.",
            "осень": "🍂 Осень — время судака и щуки! Сентябрь–октябрь: судак нагуливает жир, активен весь день. Щука жирует перед холодами. Лещ собирается в стаи на ямах — фидер отлично работает. Октябрь–ноябрь: берш, налим начинает выходить.",
            "зима": "❄️ Зима на Дону: январь–март — налим (ночью), окунь на мормышку. Февраль–март — преднерестовый жор плотвы и леща. Лёд на Дону — редкость, но в морозные зимы бывает. Берш и судак активны зимой у ям.",
        }
        return tips.get(season, tips["лето"])

    # ── СНАСТИ ──
    if any(w in l for w in ["фидер","кормушк","прикормк"]):
        return "🎣 *Фидер на Дону*: удилище 3.6–4.2 м, тест 60–150 г (сильное течение!). Леска основная 0.22–0.25 мм, поводок 0.14–0.18 мм длиной 30–60 см. Кормушка 80–120 г (в суводи меньше). Прикормка: магазинная «река» + грунт + аттрактант. Крючок №8–12 под рыбу. Подставки обязательны — поклёвка резкая."
    if any(w in l for w in ["спиннинг","воблер","джиг","блесн","приманк"]):
        return "🎣 *Спиннинг на Дону*: для хищника (судак, щука, жерех) — удилище Fast строй 10–28 г, шнур 0.10–0.14 мм. Джиг: 15–28 г в зависимости от глубины и течения. Твистер 5–8 см для судака. Воблер-суспендер 7–12 см для щуки. Поппер и кастмастер для жереха на перекатах."

    # ── ЗАПРЕТЫ ──
    if any(w in l for w in ["запрет","нерест","штраф","инспектор","рыбнадзор","закон"]):
        return "⚠️ *Нерестовый запрет в Ростовской области*: обычно апрель–середина мая (точные даты ежегодно устанавливает Росрыболовство). Под запретом: ловля у нерестилищ, с моторных лодок, сетями. Разрешено: поплавочная и донная снасть с берега, 1 удилище с 2 крючками. Норма вылова: 5 кг на человека в сутки. За нарушение — штраф 2 000–5 000 руб + стоимость рыбы по таксе. Следи за актуальными постановлениями на сайте azov.fish.gov.ru"

    # ── ДАВЛЕНИЕ / ПОГОДА ──
    if any(w in l for w in ["давлени","погод","ветер","клёв","клев","будет ли","прогноз"]):
        return "🌤 Напиши /forecast — пришлю персональный прогноз клёва с реальной погодой и индексом клёва 1–10. Или поделись 📍 геолокацией — рассчитаю для твоего конкретного места."

    # ── МАГАЗИНЫ / СНАРЯЖЕНИЕ ──
    if any(w in l for w in ["магазин","купить","снасти","приобрести","где взять"]):
        return "🏪 Напиши /shops — покажу ближайшие к тебе рыболовные магазины с адресами и телефонами. В Ростове хорошие: «Рыболов» на Нариманова, «Охота и Рыбалка» на Большой Садовой, «Мир Рыбака» на Красноармейской."

    # ── МЕСТА / ТОЧКИ ──
    if any(w in l for w in ["куда","место","точк","где ловить","где рыбачить","маршрут"]):
        return "📍 Топ точки Дона и Ростовской области:\n🏆 *Вторая дамба (Аксай)* — лещ, сазан, фидер\n🎣 *Гниловская переправа* — судак, жерех, спиннинг\n🌿 *Мёртвый Донец (Ливенцовка)* — щука, карась\n🐠 *Перекат ниже Аксая* — сазан, жерех\n🏞 *Цимлянское вдхр.* — лещ, судак, сом\n🌊 *Устье Дона (Азов)* — тарань, бычок\nПодробнее: /spots"

    # ── УРОВЕНЬ ВОДЫ ──
    if any(w in l for w in ["уровень","паводок","разлив","половодье","вода поднялась"]):
        return "🌊 Уровень воды на Дону обновляется ежечасно прямо в приложении — открой раздел «Главная». Весной (апрель–май) Дон поднимается на 1–2 м выше нормы. При высокой воде лещ и сазан выходят на залитые поля — нестандартные места дают трофеи!"

    # ── ЛОДКА ──
    if any(w in l for w in ["лодк","мотор","с лодки","сплав"]):
        return "🚤 С лодки на Дону: судак и сом — основные трофеи. Дрейф с джигом по свалам — судак; квок ночью — сом. Лодочный мотор — минимум 5 л.с. Опасные места: ниже Аксая сильное течение у дамб. Разрешение на моторную лодку обязательно. В нерест хождение с мотором запрещено."

    # ── ДЕФОЛТНЫЕ ОТВЕТЫ ──
    defaults = [
        f"Спроси конкретнее! Могу рассказать про любую рыбу Дона: лещ, судак, сазан, щука, сом, жерех, карась, белый амур, чехонь, тарань, налим, окунь. Или напиши /forecast для прогноза клёва.",
        f"🎣 На Дону сейчас *{season}* — {{'весна':'жор перед нерестом!','лето':'сом ночью и жерех утром','осень':'судак и щука на пике','зима':'налим и берш у ям'}[season]}. Что интересует — рыба, место, снасть?",
        f"Отвечу точно и по делу! Напиши: название рыбы, 'куда поехать', 'какую снасть взять' или 'что сейчас ловится' — дам конкретный ответ по-егерски.",
    ]
    return random.choice(defaults)


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
        "Или поделись 📍 геолокацией — дам прогноз и магазины рядом!\n\n"
        "*Команды дневника:*\n"
        "🐟 /поймал — записать улов в дневник\n"
        "📖 /дневник — последние 5 записей\n"
        "🏆 /топ — топ-5 рыбаков недели\n"
        "🏅 /турнир — актуальные турниры",
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


# ── /поймал — личный дневник уловов ──
FISH_TYPES_BOT = [
    "судак","щука","сом","лещ","карп","карась","окунь","плотва","чехонь",
    "толстолобик","белый амур","голавль","жерех","сазан","красноперка",
    "берш","пиленгас","тарань","язь","линь","налим","форель","ёрш","другое",
]

async def catch_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    kb = [[t] for t in FISH_TYPES_BOT[:12]]
    kb.append(["другое"])
    await update.message.reply_text(
        "🐟 *Записываю улов!*\n\nШаг 1/4 — Какую рыбу поймал?",
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True, one_time_keyboard=True)
    )
    return CATCH_FISH


async def catch_fish(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["catch"] = {"fish": update.message.text.strip()}
    await update.message.reply_text(
        "⚖️ Шаг 2/4 — Вес улова в граммах:\n_(например: 1200 или 0 если не взвешивал)_",
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardRemove()
    )
    return CATCH_WEIGHT


async def catch_weight(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    try:
        grams = int(float(text.replace(",", ".")) * (1000 if "." in text or "," in text else 1))
        if grams > 200_000:
            grams = int(float(text))
    except ValueError:
        grams = 0
    ctx.user_data["catch"]["weight_g"] = grams
    await update.message.reply_text(
        "📍 Шаг 3/4 — Где ловил?\n_(название места или /skip)_",
        parse_mode="Markdown"
    )
    return CATCH_LOCATION


async def catch_location(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["catch"]["location"] = "" if update.message.text == "/skip" else update.message.text.strip()
    await update.message.reply_text(
        "📸 Шаг 4/4 — Пришли фото улова:\n_(или /skip)_",
        parse_mode="Markdown"
    )
    return CATCH_PHOTO


async def catch_photo(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    c = ctx.user_data.get("catch", {})
    photo_url = ""
    if update.message.text != "/skip" and update.message.photo:
        try:
            photo = update.message.photo[-1]
            file = await ctx.bot.get_file(photo.file_id)
            bucket = storage.bucket()
            blob = bucket.blob(f"catches/tg_{user.id}/{photo.file_id}.jpg")
            blob.upload_from_string(bytes(await file.download_as_bytearray()), content_type="image/jpeg")
            blob.make_public()
            photo_url = blob.public_url
        except Exception as e:
            logger.warning(f"catch photo upload: {e}")

    weight_g = c.get("weight_g", 0)
    weight_str = f"{weight_g/1000:.1f} кг" if weight_g else "не указан"
    record = {
        "fishName": c.get("fish", ""),
        "weightGrams": weight_g,
        "locationName": c.get("location", ""),
        "photoUrls": [photo_url] if photo_url else [],
        "userId": f"tg_{user.id}",
        "userName": user.full_name,
        "source": "telegram",
        "createdAt": firestore.SERVER_TIMESTAMP,
        "isPublic": False,
    }
    db.collection("catches").document(f"tg_{user.id}").collection("records").add(record)

    await update.message.reply_text(
        f"✅ *Улов записан в дневник!*\n\n"
        f"🐟 {c.get('fish','')}\n"
        f"⚖️ {weight_str}\n"
        f"📍 {c.get('location','—')}\n\n"
        f"Смотри дневник: /дневник\n"
        f"Или открой сайт 👉 https://turbenbaher-del.github.io/eger-ai/",
        parse_mode="Markdown"
    )
    ctx.user_data.clear()
    return ConversationHandler.END


async def catch_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.clear()
    await update.message.reply_text("❌ Запись отменена.", reply_markup=ReplyKeyboardRemove())
    return ConversationHandler.END


# ── /дневник ──
async def cmd_diary(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    uid = f"tg_{user.id}"
    try:
        snap = (db.collection("catches").document(uid).collection("records")
                .order_by("createdAt", direction=firestore.Query.DESCENDING).limit(5).get())
    except Exception:
        snap = []
    if not snap:
        await update.message.reply_text(
            "📖 *Твой дневник пуст*\n\nДобавь первый улов командой /поймал",
            parse_mode="Markdown"
        )
        return
    lines = ["📖 *Твои последние уловы:*\n"]
    for i, doc in enumerate(snap, 1):
        d = doc.to_dict()
        fish = d.get("fishName", "Рыба")
        wg = d.get("weightGrams", 0)
        w = f"{wg/1000:.1f} кг" if wg else ""
        loc = d.get("locationName", "")
        ts = d.get("createdAt")
        date_str = ts.strftime("%d.%m") if hasattr(ts, "strftime") else ""
        lines.append(f"{i}. *{fish}* {w} {('— '+loc) if loc else ''} {date_str}")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


# ── /топ ──
async def cmd_top(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    from datetime import timedelta
    since = datetime.now(pytz.utc) - timedelta(days=7)
    try:
        snap = (db.collection("reports")
                .where("createdAt", ">=", since)
                .order_by("createdAt", direction=firestore.Query.DESCENDING)
                .limit(200).get())
    except Exception:
        snap = []
    tally: dict = {}
    for doc in snap:
        d = doc.to_dict()
        uid = d.get("userId", d.get("uid", "anon"))
        name = d.get("author", d.get("displayName", "Рыбак"))
        kg = float(d.get("weight") or 0)
        if uid not in tally:
            tally[uid] = {"name": name, "catches": 0, "kg": 0.0}
        tally[uid]["catches"] += 1
        tally[uid]["kg"] += kg
    ranked = sorted(tally.values(), key=lambda x: x["kg"], reverse=True)[:5]
    if not ranked:
        await update.message.reply_text("🏆 Нет данных за эту неделю. Стань первым — /поймал")
        return
    medals = ["🥇","🥈","🥉","4️⃣","5️⃣"]
    lines = ["🏆 *Топ-5 рыбаков недели:*\n"]
    for i, r in enumerate(ranked):
        lines.append(f"{medals[i]} {r['name']} — {r['kg']:.1f} кг ({r['catches']} ул.)")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


# ── /турнир ──
async def cmd_tournament(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    now = datetime.now(pytz.utc)
    try:
        snap = db.collection("tournaments").order_by("startDate").get()
    except Exception:
        snap = []
    active, upcoming = [], []
    for doc in snap:
        d = {**doc.to_dict(), "id": doc.id}
        start = d.get("startDate")
        end = d.get("endDate")
        if start and end:
            s = start if hasattr(start, "replace") else start.replace(tzinfo=pytz.utc)
            e = end if hasattr(end, "replace") else end.replace(tzinfo=pytz.utc)
            try:
                if s.replace(tzinfo=pytz.utc) <= now <= e.replace(tzinfo=pytz.utc):
                    active.append(d)
                elif s.replace(tzinfo=pytz.utc) > now:
                    upcoming.append(d)
            except Exception:
                pass
    if not active and not upcoming:
        await update.message.reply_text(
            "🏅 *Активных турниров нет*\n\nСледите за анонсами в новостях приложения!",
            parse_mode="Markdown"
        )
        return
    lines = []
    for t in active:
        lines.append(f"🟢 *{t.get('title','Турнир')}* — ИДЁТ\n📅 {t.get('endDate','')}\n{t.get('description','')}")
    for t in upcoming[:3]:
        lines.append(f"⏳ *{t.get('title','Турнир')}* — СКОРО\n📅 {t.get('startDate','')}")
    await update.message.reply_text(
        "🏅 *Турниры Егерь ИИ:*\n\n" + "\n\n".join(lines),
        parse_mode="Markdown"
    )


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


# ── Уровень воды ──
async def fetch_water_level() -> dict:
    url = "https://allrivers.info/gauge/don-rostov-na-donu"
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=12),
                             headers={"User-Agent":"Mozilla/5.0 (compatible; EgerBot/1.0)"}) as r:
                html = await r.text()
        # Ищем уровень: число перед "см" в контексте gauge-данных
        m = re.search(r'<[^>]*class="[^"]*value[^"]*"[^>]*>\s*(-?\d+)\s*</|(-?\d+)\s*см', html)
        if not m:
            m = re.search(r'"level"\s*:\s*(-?\d+)|(-?\d+)\s*<[^<]*см', html)
        level = int(m.group(1) or m.group(2)) if m else None
        trend_m = re.search(r'\(([+-]\d+)\)', html)
        trend = int(trend_m.group(1)) if trend_m else 0
        if level is not None:
            return {"level": level, "trend": trend, "ok": True,
                    "updated": datetime.now(MOSCOW_TZ).strftime("%H:%M %d.%m")}
    except Exception as e:
        logger.warning(f"Water level fetch error: {e}")
    month = datetime.now().month
    seasonal = {1:-20,2:-15,3:15,4:80,5:40,6:5,7:-10,8:-15,9:-5,10:0,11:-10,12:-20}
    return {"level": seasonal.get(month, 0), "trend": 0, "ok": False,
            "updated": datetime.now(MOSCOW_TZ).strftime("%H:%M %d.%m")}


def _save_water_level(data: dict):
    db.collection("water_levels").document("don-rostov").set(data)


async def monitor_water_level(app: Application):
    loop = asyncio.get_event_loop()
    while True:
        data = await fetch_water_level()
        await loop.run_in_executor(None, _save_water_level, data)
        logger.info(f"Water level updated: {data['level']} cm (ok={data['ok']})")
        await asyncio.sleep(3600)


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


# ── Прогноз выходных (пятница 17:00 МСК) ──
async def send_weekend_forecast(ctx: ContextTypes.DEFAULT_TYPE):
    w = await fetch_weather(DEFAULT_LAT, DEFAULT_LON)
    users = db.collection("bot_users").where("subscribed", "==", True).stream()
    count = 0
    for u in users:
        if count >= 200:
            break
        data = u.to_dict()
        try:
            await ctx.bot.send_message(
                chat_id=data["chat_id"],
                text=(
                    "🎣 *Прогноз на выходные!*\n\n"
                    + build_forecast(w, "Ростов-на-Дону", DEFAULT_LAT, DEFAULT_LON)
                    + "\n\n📱 Открыть приложение: https://turbenbaher-del.github.io/eger-ai/"
                ),
                parse_mode="Markdown",
                disable_web_page_preview=True
            )
            count += 1
        except Exception as e:
            logger.warning(f"Weekend push failed {data.get('chat_id')}: {e}")


# ── Напоминание об улове (вторник 09:00 МСК) ──
async def send_catch_reminder(ctx: ContextTypes.DEFAULT_TYPE):
    from datetime import timedelta
    cutoff = datetime.now(pytz.utc) - timedelta(days=5)
    users = db.collection("bot_users").where("subscribed", "==", True).stream()
    count = 0
    for u in users:
        if count >= 100:
            break
        data = u.to_dict()
        uid = f"tg_{data.get('chat_id','')}"
        try:
            snap = (db.collection("catches").document(uid).collection("records")
                    .order_by("createdAt", direction=firestore.Query.DESCENDING).limit(1).get())
            has_recent = any(
                (d.to_dict().get("createdAt") or pytz.utc.localize(datetime(2000,1,1))) >= cutoff
                for d in snap
            )
            if not has_recent:
                await ctx.bot.send_message(
                    chat_id=data["chat_id"],
                    text="🎣 Давно не рыбачил? Погода сегодня отличная! Добавь улов командой /поймал",
                    parse_mode="Markdown"
                )
                count += 1
        except Exception as e:
            logger.warning(f"Catch reminder failed {data.get('chat_id')}: {e}")


# ── Недельный дайджест (понедельник 08:00 МСК) ──
async def send_weekly_digest(ctx: ContextTypes.DEFAULT_TYPE):
    from datetime import timedelta
    since = datetime.now(pytz.utc) - timedelta(days=7)
    try:
        snap = (db.collection("reports")
                .where("createdAt", ">=", since)
                .order_by("createdAt", direction=firestore.Query.DESCENDING)
                .limit(100).get())
    except Exception:
        snap = []

    count_reports = len(list(snap))
    w = await fetch_weather(DEFAULT_LAT, DEFAULT_LON)
    season_tip = build_forecast(w, "Ростов-на-Дону", DEFAULT_LAT, DEFAULT_LON)

    users = db.collection("bot_users").where("subscribed", "==", True).stream()
    sent = 0
    for u in users:
        if sent >= 200:
            break
        data = u.to_dict()
        try:
            await ctx.bot.send_message(
                chat_id=data["chat_id"],
                text=(
                    f"📊 *Дайджест недели Егерь ИИ*\n\n"
                    f"📝 Отчётов за неделю: *{count_reports}*\n\n"
                    f"{season_tip}\n\n"
                    f"📱 https://turbenbaher-del.github.io/eger-ai/"
                ),
                parse_mode="Markdown",
                disable_web_page_preview=True
            )
            sent += 1
        except Exception as e:
            logger.warning(f"Weekly digest failed {data.get('chat_id')}: {e}")


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
    app.add_handler(CommandHandler(["дневник", "diary"], cmd_diary))
    app.add_handler(CommandHandler(["топ", "top"], cmd_top))
    app.add_handler(CommandHandler(["турнир", "tournament"], cmd_tournament))

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

    catch_conv = ConversationHandler(
        entry_points=[CommandHandler(["поймал", "catch"], catch_start)],
        states={
            CATCH_FISH:     [MessageHandler(filters.TEXT & ~filters.COMMAND, catch_fish)],
            CATCH_WEIGHT:   [MessageHandler(filters.TEXT & ~filters.COMMAND, catch_weight)],
            CATCH_LOCATION: [MessageHandler(filters.TEXT, catch_location)],
            CATCH_PHOTO:    [MessageHandler(filters.PHOTO | filters.TEXT, catch_photo)],
        },
        fallbacks=[CommandHandler("cancel", catch_cancel)],
    )
    app.add_handler(catch_conv)

    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    app.job_queue.run_daily(
        send_morning_notifications,
        time=dtime(hour=3, minute=0, tzinfo=pytz.utc),
        name="morning_forecast"
    )
    # Пятница 14:00 UTC (17:00 МСК) — прогноз выходных
    app.job_queue.run_daily(
        send_weekend_forecast,
        time=dtime(hour=14, minute=0, tzinfo=pytz.utc),
        days=(4,),  # Friday
        name="weekend_forecast"
    )
    # Вторник 06:00 UTC (09:00 МСК) — напоминание если нет уловов 5+ дней
    app.job_queue.run_daily(
        send_catch_reminder,
        time=dtime(hour=6, minute=0, tzinfo=pytz.utc),
        days=(1,),  # Tuesday
        name="catch_reminder"
    )
    # Понедельник 05:00 UTC (08:00 МСК) — недельный дайджест
    app.job_queue.run_daily(
        send_weekly_digest,
        time=dtime(hour=5, minute=0, tzinfo=pytz.utc),
        days=(0,),  # Monday
        name="weekly_digest"
    )

    loop = asyncio.get_event_loop()
    loop.create_task(monitor_rss(app))
    loop.create_task(monitor_web_news(app))
    loop.create_task(monitor_water_level(app))

    logger.info("Егерь-бот запущен!")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
