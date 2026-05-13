import os
import json
import logging
import asyncio
import feedparser
from datetime import datetime, time as dtime
import pytz

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    filters, ContextTypes, ConversationHandler, CallbackQueryHandler
)
import firebase_admin
from firebase_admin import credentials, firestore, storage

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ── Firebase init ──
_svc = os.environ.get("FIREBASE_SERVICE_ACCOUNT", "")
cred = credentials.Certificate(json.loads(_svc))
firebase_admin.initialize_app(cred, {"storageBucket": "eger-ai.firebasestorage.app"})
db = firestore.client()

# ── Config ──
TOKEN = os.environ["BOT_TOKEN"]
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))
MOSCOW_TZ = pytz.timezone("Europe/Moscow")

# Ключевые слова для автофильтра сообщений из чатов
FISH_KEYWORDS = [
    "поймал", "поймала", "улов", "клюёт", "клюет", "клёв", "клев",
    "щука", "судак", "лещ", "карась", "сазан", "амур", "окунь",
    "карп", "толстолоб", "берш", "жерех", "налим", "сом",
    "рыбалка", "рыбачил", "спиннинг", "фидер", "поплавок",
    "дон", "аксай", "донец", "цимла", "манычское",
    "кг", "кило", "граммов", "штук", "хвостов",
]

# RSS-каналы для мониторинга
RSS_CHANNELS = [
    "rybolov_don",
    "fishing_rostov61",
    "don_rybalka",
]

# ── AI ответы ──
RESPONSES = [
    "Слушай, я тебе скажу — сейчас Дон подымается, амур жмётся к камышовым крепям. Бери кукурузу варёную, иди на вторую дамбу к семи утра, и будешь с рыбой. Давление выровнялось — самое оно.",
    "На Аксае сейчас вода мутновата после дождей. Лещ стоит на 4–5 метрах у старого русла. Опарыш на крючке №8, прикормка с анисом — не промахнёшься. Лучший клёв часа три до заката.",
    "Щуку ищи у затопленных кустов на Мёртвом Донце. Воблер 7–9 см серебристый — она их любит. Температура воды +17, жор начнётся часов в шесть вечера, поверь егерю.",
    "Ветер северо-западный — карась нынче капризный. Но у камышей Левобережья в тихих заводях берёт хорошо. Мотыль или красный червь. Леску 0.18 — он осторожный нынче.",
    "Давление стабильное, 762 мм. Это знак — сазан выходит кормиться! Перекаты ниже Аксая, бойлы кукурузные. Ранним утром, когда туман ещё лежит.",
    "Как вода поднимается на полметра, белый амур идёт к берегу. Сейчас именно такой момент. Молодые камышовые побеги — лучшей насадки не найдёшь.",
    "Судак стоит на свале — там где глубина резко с 3 до 8 метров. Джиг 20–28г, твистер белый. Веди у самого дна, медленной ступенькой. Ночью он активнее всего.",
]
_ri = 0

def get_reply(text: str) -> str:
    global _ri
    l = text.lower()
    if "амур" in l or "камыш" in l: return RESPONSES[5]
    if "щук" in l: return RESPONSES[2]
    if "лещ" in l or "опарыш" in l: return RESPONSES[1]
    if "карась" in l: return RESPONSES[3]
    if "сазан" in l or "бойл" in l: return RESPONSES[4]
    if "судак" in l or "джиг" in l: return RESPONSES[6]
    r = RESPONSES[_ri % len(RESPONSES)]; _ri += 1; return r

FORECAST_TEXT = (
    "🎣 *Прогноз клёва на сегодня*\n\n"
    "📍 Аксайский район · Дон\n"
    "🌡 Воздух: +19°C · Вода: +17°C\n"
    "💨 Ветер: СЗ 3 м/с\n"
    "📊 Давление: 763 мм ↑ (стабильное)\n"
    "🌊 Уровень воды: +40 см выше нормы\n\n"
    "⭐ *Клёв: 8/10 — Отличный!*\n\n"
    "✅ Лещ — 4–5 м, старое русло, опарыш\n"
    "✅ Белый амур — камышовые крепи, кукуруза\n"
    "✅ Сазан — перекаты ниже Аксая, рассвет\n"
    "⚠️ Судак — запрет до 20 мая\n\n"
    "🕐 Лучшее время: 5:00–8:00 и 18:00–21:00"
)

# ── Состояния разговора для отчёта ──
REPORT_TITLE, REPORT_LOCATION, REPORT_FISH, REPORT_PHOTO, REPORT_TEXT = range(5)


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
        [InlineKeyboardButton("🌐 Открыть сайт", url="https://turbenbaher-del.github.io/eger-ai/")],
    ])
    await update.message.reply_text(
        f"Здорово, {user.first_name}! 🎣\n\n"
        "Я — *Егерь ИИ*, знаю каждый омут Дона и Ростовской области.\n\n"
        "Просто напиши мне вопрос о рыбалке — отвечу.\n"
        "Или выбери действие ниже:",
        parse_mode="Markdown",
        reply_markup=kb
    )


# ── /forecast ──
async def forecast(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.message.reply_text(FORECAST_TEXT, parse_mode="Markdown")
    else:
        await update.message.reply_text(FORECAST_TEXT, parse_mode="Markdown")


# ── Отчёт: старт ──
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
    report["source"] = "telegram"

    db.collection("reports").add(report)

    await update.message.reply_text(
        "✅ *Отчёт опубликован на сайте!*\n\n"
        f"🏆 Заголовок: {report.get('title','')}\n"
        f"📍 Место: {report.get('location','—')}\n"
        f"🐟 Улов: {report.get('fish','—')}\n\n"
        "Смотри в разделе «Отчёты» на сайте 👉 https://turbenbaher-del.github.io/eger-ai/",
        parse_mode="Markdown"
    )
    ctx.user_data.clear()
    return ConversationHandler.END


async def report_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.clear()
    await update.message.reply_text("❌ Отчёт отменён.")
    return ConversationHandler.END


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


# ── Обработка текста ──
async def handle_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    reply = get_reply(update.message.text)
    await update.message.reply_text(f"🎣 {reply}")


# ── Кнопки ──
async def handle_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    data = update.callback_query.data
    if data == "forecast":
        await forecast(update, ctx)
    elif data == "subscribe":
        await subscribe_toggle(update, ctx)


# ── Мониторинг чатов — фильтр сообщений ──
def is_fishing_report(text: str) -> bool:
    if not text or len(text) < 30:
        return False
    t = text.lower()
    matches = sum(1 for kw in FISH_KEYWORDS if kw in t)
    return matches >= 2

async def handle_group_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.message or update.channel_post
    if not msg or not msg.text:
        return

    chat = msg.chat
    text = msg.text

    # Проверяем что чат в списке мониторинга
    monitored = db.collection("monitored_chats").document(str(chat.id)).get()
    if not monitored.exists:
        return

    if not is_fishing_report(text):
        return

    # Сохраняем как отчёт на сайт
    sender = msg.from_user
    name = sender.full_name if sender else chat.title or "Чат"

    db.collection("reports").add({
        "title": text[:80].split("\n")[0] or "Отчёт из чата",
        "body": text[:1000],
        "displayName": f"{name} (из {chat.title or 'чата'})",
        "uid": f"tg_chat_{chat.id}",
        "source": "telegram_chat",
        "chat_title": chat.title or "",
        "timestamp": firestore.SERVER_TIMESTAMP,
    })
    logger.info(f"Saved report from chat {chat.title}: {text[:50]}")


# ── Команды управления чатами (только для админа) ──
async def cmd_addchat(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat

    if chat.type == "private":
        await update.message.reply_text("❌ Команду нужно использовать в групповом чате")
        return

    db.collection("monitored_chats").document(str(chat.id)).set({
        "title": chat.title,
        "username": chat.username or "",
        "added_by": user.id,
        "timestamp": firestore.SERVER_TIMESTAMP,
    })
    await update.message.reply_text(
        f"✅ Чат *{chat.title}* добавлен в мониторинг!\n"
        "Теперь я буду автоматически публиковать отчёты об уловах на сайте.",
        parse_mode="Markdown"
    )
    logger.info(f"Added chat to monitoring: {chat.title} ({chat.id})")


async def cmd_removechat(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    db.collection("monitored_chats").document(str(chat.id)).delete()
    await update.message.reply_text(f"❌ Чат *{chat.title}* удалён из мониторинга.", parse_mode="Markdown")


async def cmd_listchats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID and ADMIN_ID != 0:
        return
    chats = db.collection("monitored_chats").stream()
    lines = [f"• {c.to_dict().get('title','?')}" for c in chats]
    text = "📋 *Мониторируемые чаты:*\n" + ("\n".join(lines) if lines else "_(пусто)_")
    await update.message.reply_text(text, parse_mode="Markdown")


# ── Мониторинг RSS-каналов ──
async def monitor_rss(app: Application):
    seen_ids = set()
    while True:
        for channel in RSS_CHANNELS:
            try:
                url = f"https://rsshub.app/telegram/channel/{channel}"
                feed = feedparser.parse(url)
                for entry in feed.entries[:5]:
                    entry_id = entry.get("id", entry.get("link", ""))
                    if entry_id in seen_ids:
                        continue
                    seen_ids.add(entry_id)
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
                    logger.info(f"RSS: saved from @{channel}")
            except Exception as e:
                logger.warning(f"RSS {channel}: {e}")
        await asyncio.sleep(3600)


# ── Утренние уведомления (06:00 МСК) ──
async def send_morning_notifications(ctx: ContextTypes.DEFAULT_TYPE):
    users = db.collection("bot_users").where("subscribed", "==", True).stream()
    for u in users:
        data = u.to_dict()
        try:
            await ctx.bot.send_message(
                chat_id=data["chat_id"],
                text=f"🌅 *Доброе утро, рыбак!*\n\n{FORECAST_TEXT}",
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.warning(f"Failed to notify {data.get('chat_id')}: {e}")


# ── Main ──
def main():
    app = Application.builder().token(TOKEN).build()

    # Команды
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("forecast", forecast))
    app.add_handler(CommandHandler("subscribe", subscribe_toggle))
    app.add_handler(CommandHandler("unsubscribe", subscribe_toggle))
    app.add_handler(CommandHandler("addchat", cmd_addchat))
    app.add_handler(CommandHandler("removechat", cmd_removechat))
    app.add_handler(CommandHandler("listchats", cmd_listchats))

    # Мониторинг сообщений из групп и каналов
    app.add_handler(MessageHandler(
        filters.TEXT & (filters.ChatType.GROUPS | filters.ChatType.CHANNEL),
        handle_group_message
    ))

    # Отчёт (диалог)
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

    # Кнопки и текст
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    # Утренние уведомления в 06:00 МСК
    app.job_queue.run_daily(
        send_morning_notifications,
        time=dtime(hour=3, minute=0, tzinfo=pytz.utc),  # 06:00 МСК = 03:00 UTC
        name="morning_forecast"
    )

    # RSS мониторинг в фоне
    loop = asyncio.get_event_loop()
    loop.create_task(monitor_rss(app))

    logger.info("Егерь-бот запущен!")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
