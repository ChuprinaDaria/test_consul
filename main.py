import os
import re
import asyncio
import sqlite3
from datetime import datetime, timedelta

import pytz
from telethon import TelegramClient, events
from telethon.tl.custom import Button
from dotenv import load_dotenv

from parse_like_whore import parse_slot_message, parse_slots_gone_message
from db import (
    init_db,
    is_processed,
    is_content_processed_recently,
    get_sent_message_id_by_city,
    save_sent_message,
    mark_processed,  # використовуємо, щоб відмічати "зайнято"-повідомлення
)
from botstatisticshandler import BotStatisticsHandler, mark_processed_with_stats

# === Константи / змінні оточення ===
load_dotenv()

api_id = int(os.getenv("API_ID"))
api_hash = os.getenv("API_HASH")
session = os.getenv("SESSION_NAME")
bot_token = os.getenv("BOT_TOKEN")
channel_id_raw = os.getenv("BOT_USERNAME")
source_user = os.getenv("SOURCE_USER")

# Однакове ім'я БД у всіх модулях
DB_FILE = "processed_messages.db"

# Часовий пояс Канади для статистики
CANADA_TZ = pytz.timezone("America/Toronto")

# Коректна обробка channel_id
try:
    if channel_id_raw.startswith('-') or channel_id_raw.lstrip('-').isdigit():
        channel_id = int(channel_id_raw)
        print(f"📋 Використовую числовий ID каналу: {channel_id}")
    else:
        channel_id = channel_id_raw
        print(f"📝 Використовую username каналу: @{channel_id}")
except Exception:
    channel_id = channel_id_raw
    print(f"📝 Використовую як рядок: {channel_id}")

# Клієнти
user_client = TelegramClient(session, api_id, api_hash)
bot_client = TelegramClient('bot', api_id, api_hash)
stats_handler = BotStatisticsHandler()

init_db()

# ============================================================
# ХЕЛПЕРИ
# ============================================================

async def handle_slots_gone(event):
    """
    Якщо прийшло повідомлення "❌ На жаль..." – знаходимо попередній пост у каналі за містом
    і редагуємо його коротким текстом, без преміум-хвоста.
    """
    full_place, city, minutes_alive = parse_slots_gone_message(event.raw_text)
    if not city:
        return False  # це не "зайнято"-повідомлення

    prev_id = get_sent_message_id_by_city(city)
    if not prev_id:
        print(f"⚠️ Не знайдено попереднього повідомлення для міста: {city}")
        # Все одно помічаємо як оброблене, щоб не зациклитись
        try:
            mark_processed(event.id, None)
        except Exception:
            pass
        return True

    new_text = (
        f"❌ На жаль, усі слоти у {full_place} вже зайняті!\n"
        f"Слоти були доступні протягом {minutes_alive} хвилин."
    )

    try:
        await bot_client.edit_message(channel_id, prev_id, new_text)
        print(f"✏️ Оновлено повідомлення для {city}")
    except Exception as e:
        # ← ця стрічка для швидкого пошуку проблем
        print(f"❌ Не вдалося оновити повідомлення для {city}: {e}")

    # Позначаємо "зайнято"-повідомлення як оброблене
    try:
        mark_processed(event.id, None)
    except Exception:
        pass
    return True


def extract_slot_info(text, parsed_msg):
    """Витягує базову інфу про слоти для статистики."""
    city = None
    service = None
    slots_count = 0
    available_dates = []

    # Місто
    location_match = re.search(r'🔸 (Генеральне Консульство України в .+|Посольство України в .+)', text)
    if location_match:
        city = (location_match.group(1)
                .replace("Генеральне Консульство України в ", "")
                .replace("Посольство України в ", "")
                .strip())

    # Послуга
    service_match = re.search(r'🔸 Послуга: (.+)', text)
    if service_match:
        service = service_match.group(1).strip()

    # Дати/часи
    date_sections = re.findall(
        r'📅 Слоти які були опубліковані:\s*(\d{2}\.\d{2}\.\d{4}):(.*?)(?=📅|⚠️|🔥|$)',
        text, re.DOTALL
    )

    for date, times_text in date_sections:
        times = re.findall(r'\d{2}:\d{2}', times_text)
        if times:
            slots_count += len(times)
            available_dates.append(date)

    return city, service, slots_count, available_dates


# --- Мікро-аналітика без лізти у внутрішні методи StatisticsModule ---
def compute_top_hours_and_cities(days: int = 30, top_n: int = 3):
    """
    Повертає (top_hours, top_cities) за останні `days` днів.
    top_hours: список (hour, count)
    top_cities: список (city, count)
    """
    since_utc = datetime.now(pytz.UTC) - timedelta(days=days)

    hour_counts = {}
    city_counts = {}

    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        # Беремо canada_time якщо є, інакше конвертимо timestamp (UTC) → Canada TZ
        cursor.execute("""
            SELECT city, canada_time, timestamp
            FROM processed
            WHERE city IS NOT NULL
              AND (timestamp >= ?)
        """, (since_utc.strftime('%Y-%m-%d %H:%M:%S'),))

        rows = cursor.fetchall()

    for city, canada_time_str, timestamp_str in rows:
        if not city:
            continue

        # Місто
        city_counts[city] = city_counts.get(city, 0) + 1

        # Час (година в Канаді)
        try:
            if canada_time_str:
                # ISO або "%Y-%m-%d %H:%M:%S"
                if 'T' in canada_time_str:
                    ct = datetime.fromisoformat(canada_time_str.replace('Z', '+00:00'))
                else:
                    ct = datetime.strptime(canada_time_str, '%Y-%m-%d %H:%M:%S')
            else:
                utc_time = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                utc_time = pytz.UTC.localize(utc_time)
                ct = utc_time.astimezone(CANADA_TZ)
            hour = ct.hour
            hour_counts[hour] = hour_counts.get(hour, 0) + 1
        except Exception:
            continue

    top_hours = sorted(hour_counts.items(), key=lambda x: x[1], reverse=True)[:top_n]
    top_cities = sorted(city_counts.items(), key=lambda x: x[1], reverse=True)[:top_n]
    return top_hours, top_cities


_announced_today = set()  # {(YYYY-MM-DD, hour)}

async def notify_upcoming_slots_task():
    """
    Раз на хвилину перевіряємо: якщо за 5 хвилин починається “топ-година”,
    шлемо тихе повідомлення з найчастішими містами.
    """
    global _announced_today

    while True:
        try:
            now = datetime.now(CANADA_TZ)
            top_hours, top_cities = compute_top_hours_and_cities(days=30, top_n=3)

            # Якщо нема достатньо статистики – спимо
            if not top_hours or not top_cities:
                await asyncio.sleep(60)
                continue

            # Наступна година, якщо зараз наприкінці години (55-59 хв) — попередження за 5 хв
            # або, загальніше: якщо now.minute між 55..59 і наступна година у топ-годинах
            if 55 <= now.minute <= 59:
                next_hour = (now.hour + 1) % 24
                if any(h == next_hour for (h, c) in top_hours):
                    key = (now.strftime('%Y-%m-%d'), next_hour)
                    if key not in _announced_today:
                        # Формуємо список міст (через кому)
                        cities_list = ", ".join(city for city, _ in top_cities)
                        text = f"🔔 Є за 5 хв можливі слоти в {cities_list} (за статистикою минулого місяця)."

                        try:
                            await bot_client.send_message(channel_id, text, silent=True)
                            print(f"🔕 Тихе попередження на {next_hour:02d}:00 — {cities_list}")
                        except Exception as e:
                            print(f"⚠️ Не вдалося надіслати тихе попередження: {e}")

                        _announced_today.add(key)

            # Скидаємо маркери на новий день
            if now.hour == 0 and now.minute == 0:
                _announced_today = set()

        except Exception as e:
            print(f"⚠️ Помилка в notify_upcoming_slots_task: {e}")
        finally:
            await asyncio.sleep(60)


# ============================================================
# ХЕНДЛЕР НОВИХ ПОВІДОМЛЕНЬ (від джерела)
# ============================================================

@user_client.on(events.NewMessage(from_users=source_user))
async def handler(event):
    print("\n" + "="*60)
    print("📥 НОВЕ ПОВІДОМЛЕННЯ ОТРИМАНО!")
    print("="*60)

    try:
        msg_id = event.id
        sender = await event.get_sender()
        sender_name = getattr(sender, 'username', 'Невідомо')

        print(f"📨 ID повідомлення: {msg_id}")
        print(f"👤 Відправник: @{sender_name}")
        print(f"📝 Текст повідомлення:")
        print("-" * 40)
        print(event.raw_text[:500] + ("..." if len(event.raw_text) > 500 else ""))
        print("-" * 40)

        # 0) Якщо це повідомлення про "слоти вже зайняті" – обробляємо його і завершуємо
        handled_gone = await handle_slots_gone(event)
        if handled_gone:
            return

        # 1) Антидубль по msg_id
        if is_processed(msg_id):
            print("⏭️ ПРОПУЩЕНО: Повідомлення вже було оброблено раніше")
            return

        # 2) Парсимо "З'явились нові слоти!"
        print("🔄 Парсинг повідомлення...")
        parsed_msg, buttons, content_hash = parse_slot_message(event.raw_text)

        if parsed_msg and buttons and content_hash:
            # 3) Антидубль по контенту (за 30 хв)
            if is_content_processed_recently(content_hash, 30):
                print("⏭️ ПРОПУЩЕНО: Аналогічний контент публікувався протягом останніх 30 хвилин")
                print("💡 Слот може з'явитися знову через 30+ хвилин якщо хтось відмовиться")
                # все одно помітимо як оброблене за msg_id, щоб не дьоргати по колу
                try:
                    mark_processed(msg_id, content_hash)
                except Exception:
                    pass
                return

            print("✅ УСПІШНО РОЗПАРСЕНО!")
            print("📄 Відформатоване повідомлення:")
            print("-" * 40)
            print(parsed_msg)
            print("-" * 40)

            print("🔘 Кнопки:")
            for btn in buttons:
                print(f"   • {btn.text} → {btn.url}")

            # 4) Відправляємо в канал
            print("📤 Відправляю в канал...")
            try:
                sent = await bot_client.send_message(
                    channel_id,
                    parsed_msg,
                    buttons=buttons,
                    parse_mode='markdown'
                )

                # 5) Дані для статистики
                city, service, slots_count, available_dates = extract_slot_info(event.raw_text, parsed_msg)

                # 6) Позначаємо як оброблене зі статистикою (вставка або оновлення)
                mark_processed_with_stats(
                    msg_id=msg_id,
                    content_hash=content_hash,
                    city=city,
                    service=service,
                    slots_count=slots_count,
                    available_dates=available_dates
                )

                # 7) Зберігаємо message_id для можливого редагування (”❌ На жаль…”)
                save_sent_message(content_hash, sent.id)

                print(f"🎉 УСПІШНО ВІДПРАВЛЕНО в канал @{channel_id}!")
                print(f"📊 Додано до статистики: {city}, {service}, {slots_count} слотів")

            except Exception as send_error:
                print(f"❌ ПОМИЛКА при відправці: {send_error}")

        else:
            print("⚠️ НЕ РОЗПІЗНАНО: Повідомлення не містить інформації про слоти або має неправильний формат")
            print("💡 Очікувані ключові слова: \"З'явились нові слоти!\"")

        # 8) Наостанок — відмітимо msg_id, щоб повторно не обробляти
        try:
            mark_processed(msg_id, None)
        except Exception:
            pass

    except Exception as e:
        print(f"❌ КРИТИЧНА ПОМИЛКА при обробці: {e}")
        import traceback
        traceback.print_exc()

    print("="*60)


# ============================================================
# БОТ-КОМАНДИ
# ============================================================

@bot_client.on(events.NewMessage(pattern='/start'))
async def start_handler(event):
    await stats_handler.handle_start_command(event)

@bot_client.on(events.CallbackQuery)
async def callback_handler(event):
    await stats_handler.handle_stats_callback(event)


# ============================================================
# ЗАПУСК
# ============================================================

async def main():
    print("🚀 ЗАПУСК БОТА ДЛЯ ПЕРЕСИЛАННЯ СЛОТІВ")
    print("="*50)

    try:
        # Під'єднання
        print("🔄 Підключення до Telegram...")
        await user_client.start()
        await bot_client.start(bot_token=bot_token)

        me = await user_client.get_me()
        bot = await bot_client.get_me()

        print(f"✅ USER клієнт: {me.first_name} (ID: {me.id})")
        print(f"✅ BOT клієнт: {bot.first_name} (@{bot.username})")

        # Джерело
        try:
            source_entity = await user_client.get_entity(source_user)
            print(f"✅ Джерело: {getattr(source_entity, 'first_name', 'N/A')} (@{getattr(source_entity, 'username', 'N/A')})")
        except Exception as e:
            print(f"❌ Не вдалося знайти джерело {source_user}: {e}")
            return

        # Канал
        try:
            channel_entity = await bot_client.get_entity(channel_id)
            channel_title = getattr(channel_entity, 'title', 'Невідомо')
            print(f"✅ Канал призначення: {channel_title} (@{channel_id})")
        except Exception as e:
            print(f"❌ Не вдалося знайти канал {channel_id}: {e}")
            print("💡 Переконайтеся що бот доданий до каналу як адміністратор!")
            return

        print("\n" + "="*50)
        print("🎯 ВСЕ ГОТОВО! Чекаю нові повідомлення про слоти...")
        print("💡 Бот буде автоматично пересилати ТІЛЬКИ НОВІ повідомлення")
        print("📊 Статистика збирається автоматично. Використайте /start для перегляду")
        print("🔕 Тихі попередження вмикнено (за 5 хв до найімовірнішої години)")
        print("📱 Для зупинки натисніть Ctrl+C")
        print("="*50)

        # Фонова задача з тихими попередженнями
        asyncio.create_task(notify_upcoming_slots_task())

        # Слухаємо нові повідомлення
        await user_client.run_until_disconnected()

    except Exception as e:
        print(f"❌ КРИТИЧНА ПОМИЛКА: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Бот зупинено користувачем")
    except Exception as e:
        print(f"❌ ПОМИЛКА запуску: {e}")
        import traceback
        traceback.print_exc()
