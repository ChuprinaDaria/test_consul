import os
import re
import asyncio
from datetime import datetime, timedelta
from collections import defaultdict
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
    mark_processed_with_stats,
    mark_gone_processed,
    get_statistics_data
)
from botstatisticshandler import BotStatisticsHandler

# === Константи / змінні оточення ===
load_dotenv()

api_id = int(os.getenv("API_ID"))
api_hash = os.getenv("API_HASH")
session = os.getenv("SESSION_NAME")
bot_token = os.getenv("BOT_TOKEN")
channel_id_raw = os.getenv("BOT_USERNAME")
source_user = os.getenv("SOURCE_USER")

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
    Якщо прийшло повідомлення "❌ На жаль..." — відправляємо тиху нотифікацію
    БЕЗ редагування попереднього повідомлення
    """
    full_place, city, time_display = parse_slots_gone_message(event.raw_text)
    if not city:
        return False  # це не "зайнято"-повідомлення

    # Формуємо чисте повідомлення БЕЗ преміум-приписки
    clean_text = f"❌ **На жаль, слотів у {full_place} більше немає!**\n\n⏱️ Слоти були доступні **{time_display}**"

    try:
        # Відправляємо ТИХО (silent=True)
        await bot_client.send_message(channel_id, clean_text, silent=True, parse_mode='markdown')
        print(f"🔕 Тиха нотифікація про зайнятість слотів у {city}: {time_display}")
    except Exception as e:
        print(f"❌ Не вдалося відправити тиху нотифікацію для {city}: {e}")

    # Позначаємо "зайнято"-повідомлення як оброблене
    try:
        mark_gone_processed("", event.id)
    except Exception:
        pass
    return True


def extract_slot_info(original_text, parsed_msg):
    """Витягує інфо про слоти для статистики."""
    city = service = None
    slots_count = 0
    available_dates = []

    # Місто з повідомлення
    if "слоти в " in parsed_msg:
        city_match = re.search(r'слоти в (.+?)!', parsed_msg)
        if city_match:
            city = city_match.group(1).strip()

    # Послуга з оригінального тексту
    service_match = re.search(r'🔸 Послуга: (.+)', original_text)
    if service_match:
        service = service_match.group(1).strip()

    # Кількість слотів та дати з часів у повідомленні
    times_section = re.search(r'🕐 \*\*Доступні часи:\*\* (.+)', parsed_msg)
    if times_section:
        times_text = times_section.group(1)
        # Рахуємо всі часи
        all_times = re.findall(r'\d{2}:\d{2}', times_text)
        slots_count = len(all_times)
        
        # Витягуємо дати
        dates = re.findall(r'\*\*(\d{2}\.\d{2}\.\d{4})\*\*:', times_text)
        available_dates = dates

    return city, service, slots_count, available_dates


def get_hourly_city_stats(days=30):
    """Отримує статистику по годинах та містах"""
    data = get_statistics_data(days)
    
    hour_counts = defaultdict(int)
    city_counts = defaultdict(int)
    
    for city, service, slots_count, canada_time_str, timestamp in data:
        if city:
            city_counts[city] += 1
            
        # Обробка часу
        try:
            if canada_time_str:
                if 'T' in canada_time_str:
                    ct = datetime.fromisoformat(canada_time_str.replace('Z', '+00:00'))
                else:
                    ct = datetime.strptime(canada_time_str, '%Y-%m-%d %H:%M:%S')
            else:
                utc_time = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
                utc_time = pytz.UTC.localize(utc_time)
                ct = utc_time.astimezone(CANADA_TZ)
            
            hour_counts[ct.hour] += 1
        except:
            continue
    
    # Топ-3 години та міста
    top_hours = sorted(hour_counts.items(), key=lambda x: x[1], reverse=True)[:3]
    top_cities = sorted(city_counts.items(), key=lambda x: x[1], reverse=True)[:3]
    
    return top_hours, top_cities

def generate_content_hash_improved(text, parsed_msg):
    """
    Поліпшена функція для генерації хешу контенту.
    Враховує тільки ключову інформацію: місто + дати + часи
    """
    import hashlib
    
    city = ""
    dates_times = ""
    
    # Витягуємо місто
    if "слоти в " in parsed_msg:
        city_match = re.search(r'слоти в (.+?)!', parsed_msg)
        if city_match:
            city = city_match.group(1).strip()
    
    # Витягуємо дати та часи з оригінального тексту
    date_sections = re.findall(r'(\d{2}\.\d{2}\.\d{4}):\s*([0-9:\s]+)', text)
    if date_sections:
        # Сортуємо дати та часи для консистентності
        sorted_dates = sorted(date_sections)
        dates_times = ";".join([f"{date}:{times.strip()}" for date, times in sorted_dates])
    
    # Генеруємо хеш
    content_for_hash = f"{city}_{dates_times}"
    return hashlib.md5(content_for_hash.encode()).hexdigest()

# --- Мікро-аналітика без лізти у внутрішні методи StatisticsModule ---
_announced_today = set()  # {(YYYY-MM-DD, hour)}

async def notify_upcoming_slots_task():
    """
    Раз на хвилину перевіряємо: якщо за 5 хвилин починається "топ-година",
    шлемо тихе повідомлення з найчастішими містами.
    """
    global _announced_today

    while True:
        try:
            now = datetime.now(CANADA_TZ)
            top_hours, top_cities = get_hourly_city_stats(days=30)

            # Якщо немає достатньо статистики — спимо
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
                        cities_list = ", ".join(city for city, _ in top_cities[:2])  # Топ-2 міста
                        text = f"🔔 **За 5 хвилин можливі слоти в {cities_list}**\n\n📊 _(За статистикою минулого місяця)_"

                        try:
                            await bot_client.send_message(channel_id, text, silent=True, parse_mode='markdown')
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
    print("🔥 НОВЕ ПОВІДОМЛЕННЯ ОТРИМАНО!")
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

        # 0) Якщо це повідомлення про "слоти вже зайняті" — обробляємо його і завершуємо
        handled_gone = await handle_slots_gone(event)
        if handled_gone:
            return

        # 1) Антидубль по msg_id
        if is_processed(msg_id):
            print("⭕ ПРОПУЩЕНО: Повідомлення вже було оброблено раніше")
            return

        # 2) Парсимо "З'явились нові слоти!"
        print("📄 Парсинг повідомлення...")
        parsed_msg, buttons, content_hash = parse_slot_message(event.raw_text)

        if parsed_msg and buttons and content_hash:
            # Генеруємо кращий хеш
            improved_hash = generate_content_hash_improved(event.raw_text, parsed_msg)
            print(f"🔍 Поліпшений хеш: {improved_hash[:10]}...")
            
            # Антидубль за 60 хвилин (було 30)
            if is_content_processed_recently(improved_hash, 60):
                print("⭕ ПРОПУЩЕНО: Той самий контент за останню годину")
                mark_processed_with_stats(msg_id, improved_hash)
                return

            print("✅ УСПІШНО РОЗПАРСЕНО!")
            print("📄 Відформатоване повідомлення:")
            print("-" * 40)
            print(parsed_msg)
            print("-" * 40)

            print("📘 Кнопки:")
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

                # 6) Зберігаємо з новим хешем
                mark_processed_with_stats(
                    msg_id=msg_id,
                    content_hash=improved_hash,  # ← ЗМІНЕНО
                    city=city,
                    service=service,
                    slots_count=slots_count,
                    available_dates=available_dates
                )

                # 7) Зберігаємо message_id
                save_sent_message(improved_hash, sent.id)  # ← ЗМІНЕНО

                print(f"🎉 УСПІШНО ВІДПРАВЛЕНО в канал @{channel_id}!")
                print(f"📊 Додано до статистики: {city}, {service}, {slots_count} слотів")

            except Exception as send_error:
                print(f"❌ ПОМИЛКА при відправці: {send_error}")

        else:
            print("⚠️ НЕ РОЗПІЗНАНО: Повідомлення не містить інформацію про слоти або має неправильний формат")
            print("💡 Очікувані ключові слова: \"З'явились нові слоти!\"")

        # 8) Наостанок — відмічуємо msg_id, щоб повторно не обробляти
        try:
            mark_processed_with_stats(msg_id, None)
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
        # Підключання
        print("📄 Підключення до Telegram...")
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
            print("💡 Переконайтесь що бот доданий до каналу як адміністратор!")
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
        print(f"❌ Помилка запуску: {e}")
        import traceback
        traceback.print_exc()