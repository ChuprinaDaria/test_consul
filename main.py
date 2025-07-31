import os
from telethon import TelegramClient, events
from telethon.tl.custom import Button
from dotenv import load_dotenv
from parse_like_whore import parse_slot_message
from db import init_db, is_processed, mark_processed, is_content_processed
import asyncio

load_dotenv()

# Отримуємо змінні оточення
api_id = int(os.getenv("API_ID"))
api_hash = os.getenv("API_HASH")
session = os.getenv("SESSION_NAME")
bot_token = os.getenv("BOT_TOKEN")
channel_id_raw = os.getenv("BOT_USERNAME")
source_user = os.getenv("SOURCE_USER")

# Правильно обробляємо channel_id
try:
    if channel_id_raw.startswith('-') or channel_id_raw.lstrip('-').isdigit():
        channel_id = int(channel_id_raw)
        print(f"📋 Використовую числовий ID каналу: {channel_id}")
    else:
        channel_id = channel_id_raw
        print(f"📝 Використовую username каналу: @{channel_id}")
except:
    channel_id = channel_id_raw
    print(f"📝 Використовую як рядок: {channel_id}")

# Створюємо клієнтів
user_client = TelegramClient(session, api_id, api_hash)
bot_client = TelegramClient('bot', api_id, api_hash)

init_db()

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

        # Перевіряємо чи вже оброблено
        if is_processed(msg_id):
            print("⏭️ ПРОПУЩЕНО: Повідомлення вже було оброблено раніше")
            return

        # Парсимо повідомлення
        print("🔄 Парсинг повідомлення...")
        parsed_msg, buttons, content_hash = parse_slot_message(event.raw_text)

        if parsed_msg and buttons and content_hash:
            # Перевіряємо чи не дублюється контент
            if is_content_processed(content_hash):
                print("⏭️ ПРОПУЩЕНО: Аналогічний контент вже був опублікований")
                return
            
            print("✅ УСПІШНО РОЗПАРСЕНО!")
            print("📄 Відформатоване повідомлення:")
            print("-" * 40)
            print(parsed_msg)
            print("-" * 40)
            
            print("🔘 Кнопки:")
            for btn in buttons:
                print(f"   • {btn.text} → {btn.url}")

            # Відправляємо в канал
            print("📤 Відправляю в канал...")
            try:
                await bot_client.send_message(
                    channel_id, 
                    parsed_msg, 
                    buttons=buttons, 
                    parse_mode='markdown'
                )
                
                # Позначаємо як оброблене з хешем контенту
                mark_processed(msg_id, content_hash)
                
                print(f"🎉 УСПІШНО ВІДПРАВЛЕНО в канал @{channel_id}!")
                
            except Exception as send_error:
                print(f"❌ ПОМИЛКА при відправці: {send_error}")
                
        else:
            print("⚠️ НЕ РОЗПІЗНАНО: Повідомлення не містить інформації про слоти або має неправильний формат")
            print("💡 Очікувані ключові слова: \"З'явились нові слоти!\"")
    
    except Exception as e:
        print(f"❌ КРИТИЧНА ПОМИЛКА при обробці: {e}")
        import traceback
        traceback.print_exc()
    
    print("="*60)

async def main():
    print("🚀 ЗАПУСК БОТА ДЛЯ ПЕРЕСИЛАННЯ СЛОТІВ")
    print("="*50)
    
    try:
        # Запускаємо клієнтів
        print("🔄 Підключення до Telegram...")
        await user_client.start()
        await bot_client.start(bot_token=bot_token)
        
        # Перевіряємо підключення
        me = await user_client.get_me()
        bot = await bot_client.get_me()
        
        print(f"✅ USER клієнт: {me.first_name} (ID: {me.id})")
        print(f"✅ BOT клієнт: {bot.first_name} (@{bot.username})")
        
        # Перевіряємо джерело
        try:
            source_entity = await user_client.get_entity(source_user)
            print(f"✅ Джерело: {source_entity.first_name} (@{source_entity.username})")
        except Exception as e:
            print(f"❌ Не вдалося знайти джерело {source_user}: {e}")
            return
        
        # Перевіряємо канал призначення
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
        print("📱 Для зупинки натисніть Ctrl+C")
        print("="*50)
        
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