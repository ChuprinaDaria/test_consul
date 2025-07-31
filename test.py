import os
from telethon import TelegramClient
from dotenv import load_dotenv
import asyncio

load_dotenv()

# Отримуємо змінні
api_id = int(os.getenv("API_ID"))
api_hash = os.getenv("API_HASH")
session = os.getenv("SESSION_NAME")
bot_token = os.getenv("BOT_TOKEN")
channel_id_raw = os.getenv("BOT_USERNAME")

print("🔍 ДІАГНОСТИКА КАНАЛУ")
print("=" * 40)
print(f"Сирий channel_id з .env: '{channel_id_raw}' (тип: {type(channel_id_raw)})")

# Перетворюємо правильно
try:
    if channel_id_raw.startswith('-') or channel_id_raw.lstrip('-').isdigit():
        channel_id = int(channel_id_raw)
        print(f"✅ Перетворено в число: {channel_id} (тип: {type(channel_id)})")
    else:
        channel_id = channel_id_raw
        print(f"📝 Залишено як рядок: {channel_id}")
except Exception as e:
    print(f"❌ Помилка перетворення: {e}")
    exit(1)

async def test_channel_access():
    # Тестуємо доступ
    user_client = TelegramClient(session, api_id, api_hash)
    bot_client = TelegramClient('bot', api_id, api_hash)
    
    await user_client.start()
    await bot_client.start(bot_token=bot_token)
    
    print(f"\n🤖 Тестуємо доступ до каналу {channel_id}...")
    
    # Спочатку спробуємо USER клієнтом
    try:
        print("1️⃣ Перевірка USER клієнтом...")
        user_entity = await user_client.get_entity(channel_id)
        print(f"✅ USER знайшов: {getattr(user_entity, 'title', 'Невідомо')}")
        
        # Перевіряємо чи користувач в каналі
        try:
            participants = await user_client.get_participants(channel_id, limit=1)
            print(f"✅ USER має доступ до учасників каналу")
        except Exception as e:
            print(f"⚠️ USER не має повного доступу: {e}")
            
    except Exception as e:
        print(f"❌ USER не може знайти канал: {e}")
        print("💡 Можливо ви не приєдналися до каналу")
        return
    
    # Тепер спробуємо BOT клієнтом
    try:
        print("\n2️⃣ Перевірка BOT клієнтом...")
        bot_entity = await bot_client.get_entity(channel_id)
        print(f"✅ BOT знайшов: {getattr(bot_entity, 'title', 'Невідомо')}")
        
        # Тестове повідомлення
        try:
            test_msg = f"🧪 Тест доступу - {asyncio.get_event_loop().time()}"
            await bot_client.send_message(channel_id, test_msg)
            print("✅ BOT може надсилати повідомлення!")
        except Exception as e:
            print(f"❌ BOT не може надсилати повідомлення: {e}")
            print("💡 Додайте бота як адміністратора з правами публікації")
            
    except Exception as e:
        print(f"❌ BOT не може знайти канал: {e}")
        print("💡 Бот не доданий до каналу або не має прав")
    
    # Показуємо всі канали користувача
    print(f"\n3️⃣ Всі ваші канали:")
    print("-" * 30)
    async for dialog in user_client.iter_dialogs():
        if dialog.is_channel:
            print(f"📢 {dialog.name} (ID: {dialog.id})")
    
    await user_client.disconnect()
    await bot_client.disconnect()

if __name__ == "__main__":
    asyncio.run(test_channel_access())#