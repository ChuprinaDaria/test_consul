import re
from telethon.tl.custom import Button
import hashlib

def get_city_color(city):
    """Повертає колір кружечка для конкретного міста в Канаді"""
    city_colors = {
        'Едмонтоні': '🔴',
        'Оттаві': '🟠',     
        'Торонто': '🟡',
        'Канаді': '🟢'  # для Посольства
    }
    
    for city_name, color in city_colors.items():
        if city_name in city:
            return color
    return '🟢'

def parse_slot_message(text):
    """Спрощений парсер - тільки місто та часи"""
    if not text or "З'явились нові слоти!" not in text:
        return None, None, None
    
    # Консульство/посольство
    location_match = re.search(r'🔸 (Генеральне Консульство України в .+|Посольство України в .+)', text)
    service_match = re.search(r'🔸 Послуга: (.+)', text)
    
    # Всі дати та часи
    date_sections = re.findall(r'(\d{2}\.\d{2}\.\d{4}):\s*([0-9:\s]+)', text)
    
    if not (location_match and service_match and date_sections):
        return None, None, None

    location_full = location_match.group(1)
    city = (location_full.replace("Генеральне Консульство України в ", "")
                         .replace("Посольство України в ", "").strip())
    service = service_match.group(1).strip()
    
    # Обробляємо дати та часи
    total_slots = 0
    date_time_info = []
    all_dates = []
    
    for date, times_str in date_sections:
        times = re.findall(r'\d{2}:\d{2}', times_str)
        if times:
            total_slots += len(times)
            all_dates.append(date)
            # Форматуємо часи для кожної дати
            times_formatted = " ".join(times)
            date_time_info.append(f"**{date}**: {times_formatted}")
    
    if total_slots == 0:
        return None, None, None
    
    # Генеруємо хеш включаючи часи
    times_for_hash = ";".join([f"{date}:{times_str.strip()}" for date, times_str in date_sections])
    content_hash = hashlib.md5(f"{city}_{service}_{times_for_hash}".encode()).hexdigest()
    
    # Мінімальне повідомлення - тільки місто та часи
    city_color = get_city_color(city)
    times_inline = " ".join(date_time_info)
    
    msg = f"{city_color} **Є слоти в {city}!** 🕐 **Доступні часи:** {times_inline}"

    buttons = [Button.url("🔗 Записатися", "https://id.e-consul.gov.ua/")]
    
    return msg, buttons, content_hash

def parse_slots_gone_message(text: str):
    """Парсить повідомлення про зайняті слоти"""
    if not text or "❌ На жаль" not in text:
        return None, None, None

    # Шукаємо місце та час
    gone_match = re.search(
        r"❌\s*На жаль,\s*усі\s*слоти\s*у\s*(.+?)\s*вже\s*зайняті!\s*Слоти\s*були\s*доступні\s*протягом\s*(\d+)\s*(хвилин|секунд)",
        text, re.IGNORECASE | re.DOTALL
    )
    
    if not gone_match:
        return None, None, None

    full_place = gone_match.group(1).strip()
    time_count = int(gone_match.group(2))
    time_unit = gone_match.group(3)
    
    # Отримуємо назву міста
    city = (full_place.replace("Генеральне Консульство України в ", "")
                     .replace("Посольство України в ", "").strip())
    
    # Конвертуємо в хвилини якщо потрібно
    if time_unit == "секунд":
        if time_count < 60:
            time_display = f"{time_count} секунд"
        else:
            minutes = time_count // 60
            seconds = time_count % 60
            time_display = f"{minutes} хв {seconds} сек" if seconds > 0 else f"{minutes} хвилин"
    else:
        time_display = f"{time_count} хвилин"
    
    return full_place, city, time_display

def test_parser():
    """Функція для тестування парсера"""
    test_messages = [
        # Твій приклад з Посольством України в Канаді
        """🆕 З'явились нові слоти!
🔸 Посольство України в Канаді
🔸 Послуга: Оформлення закордонного паспорта
📅 Слоти які були опубліковані:
14.08.2025: 09:00 09:10
👀 Будь ласка, стежте за нашими новими функціями.
Скоро ми вас приголомшимо!
🔥 Ви отримали це повідомлення без затримок!
Дякуємо за оформлення преміум підписки!""",
        
        # Приклад з Едмонтоном
        """🆕 З'явились нові слоти!
🔸 Генеральне Консульство України в Едмонтоні
🔸 Послуга: Оформлення закордонного паспорта
📅 Слоти які були опубліковані:
16.08.2025: 14:00 14:10 14:20
🔥 Ви отримали це повідомлення без затримок!""",
        
        # Приклад з кількома датами
        """🆕 З'явились нові слоти!
🔸 Генеральне Консульство України в Торонто
🔸 Послуга: Оформлення закордонного паспорта
📅 Слоти які були опубліковані:
17.08.2025: 11:15 11:25
18.08.2025: 09:30 10:00 10:30
🔥 Ви отримали це повідомлення без затримок!""",
        
        # Приклад повідомлення про зайнятість
        """❌ На жаль, усі слоти у Посольство України в Канаді вже зайняті!
Слоти були доступні протягом 59 секунд.
🔥 Тільки преміум користувачі отримують такі повідомлення. Дякуємо за оформлення преміум підписки!"""
    ]

    print("🧪 ТЕСТУВАННЯ ПАРСЕРА")
    print("=" * 60)
    
    for i, test_message in enumerate(test_messages[:3], 1):  # Тестуємо перші 3 (слоти)
        print(f"\n🧪 ТЕСТ {i} (слоти):")
        print("-" * 40)
        result, buttons, content_hash = parse_slot_message(test_message)
        if result:
            print("✅ УСПІШНО:")
            print(result)
            print(f"Хеш: {content_hash[:10]}...")
            print(f"Кнопки: {[btn.text for btn in buttons]}")
        else:
            print("❌ НЕ РОЗПІЗНАНО")
        print()
    
    # Тестуємо "зайнято"
    print(f"\n🧪 ТЕСТ 4 (зайнято):")
    print("-" * 40)
    gone_result = parse_slots_gone_message(test_messages[3])
    if gone_result[0]:
        full_place, city, time_display = gone_result
        print("✅ УСПІШНО:")
        print(f"Повне місце: {full_place}")
        print(f"Місто: {city}")
        print(f"Час доступності: {time_display}")
    else:
        print("❌ НЕ РОЗПІЗНАНО")
    
    print("\n" + "=" * 60)
    print("🎯 ТЕСТУВАННЯ ЗАВЕРШЕНО")

if __name__ == "__main__":
    test_parser()