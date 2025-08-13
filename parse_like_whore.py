import re
from telethon.tl.custom import Button
from datetime import datetime, timedelta
import hashlib

def get_city_color(city):
    """
    Повертає колір кружечка для конкретного міста в Канаді
    """
    city_colors = {
        'Едмонтоні': '🔴',  # синій
        'Оттаві': '🟠',     # помаранчевий  
        'Торонто': '🟡',    # фіолетовий
    }
    
    # Шукаємо місто в назві
    for city_name, color in city_colors.items():
        if city_name.lower() in city.lower():
            return color
    
    # За замовчуванням зелений для інших міст
    return '🟢'

def parse_times_and_calculate_services(times_str):
    """
    Парсить часи та розраховує доступні слоти для кожної послуги
    """
    # Знаходимо всі часи
    times = re.findall(r'\d{2}:\d{2}', times_str)
    if not times:
        return [], {}
    
    # Конвертуємо в datetime для розрахунків
    time_objects = []
    for time_str in times:
        try:
            time_obj = datetime.strptime(time_str, '%H:%M')
            time_objects.append((time_str, time_obj))
        except ValueError:
            continue
    
    # Сортуємо за часом
    time_objects.sort(key=lambda x: x[1])
    sorted_times = [t[0] for t in time_objects]
    
    # Розраховуємо слоти для різних послуг
    services = {
        'adult': sorted_times.copy(),  # Дорослі - всі слоти (10 хв)
        'teen': sorted_times.copy(),   # 12-16 років - всі слоти (10 хв)  
        'child': []                    # До 12 років - тільки ті, що мають 15+ хв різниці
    }
    
    # Для дитячих паспортів до 12 років (15 хв) - шукаємо слоти з достатнім проміжком
    if len(time_objects) > 0:
        # Перший слот завжди доступний
        services['child'].append(time_objects[0][0])
        
        # Перевіряємо інші слоти
        last_child_time = time_objects[0][1]
        
        for time_str, time_obj in time_objects[1:]:
            # Якщо між слотами >= 15 хвилин, можна записатися
            time_diff = (time_obj - last_child_time).total_seconds() / 60
            if time_diff >= 15:
                services['child'].append(time_str)
                last_child_time = time_obj
    
    return sorted_times, services

def generate_content_hash(location, service, date_sections):
    """
    Генерує хеш для контенту повідомлення щоб уникнути дублювання
    """
    content = f"{location}_{service}_{'_'.join([f'{date}:{times}' for date, times in date_sections])}"
    return hashlib.md5(content.encode()).hexdigest()

def parse_slot_message(text):
    """
    Парсить повідомлення про слоти від ConsulateUkraineBot
    Повертає відформатоване повідомлення, кнопки та хеш контенту
    """
    if not text or "З'явились нові слоти!" not in text:
        return None, None, None
    
    # Шукаємо консульство/посольство
    location = re.search(r'🔸 (Генеральне Консульство України в .+|Посольство України в .+)', text)
    
    # Шукаємо послугу
    service = re.search(r'🔸 Послуга: (.+)', text)
    
    # Шукаємо всі дати та часи (поліпшений regex)
    date_sections = re.findall(r'📅 Слоти які були опубліковані:\s*(\d{2}\.\d{2}\.\d{4}):(.*?)(?=📅|⚠️|🔥|$)', text, re.DOTALL)
    
    if not (location and service and date_sections):
        print("🔍 Debug: Не знайдено всі необхідні компоненти")
        print(f"Location: {bool(location)}")
        print(f"Service: {bool(service)}")
        print(f"Date sections: {len(date_sections)}")
        return None, None, None

    # Генеруємо хеш контенту для перевірки дублювання
    content_hash = generate_content_hash(location.group(1), service.group(1), date_sections)

    # Отримуємо назву міста
    city = location.group(1).replace("Генеральне Консульство України в ", "").replace("Посольство України в ", "").strip()
    
    # Отримуємо колір для міста
    city_color = get_city_color(city)
    
    # Отримуємо послугу
    poslyga = service.group(1).strip()
    
    # Обробляємо всі дати та часи
    date_info = []
    all_dates = []
    has_child_services = False
    child_services_info = []
    
    for date, times_text in date_sections:
        # Парсимо часи та розраховуємо послуги
        sorted_times, services = parse_times_and_calculate_services(times_text)
        
        if sorted_times:
            # Додаємо інформацію про дату
            date_info.append(f"📆 **{date}**: `{', '.join(sorted_times)}`")
            all_dates.append(date.replace('.', '_'))
            
            # Збираємо інформацію про дитячі послуги
            if services['teen'] or services['child']:
                has_child_services = True
                
                if services['teen']:
                    child_services_info.append(f"🔹 **Паспорт дитині 12-16 років** (10 хв):")
                    child_services_info.append(f"   • {date}: {', '.join(services['teen'])}")
                
                if services['child']:
                    child_services_info.append(f"🔹 **Паспорт дитині до 12 років** (15 хв):")
                    child_services_info.append(f"   • {date}: {', '.join(services['child'])}")
    
    if not date_info:
        print("🔍 Debug: Не знайдено жодних часів")
        return None, None, None
    
    # Створюємо основне повідомлення з кольоровим кружечком
    adult_times = []
    for date, times_text in date_sections:
        times = re.findall(r'\d{2}:\d{2}', times_text)
        if times:
            adult_times.append(f"{date}: {', '.join(times)}")
    
    msg = f"""{city_color} Доступні слоти в {city}!

📌 Послуга: {poslyga}

🔹 **Дорослі (10хв):** {'; '.join(adult_times)}"""

    # Додаємо дитячі послуги якщо є (з відступом)
    if has_child_services:
        # Збираємо дитячі слоти
        teen_slots = []
        child_slots = []
        
        for date, times_text in date_sections:
            sorted_times, services = parse_times_and_calculate_services(times_text)
            if services['teen']:
                teen_slots.append(f"{date}: {', '.join(services['teen'])}")
            if services['child']:
                child_slots.append(f"{date}: {', '.join(services['child'])}")
        
        # Додаємо порожні рядки для відступу
        msg += "\n\n"
        
        if teen_slots:
            msg += f"Записи дітям 12-16 років (10хв): {'; '.join(teen_slots)}\n"
        if child_slots:
            msg += f"До 12 років (15хв): {'; '.join(child_slots)}"

    # Кнопка для переходу на сайт
    buttons = [Button.url("🔗 Записатися на слот", "https://id.e-consul.gov.ua/")]
    
    return msg, buttons, content_hash


def parse_slots_gone_message(text: str):
    """
    Парсить повідомлення формату:
    ❌ На жаль, усі слоти у {повна_назва_місця} вже зайняті!
    Слоти були доступні протягом {N} хвилин.
    (хвіст про преміум ігноруємо)

    Повертає: (full_place, city, minutes_alive) або (None, None, None)
    """
    if not text:
        return None, None, None

    # Дістаємо повну назву місця і кількість хвилин
    m = re.search(
        r"❌\s*На жаль,\s*усі\s*слоти\s*у\s*(.+?)\s*вже\s*зайняті!\s*Слоти\s*були\s*доступні\s*протягом\s*(\d+)\s*хвилин",
        text, re.IGNORECASE | re.DOTALL
    )
    if not m:
        return None, None, None

    full_place = m.group(1).strip()
    minutes_alive = m.group(2).strip()

    # Вирізаємо чисту назву міста з повної назви установи
    city = re.sub(
        r"^(Генеральне\s+Консульство\s+України\s+в|Посольство\s+України\s+в)\s+",
        "",
        full_place
    ).strip()

    return full_place, city, minutes_alive


def test_parser():
    """Функція для тестування парсера"""
    test_messages = [
        """🆕 З'явились нові слоти!
🔸 Посольство України в Канаді
🔸 Послуга: Оформлення закордонного паспорта
📅 Слоти які були опубліковані:
15.01.2026: 10:30
🔥 Ви отримали це повідомлення без затримок!""",
        
        """🆕 З'явились нові слоти!
🔸 Генеральне Консульство України в Едмонтоні
🔸 Послуга: Оформлення закордонного паспорта
📅 Слоти які були опубліковані:
16.01.2026: 14:00
🔥 Ви отримали це повідомлення без затримок!""",
        
        """🆕 З'явились нові слоти!
🔸 Генеральне Консульство України в Торонто
🔸 Послуга: Оформлення закордонного паспорта
📅 Слоти які були опубліковані:
17.01.2026: 11:15
🔥 Ви отримали це повідомлення без затримок!""",
        
        """🆕 З'явились нові слоти!
🔸 Генеральне Консульство України в Оттаві
🔸 Послуга: Оформлення закордонного паспорта
📅 Слоти які були опубліковані:
18.01.2026: 09:30
🔥 Ви отримали це повідомлення без затримок!"""
    ]

    for i, test_message in enumerate(test_messages, 1):
        print(f"🧪 ТЕСТ {i}:")
        print("=" * 60)
        result, buttons, content_hash = parse_slot_message(test_message)
        if result:
            print(result)
            print(f"\nContent hash: {content_hash}")
        else:
            print("❌ Тест неуспішний")
        print()

if __name__ == "__main__":
    test_parser()