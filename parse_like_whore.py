import re
from telethon.tl.custom import Button
from datetime import datetime, timedelta

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

def parse_slot_message(text):
    """
    Парсить повідомлення про слоти від ConsulateUkraineBot
    Повертає відформатоване повідомлення та кнопки
    """
    if not text or "З'явились нові слоти!" not in text:
        return None, None
    
    # Шукаємо консульство/посольство
    location = re.search(r'🔸 (Генеральне Консульство України в .+|Посольство України в .+)', text)
    
    # Шукаємо послугу
    service = re.search(r'🔸 Послуга: (.+)', text)
    
    # Шукаємо всі дати та часи (поліпшений regex)
    date_sections = re.findall(r'📅 Слоти які були опубліковані:\s*(\d{2}\.\d{2}\.\d{4}):(.*?)(?=📅|⚠️|$)', text, re.DOTALL)
    
    if not (location and service and date_sections):
        print("🔍 Debug: Не знайдено всі необхідні компоненти")
        print(f"Location: {bool(location)}")
        print(f"Service: {bool(service)}")
        print(f"Date sections: {len(date_sections)}")
        return None, None

    # Отримуємо назву міста
    city = location.group(1).replace("Генеральне Консульство України в ", "").replace("Посольство України в ", "").strip()
    
    # Отримуємо послугу
    poslyga = service.group(1).strip()
    
    # Обробляємо всі дати та часи
    date_info = []
    all_dates = []
    services_summary = {
        'adult': [],
        'teen': [], 
        'child': []
    }
    
    for date, times_text in date_sections:
        # Парсимо часи та розраховуємо послуги
        sorted_times, services = parse_times_and_calculate_services(times_text)
        
        if sorted_times:
            # Додаємо інформацію про дату
            date_info.append(f"📆 <b>{date}</b>: <code>{', '.join(sorted_times)}</code>")
            all_dates.append(date.replace('.', '_'))
            
            # Зберігаємо послуги для підсумку
            if services['adult']:
                services_summary['adult'].append(f"{date}: {', '.join(services['adult'])}")
            if services['teen']:
                services_summary['teen'].append(f"{date}: {', '.join(services['teen'])}")
            if services['child']:
                services_summary['child'].append(f"{date}: {', '.join(services['child'])}")
    
    if not date_info:
        print("🔍 Debug: Не знайдено жодних часів")
        return None, None
    
    # Створюємо основне повідомлення
    msg = f"""<b>🟢 Доступні слоти в <u>{city}</u>!</b>

<b>📌 Послуга:</b> {poslyga}

{chr(10).join(date_info)}"""
    
    # Додаємо аналіз послуг, якщо є більше 2 слотів
    total_slots = sum(len(times_text.split()) for _, times_text in date_sections)
    
    if total_slots > 2:
        msg += f"\n\n📊 <b>Доступні записи за послугами:</b>\n"
        
        if services_summary['adult']:
            msg += f"\n🔹 <b>Паспорт дорослому</b> (10 хв):\n"
            for service_line in services_summary['adult']:
                msg += f"   • {service_line}\n"
        
        if services_summary['teen']:
            msg += f"\n🔹 <b>Паспорт дитині 12-16 років</b> (10 хв):\n"
            for service_line in services_summary['teen']:
                msg += f"   • {service_line}\n"
        
        if services_summary['child']:
            msg += f"\n🔹 <b>Паспорт дитині до 12 років</b> (15 хв):\n"
            for service_line in services_summary['child']:
                msg += f"   • {service_line}\n"
    
    msg += f"\n⚡ <i>Не барися — розбирають швидко!</i>\n\n#слоти #{poslyga.split()[0].lower()} #{city.replace(' ', '_')} #{'/'.join(all_dates)}"

    # Кнопка для переходу на сайт
    buttons = [Button.url("🔗 Записатися на слот", "https://id.e-consul.gov.ua/")]
    
    return msg, buttons


def test_parser():
    """Функція для тестування парсера"""
    test_message1 = """🆕 З'явились нові слоти!
🔸 Генеральне Консульство України в Торонто
🔸 Послуга: Оформлення закордонного паспорта
📅 Слоти які були опубліковані:
21.01.2026: 14:00 14:10 14:20 14:30 14:40 14:50 15:00 15:10 15:20 15:30 15:40 15:50 16:00 16:10 16:20 16:30 16:40 16:50 17:00 17:10 17:20 17:30 17:40 17:50
⚠️ У безкоштовній версії надходження повідомлень про нові слоти відбувається із затримкою 5 хвилин."""

    test_message2 = """🆕 З'явились нові слоти!
🔸 Генеральне Консульство України в Едмонтоні
🔸 Послуга: Оформлення закордонного паспорта
📅 Слоти які були опубліковані:
21.01.2026: 13:00 13:10 13:20 13:30 13:40 13:50 10:00 10:10 10:20 10:30 10:40 10:50 11:00 11:10 11:20 11:30 11:40 11:50 12:00 12:10 12:20 12:30 12:40 12:50
⚠️ У безкоштовній версії надходження повідомлень про нові слоти відбувається із затримкою 5 хвилин."""

    print("🧪 ТЕСТ 1 - Торонто:")
    print("=" * 60)
    result1, buttons1 = parse_slot_message(test_message1)
    if result1:
        print(result1)
    else:
        print("❌ Тест 1 неуспішний")
    
    print("\n🧪 ТЕСТ 2 - Едмонтон:")
    print("=" * 60)
    result2, buttons2 = parse_slot_message(test_message2)
    if result2:
        print(result2)
    else:
        print("❌ Тест 2 неуспішний")

if __name__ == "__main__":
    test_parser()