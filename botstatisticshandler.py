from collections import defaultdict
from telethon.tl.custom import Button
from db import get_statistics_data

class BotStatisticsHandler:
    def __init__(self):
        pass
    
    async def handle_start_command(self, event):
        """Обробка команди /start"""
        welcome_msg = """🤖 **Привіт! Я бот для відстеження слотів консульств України в Канаді**

📊 **Функції:**
• Автоматичне відстеження нових слотів
• Оновлення повідомлень коли слоти зайняті
• Тихі сповіщення за 5 хв (на основі статистики)
• Проста статистика

Натисни кнопку нижче для перегляду статистики!"""
        
        buttons = [
            [Button.inline("📊 Статистика за тиждень", b"stats_7")],
            [Button.inline("📆 Статистика за місяць", b"stats_30")]
        ]
        
        await event.respond(welcome_msg, buttons=buttons)
    
    async def handle_stats_callback(self, event):
        """Обробка callback для статистики"""
        data = event.data.decode()
        
        if data.startswith("stats_"):
            # Підтримуємо і старий і новий формат
            period_str = data.replace("stats_", "")
            
            if period_str == "week":
                days = 7
            elif period_str == "month": 
                days = 30
            elif period_str == "year":
                days = 365
            elif period_str == "refresh":
                days = 7  # за замовчуванням
            else:
                try:
                    days = int(period_str)
                except ValueError:
                    days = 7  # fallback
            
            stats_msg = self.format_simple_statistics(days)
            
            buttons = [
                [Button.inline("📊 За тиждень", b"stats_7")],
                [Button.inline("📆 За місяць", b"stats_30")],
                [Button.inline("🔄 Оновити", f"stats_{days}".encode())]
            ]
            
            # Перевіряємо чи відрізняється контент перед редагуванням
            try:
                await event.edit(stats_msg, buttons=buttons)
            except Exception as e:
                if "MessageNotModifiedError" in str(e):
                    print("⚠️ Контент не змінився, пропускаємо редагування")
                    await event.answer("🔄 Дані вже актуальні")
                else:
                    print(f"❌ Помилка редагування статистики: {e}")
                    await event.answer("❌ Помилка оновлення")
    
    def format_simple_statistics(self, period_days):
        """Проста статистика з годинами для кожного міста"""
        from db import get_statistics_data
        from datetime import datetime
        from collections import defaultdict
        import pytz
        
        data = get_statistics_data(period_days)
        
        if not data:
            return f"📊 **Статистика за {period_days} днів**\n\n❌ Даних немає"
        
        # Підрахунки
        total_messages = len(data)
        total_slots = sum(slots or 0 for _, _, slots, _, _ in data)
        
        city_stats = defaultdict(int)
        service_stats = defaultdict(int)
        city_hours = defaultdict(lambda: defaultdict(int))  # {місто: {година: кількість}}
        
        CANADA_TZ = pytz.timezone('America/Toronto')
        
        for city, service, slots, canada_time_str, timestamp in data:
            if city:
                city_stats[city] += 1
                
                # Обробка часу для статистики по годинах
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
                    
                    city_hours[city][ct.hour] += 1
                except:
                    continue
                    
            if service:
                service_stats[service] += 1
        
        # Форматування
        msg = f"📊 **Статистика за {period_days} днів**\n\n"
        msg += f"📈 **Всього повідомлень:** {total_messages}\n"
        msg += f"🎯 **Всього слотів:** {total_slots}\n"
        msg += f"📊 **В середньому слотів на повідомлення:** {total_slots/total_messages:.1f}\n\n"
        
        # Топ міст з годинами
        msg += "🏙️ **Найактивніші міста:**\n"
        sorted_cities = sorted(city_stats.items(), key=lambda x: x[1], reverse=True)[:3]
        
        for i, (city, count) in enumerate(sorted_cities, 1):
            percentage = (count / total_messages) * 100
            msg += f"{i}. **{city}**: {count} разів ({percentage:.1f}%)\n"
            
            # Топ-3 години для цього міста
            if city in city_hours:
                top_hours = sorted(city_hours[city].items(), key=lambda x: x[1], reverse=True)[:3]
                if top_hours:
                    hours_str = ", ".join([f"{h:02d}:00" for h, _ in top_hours])
                    msg += f"   🕐 Найчастіші години: {hours_str}\n"
            msg += "\n"
        
        return msg