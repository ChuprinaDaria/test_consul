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
            [Button.callback("📊 Статистика за тиждень", b"stats_7")],
            [Button.callback("📆 Статистика за місяць", b"stats_30")]
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
                [Button.callback("📊 За тиждень", b"stats_7")],
                [Button.callback("📆 За місяць", b"stats_30")],
                [Button.callback("🔄 Оновити", f"stats_{days}".encode())]
            ]
            
            await event.edit(stats_msg, buttons=buttons)
    
    def format_simple_statistics(self, period_days):
        """Проста статистика без зайвого"""
        data = get_statistics_data(period_days)
        
        if not data:
            return f"📊 **Статистика за {period_days} днів**\n\n❌ Даних немає"
        
        # Підрахунки
        total_messages = len(data)
        total_slots = sum(slots or 0 for _, _, slots, _, _ in data)
        
        city_stats = defaultdict(int)
        service_stats = defaultdict(int)
        
        for city, service, slots, _, _ in data:
            if city:
                city_stats[city] += 1
            if service:
                service_stats[service] += 1
        
        # Форматування
        msg = f"📊 **Статистика за {period_days} днів**\n\n"
        msg += f"📈 **Всього повідомлень:** {total_messages}\n"
        msg += f"🎯 **Всього слотів:** {total_slots}\n"
        msg += f"📊 **В середньому слотів на повідомлення:** {total_slots/total_messages:.1f}\n\n"
        
        # Топ міст
        msg += "🏙️ **Найактивніші міста:**\n"
        for i, (city, count) in enumerate(sorted(city_stats.items(), key=lambda x: x[1], reverse=True)[:3], 1):
            percentage = (count / total_messages) * 100
            msg += f"{i}. **{city}**: {count} разів ({percentage:.1f}%)\n"
        
        return msg