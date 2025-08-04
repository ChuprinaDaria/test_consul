import sqlite3
from datetime import datetime, timedelta
from collections import defaultdict, Counter
import pytz
from telethon.tl.custom import Button
import re

# Використовуємо вашу існуючу БД
DB_FILE = 'processed_messages.db'

class StatisticsModule:
    def __init__(self, db_path=DB_FILE):
        self.db_path = db_path
        self.canada_tz = pytz.timezone('America/Toronto')  # Часовий пояс Канади
        self.init_statistics_tables()
    
    def init_statistics_tables(self):
        """Додає таблиці статистики до існуючої БД"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Додаємо колонки до існуючої таблиці processed (якщо їх немає)
        try:
            cursor.execute('ALTER TABLE processed ADD COLUMN city TEXT')
        except sqlite3.OperationalError:
            pass  # Колонка вже існує
            
        try:
            cursor.execute('ALTER TABLE processed ADD COLUMN service TEXT')
        except sqlite3.OperationalError:
            pass
            
        try:
            cursor.execute('ALTER TABLE processed ADD COLUMN slots_count INTEGER')
        except sqlite3.OperationalError:
            pass
            
        try:
            cursor.execute('ALTER TABLE processed ADD COLUMN available_dates TEXT')
        except sqlite3.OperationalError:
            pass
            
        try:
            cursor.execute('ALTER TABLE processed ADD COLUMN canada_time DATETIME')
        except sqlite3.OperationalError:
            pass
        
        # Таблиця для статистики натискань кнопок
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS button_clicks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                button_type TEXT,
                click_time DATETIME,
                canada_time DATETIME
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def update_processed_record(self, msg_id, city, service, slots_count, available_dates, content_hash):
        """Оновлює існуючий запис з додатковими даними для статистики"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Конвертуємо час в часовий пояс Канади
        cursor.execute('SELECT timestamp FROM processed WHERE msg_id = ?', (msg_id,))
        result = cursor.fetchone()
        
        if result:
            # Парсимо існуючий timestamp
            timestamp_str = result[0]
            try:
                utc_time = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                utc_time = pytz.UTC.localize(utc_time)
                canada_time = utc_time.astimezone(self.canada_tz)
            except:
                # Якщо не вдалось розпарсити, використовуємо поточний час
                now_utc = datetime.now(pytz.UTC)
                canada_time = now_utc.astimezone(self.canada_tz)
            
            # Оновлюємо запис з додатковими даними
            cursor.execute('''
                UPDATE processed 
                SET city = ?, service = ?, slots_count = ?, available_dates = ?, canada_time = ?
                WHERE msg_id = ?
            ''', (city, service, slots_count, str(available_dates), canada_time.isoformat(), msg_id))
            
            conn.commit()
            conn.close()
            return True
        
        conn.close()
        return False
    
    def log_button_click(self, user_id, button_type):
        """Логування натискання кнопки"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        now_utc = datetime.now(pytz.UTC)
        canada_time = now_utc.astimezone(self.canada_tz)
        
        cursor.execute('''
            INSERT INTO button_clicks (user_id, button_type, click_time, canada_time)
            VALUES (?, ?, ?, ?)
        ''', (user_id, button_type, now_utc, canada_time))
        
        conn.commit()
        conn.close()
    
    def get_statistics(self, period='week'):
        """Отримання статистики за період з існуючої БД"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Визначаємо період
        now = datetime.now()
        if period == 'week':
            start_date = now - timedelta(days=7)
            period_name = "тиждень"
        elif period == 'month':
            start_date = now - timedelta(days=30)
            period_name = "місяць"
        elif period == 'year':
            start_date = now - timedelta(days=365)
            period_name = "рік"
        else:
            start_date = now - timedelta(days=7)
            period_name = "тиждень"
        
        start_date_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
        
        # Статистика з таблиці processed
        cursor.execute('''
            SELECT city, service, canada_time, slots_count, timestamp, content_hash 
            FROM processed 
            WHERE timestamp >= ? 
            AND city IS NOT NULL
            ORDER BY timestamp
        ''', (start_date_str,))
        
        slot_data = cursor.fetchall()
        
        # Статистика натискань кнопок
        cursor.execute('''
            SELECT button_type, canada_time, user_id
            FROM button_clicks 
            WHERE click_time >= ?
            ORDER BY canada_time
        ''', (start_date_str,))
        
        click_data = cursor.fetchall()
        
        conn.close()
        
        return self.analyze_data(slot_data, click_data, period_name)
    
    def analyze_data(self, slot_data, click_data, period_name):
        """Аналіз даних та формування статистики"""
        stats = {
            'period': period_name,
            'total_messages': len(slot_data),
            'total_clicks': len(click_data),
            'cities_stats': defaultdict(int),
            'services_stats': defaultdict(int),
            'hourly_distribution': defaultdict(int),
            'daily_distribution': defaultdict(int),
            'total_slots': 0,
            'avg_slots_per_message': 0,
            'most_active_hours': [],
            'most_active_days': [],
            'click_stats': defaultdict(int)
        }
        
        if not slot_data:
            return self.format_empty_stats(period_name)
        
        # Аналіз повідомлень про слоти
        for city, service, canada_time_str, slots_count, timestamp, content_hash in slot_data:
            # Спробуємо отримати час з canada_time або timestamp
            try:
                if canada_time_str:
                    if 'T' in canada_time_str:
                        canada_time = datetime.fromisoformat(canada_time_str.replace('Z', '+00:00'))
                    else:
                        canada_time = datetime.strptime(canada_time_str, '%Y-%m-%d %H:%M:%S')
                else:
                    # Використовуємо timestamp і конвертуємо в часовий пояс Канади
                    utc_time = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
                    utc_time = pytz.UTC.localize(utc_time)
                    canada_time = utc_time.astimezone(self.canada_tz)
            except:
                # Якщо не вдалось розпарсити, пропускаємо
                continue
            
            if city:
                stats['cities_stats'][city] += 1
            if service:
                stats['services_stats'][service] += 1
            
            if slots_count:
                stats['total_slots'] += slots_count
            
            # Розподіл по годинах (коли з'являлися повідомлення)
            hour = canada_time.hour
            stats['hourly_distribution'][hour] += 1
            
            # Розподіл по днях тижня
            day_name = canada_time.strftime('%A')
            stats['daily_distribution'][day_name] += 1
        
        # Аналіз натискань кнопок
        for button_type, click_time_str, user_id in click_data:
            stats['click_stats'][button_type] += 1
        
        # Розрахунок середніх значень
        if stats['total_messages'] > 0:
            stats['avg_slots_per_message'] = stats['total_slots'] / stats['total_messages']
        
        # Найактивніші години (топ 3)
        stats['most_active_hours'] = sorted(
            stats['hourly_distribution'].items(), 
            key=lambda x: x[1], 
            reverse=True
        )[:3]
        
        # Найактивніші дні (топ 3)
        stats['most_active_days'] = sorted(
            stats['daily_distribution'].items(), 
            key=lambda x: x[1], 
            reverse=True
        )[:3]
        
        return self.format_statistics(stats)
    
    def format_empty_stats(self, period_name):
        """Форматування порожньої статистики"""
        return f"""📊 **Статистика за {period_name}**

❌ За цей період даних немає

🔍 Можливі причини:
• Не було повідомлень про слоти
• Період занадто короткий
• База даних порожня

Спробуйте вибрати більший period або зачекайте нових повідомлень."""

    def format_statistics(self, stats):
        """Форматування статистики у красивий текст"""
        msg = f"""📊 **Статистика за {stats['period']}**

📈 **Загальна інформація:**
• Всього повідомлень про слоти: {stats['total_messages']}
• Всього доступних слотів: {stats['total_slots']}
• Середня кількість слотів на повідомлення: {stats['avg_slots_per_message']:.1f}
• Натискань кнопок: {stats['total_clicks']}

🏙️ **Статистика по містах:**"""
        
        # Топ міст
        top_cities = sorted(stats['cities_stats'].items(), key=lambda x: x[1], reverse=True)
        for i, (city, count) in enumerate(top_cities[:5], 1):
            percentage = (count / stats['total_messages']) * 100
            msg += f"\n{i}. {city}: {count} повідомлень ({percentage:.1f}%)"
        
        msg += f"\n\n🛡️ **Статистика по послугах:**"
        
        # Топ послуг
        top_services = sorted(stats['services_stats'].items(), key=lambda x: x[1], reverse=True)
        for i, (service, count) in enumerate(top_services[:3], 1):
            percentage = (count / stats['total_messages']) * 100
            msg += f"\n{i}. {service}: {count} повідомлень ({percentage:.1f}%)"
        
        msg += f"\n\n🕐 **Найактивніші години (час Канади):**"
        
        for i, (hour, count) in enumerate(stats['most_active_hours'], 1):
            percentage = (count / stats['total_messages']) * 100
            msg += f"\n{i}. {hour:02d}:00-{hour+1:02d}:00: {count} повідомлень ({percentage:.1f}%)"
        
        msg += f"\n\n📅 **Найактивніші дні тижня:**"
        
        day_names = {
            'Monday': 'Понеділок',
            'Tuesday': 'Вівторок', 
            'Wednesday': 'Середа',
            'Thursday': 'Четвер',
            'Friday': "П'ятниця",
            'Saturday': 'Субота',
            'Sunday': 'Неділя'
        }
        
        for i, (day, count) in enumerate(stats['most_active_days'], 1):
            percentage = (count / stats['total_messages']) * 100
            day_ua = day_names.get(day, day)
            msg += f"\n{i}. {day_ua}: {count} повідомлень ({percentage:.1f}%)"
        
        if stats['click_stats']:
            msg += f"\n\n🖱️ **Статистика натискань:**"
            for button_type, count in stats['click_stats'].items():
                msg += f"\n• {button_type}: {count} натискань"
        
        return msg
    
    def get_statistics_buttons(self):
        """Генерація кнопок для вибору періоду статистики"""
        buttons = [
            [Button.callback("📅 За тиждень", b"stats_week")],
            [Button.callback("📆 За місяць", b"stats_month")],
            [Button.callback("🗓️ За рік", b"stats_year")],
            [Button.callback("🔄 Оновити", b"stats_refresh")]
        ]
        return buttons
# Приклад використання в основному боті
class BotStatisticsHandler:
    def __init__(self):
        self.stats_module = StatisticsModule()
    
    async def handle_start_command(self, event):
        """Обробка команди /start"""
        self.stats_module.log_button_click(event.sender_id, "start_command")
        
        welcome_msg = """🤖 **Привіт! Я бот для відстеження слотів консульств України в Канаді**

📊 Хочеш переглянути статистику? Натисни кнопку нижче!

🔔 Я автоматично відстежую появу нових слотів та збираю статистику:
• По містах та консульствах
• По часу появи слотів  
• По популярних послугах
• По активності користувачів"""
        
        buttons = [
            [Button.callback("📊 Переглянути статистику", b"show_stats")],
            [Button.url("ℹ️ Довідка", "https://t.me/your_help_channel")]
        ]
        
        await event.respond(welcome_msg, buttons=buttons)
    
    async def handle_stats_callback(self, event):
        """Обробка callback для статистики"""
        data = event.data.decode()
        
        if data == "show_stats":
            self.stats_module.log_button_click(event.sender_id, "show_stats")
            msg = "📊 **Оберіть період для перегляду статистики:**"
            buttons = self.stats_module.get_statistics_buttons()
            await event.edit(msg, buttons=buttons)
            
        elif data.startswith("stats_"):
            period = data.replace("stats_", "")
            self.stats_module.log_button_click(event.sender_id, f"stats_{period}")
            
            if period == "refresh":
                period = "week"  # За замовчуванням тиждень
            
            stats_msg = self.stats_module.get_statistics(period)
            buttons = self.stats_module.get_statistics_buttons()
            
            await event.edit(stats_msg, buttons=buttons)
    
    def log_new_slot_message(self, city, service, slots_count, available_dates, content_hash):
        """Логування нového повідомлення про слоти"""
        return self.stats_module.log_slot_message(city, service, slots_count, available_dates, content_hash)


# Функції для інтеграції з вашою існуючою системою
def is_processed(msg_id: int) -> bool:
    """Перевіряє чи повідомлення вже оброблено"""
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT 1 FROM processed WHERE msg_id = ?', (msg_id,))
        return cursor.fetchone() is not None

def is_content_processed_recently(content_hash: str, minutes: int = 30) -> bool:
    """Перевіряє чи контент публікувався нещодавно"""
    if not content_hash:
        return False
        
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        
        time_limit = datetime.now() - timedelta(minutes=minutes)
        time_limit_str = time_limit.strftime('%Y-%m-%d %H:%M:%S')
        
        cursor.execute('''
            SELECT 1 FROM processed 
            WHERE content_hash = ? 
            AND timestamp > ?
        ''', (content_hash, time_limit_str))
        
        return cursor.fetchone() is not None

def mark_processed_with_stats(msg_id: int, content_hash: str, city: str = None, service: str = None, slots_count: int = None, available_dates: list = None):
    """Позначає повідомлення як оброблене з додатковими даними для статистики"""
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        
        # Конвертуємо час в часовий пояс Канади для статистики
        canada_tz = pytz.timezone('America/Toronto')
        utc_time = datetime.now(pytz.UTC)
        canada_time = utc_time.astimezone(canada_tz)
        
        cursor.execute('''INSERT OR IGNORE INTO processed 
                      (msg_id, content_hash, city, service, slots_count, available_dates, canada_time) 
                      VALUES (?, ?, ?, ?, ?, ?, ?)''', 
                      (msg_id, content_hash, city, service, slots_count, str(available_dates) if available_dates else None, canada_time.isoformat()))
        conn.commit()