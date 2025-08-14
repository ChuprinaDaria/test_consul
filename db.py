import sqlite3
from datetime import datetime, timedelta
import pytz

DB_FILE = 'processed_messages.db'
CANADA_TZ = pytz.timezone('America/Toronto')

def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS processed (
                msg_id INTEGER PRIMARY KEY,
                content_hash TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                city TEXT,
                service TEXT,
                slots_count INTEGER,
                available_dates TEXT,
                canada_time DATETIME,
                sent_msg_id INTEGER,
                is_gone_processed BOOLEAN DEFAULT 0
            )
        ''')
        
        # Міграція існуючих колонок
        existing_columns = [row[1] for row in cursor.execute("PRAGMA table_info(processed)").fetchall()]
        
        columns_to_add = [
            ('city', 'TEXT'),
            ('service', 'TEXT'),
            ('slots_count', 'INTEGER'),
            ('available_dates', 'TEXT'),
            ('canada_time', 'DATETIME'),
            ('sent_msg_id', 'INTEGER'),
            ('is_gone_processed', 'BOOLEAN DEFAULT 0')
        ]
        
        for column_name, column_type in columns_to_add:
            if column_name not in existing_columns:
                try:
                    cursor.execute(f'ALTER TABLE processed ADD COLUMN {column_name} {column_type}')
                    print(f"✅ Додано колонку {column_name}")
                except sqlite3.OperationalError:
                    pass
        
        conn.commit()

def is_processed(msg_id: int) -> bool:
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT 1 FROM processed WHERE msg_id = ?', (msg_id,))
        return cursor.fetchone() is not None

def is_content_processed_recently(content_hash: str, minutes: int = 30) -> bool:
    if not content_hash:
        return False
        
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        time_limit = datetime.now() - timedelta(minutes=minutes)
        time_limit_str = time_limit.strftime('%Y-%m-%d %H:%M:%S')
        
        cursor.execute('''
            SELECT 1 FROM processed 
            WHERE content_hash = ? AND timestamp > ? AND is_gone_processed = 0
        ''', (content_hash, time_limit_str))
        
        result = cursor.fetchone() is not None
        
        if result:
            print(f"🕒 Контент з хешем {content_hash[:8]}... вже публікувався протягом останніх {minutes} хвилин")
        else:
            print(f"✅ Контент з хешом {content_hash[:8]}... можна публікувати")
            
        return result

def mark_processed(msg_id: int, content_hash: str = None):
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute('INSERT OR IGNORE INTO processed (msg_id, content_hash) VALUES (?, ?)', 
                      (msg_id, content_hash))
        conn.commit()

def mark_processed_with_stats(msg_id: int, content_hash: str, city: str = None, service: str = None, slots_count: int = None, available_dates: list = None):
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        canada_time = datetime.now(pytz.UTC).astimezone(CANADA_TZ)
        
        cursor.execute('''
            INSERT OR IGNORE INTO processed 
            (msg_id, content_hash, city, service, slots_count, available_dates, canada_time, is_gone_processed) 
            VALUES (?, ?, ?, ?, ?, ?, ?, 0)
        ''', (msg_id, content_hash, city, service, slots_count, 
              str(available_dates) if available_dates else None, canada_time.isoformat()))
        conn.commit()

def save_sent_message(content_hash: str, sent_msg_id: int):
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE processed SET sent_msg_id = ? WHERE content_hash = ? AND is_gone_processed = 0
        ''', (sent_msg_id, content_hash))
        conn.commit()

def get_sent_message_id_by_city(city: str):
    """Знаходить останнє активне повідомлення для міста"""
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT sent_msg_id, content_hash FROM processed 
            WHERE city = ? AND sent_msg_id IS NOT NULL AND is_gone_processed = 0
            ORDER BY timestamp DESC LIMIT 1
        ''', (city,))
        row = cursor.fetchone()
        return row[0] if row else None, row[1] if row else None

def mark_gone_processed(content_hash: str, gone_msg_id: int):
    """Позначає що для цього контенту оброблено повідомлення про зайнятість"""
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE processed SET is_gone_processed = 1 WHERE content_hash = ?
        ''', (content_hash,))
        # Додаємо запис про "gone" повідомлення
        cursor.execute('''
            INSERT OR IGNORE INTO processed (msg_id, is_gone_processed) VALUES (?, 1)
        ''', (gone_msg_id,))
        conn.commit()

def cleanup_old_records(days: int = 30):
    """Видаляє записи старше вказаної кількості днів"""
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            DELETE FROM processed 
            WHERE timestamp < datetime('now', '-' || ? || ' days')
        ''', (days,))
        deleted = cursor.rowcount
        conn.commit()
        return deleted

def get_recent_publications(hours: int = 24):
    """Показує останні публікації для діагностики"""
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT msg_id, content_hash, timestamp, city, service, slots_count
            FROM processed 
            WHERE timestamp > datetime('now', '-' || ? || ' hours')
            ORDER BY timestamp DESC
        ''', (hours,))
        return cursor.fetchall()

def get_statistics_data(days: int = 30):
    """Отримує дані для статистики"""
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        since_date = datetime.now() - timedelta(days=days)
        
        cursor.execute('''
            SELECT city, service, slots_count, canada_time, timestamp
            FROM processed 
            WHERE city IS NOT NULL 
              AND timestamp >= ?
              AND is_gone_processed = 0
            ORDER BY timestamp DESC
        ''', (since_date.strftime('%Y-%m-%d %H:%M:%S'),))
        
        return cursor.fetchall()