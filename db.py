import sqlite3
from datetime import datetime, timedelta

DB_FILE = 'processed_messages.db'

def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS processed (
                msg_id INTEGER PRIMARY KEY,
                content_hash TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()

def is_processed(msg_id: int) -> bool:
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT 1 FROM processed WHERE msg_id = ?', (msg_id,))
        return cursor.fetchone() is not None

def is_content_processed_recently(content_hash: str, minutes: int = 30) -> bool:
    """
    Перевіряє чи був опублікований такий же контент протягом останніх N хвилин
    """
    if not content_hash:
        return False
        
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        
        # Розраховуємо час N хвилин тому
        time_limit = datetime.now() - timedelta(minutes=minutes)
        time_limit_str = time_limit.strftime('%Y-%m-%d %H:%M:%S')
        
        # Шукаємо записи з таким же хешем за останні N хвилин
        cursor.execute('''
            SELECT 1 FROM processed 
            WHERE content_hash = ? 
            AND timestamp > ?
        ''', (content_hash, time_limit_str))
        
        result = cursor.fetchone() is not None
        
        if result:
            print(f"🕒 Контент з хешем {content_hash[:8]}... вже публікувався протягом останніх {minutes} хвилин")
        else:
            print(f"✅ Контент з хешом {content_hash[:8]}... можна публікувати (не було протягом {minutes} хв)")
            
        return result

def mark_processed(msg_id: int, content_hash: str = None):
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute('INSERT OR IGNORE INTO processed (msg_id, content_hash) VALUES (?, ?)', 
                      (msg_id, content_hash))
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
            SELECT msg_id, content_hash, timestamp 
            FROM processed 
            WHERE timestamp > datetime('now', '-' || ? || ' hours')
            ORDER BY timestamp DESC
        ''', (hours,))
        return cursor.fetchall()
    

def add_sent_msg_column():
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        try:
            cursor.execute('ALTER TABLE processed ADD COLUMN sent_msg_id INTEGER')
        except sqlite3.OperationalError:
            pass
        conn.commit()

def save_sent_message(content_hash: str, sent_msg_id: int):
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE processed
            SET sent_msg_id = ?
            WHERE content_hash = ?
        ''', (sent_msg_id, content_hash))
        conn.commit()

def get_sent_message_id(content_hash: str):
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT sent_msg_id FROM processed WHERE content_hash = ?', (content_hash,))
        row = cursor.fetchone()
        return row[0] if row else None

def get_sent_message_id_by_city(city: str):
    """
    Повертає останній sent_msg_id для заданого міста.
    """
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT sent_msg_id 
            FROM processed 
            WHERE city = ?
            ORDER BY timestamp DESC 
            LIMIT 1
        """, (city,))
        row = cursor.fetchone()
        return row[0] if row else None
