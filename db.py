import sqlite3

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

def is_content_processed(content_hash: str) -> bool:
    """Перевіряє чи вже було оброблено повідомлення з таким же контентом"""
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT 1 FROM processed WHERE content_hash = ?', (content_hash,))
        return cursor.fetchone() is not None

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
    #