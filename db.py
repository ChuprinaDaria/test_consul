import sqlite3

DB_FILE = 'processed_messages.db'

def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS processed (
                msg_id INTEGER PRIMARY KEY
            )
        ''')
        conn.commit()

def is_processed(msg_id: int) -> bool:
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT 1 FROM processed WHERE msg_id = ?', (msg_id,))
        return cursor.fetchone() is not None

def mark_processed(msg_id: int):
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute('INSERT OR IGNORE INTO processed (msg_id) VALUES (?)', (msg_id,))
        conn.commit()
