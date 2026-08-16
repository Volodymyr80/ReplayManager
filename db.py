import sqlite3
import logging

logger = logging.getLogger("replay_manager.db")

def get_db_connection(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    # Enable WAL mode for concurrent read/write support
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def init_db(db_path: str):
    logger.info("Initializing database: %s", db_path)
    with get_db_connection(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticket_id TEXT NOT NULL,
                event_hash TEXT,
                routing_key TEXT NOT NULL,
                payload TEXT NOT NULL,
                status TEXT NOT NULL,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Ensure event_hash column exists (fallback for existing DBs)
        try:
            conn.execute("ALTER TABLE events ADD COLUMN event_hash TEXT;")
        except sqlite3.OperationalError:
            pass # Column already exists
            
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_ticket_id ON events (ticket_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_event_hash ON events (event_hash);")
        conn.commit()
    logger.info("Database initialized successfully.")

def save_event(db_path: str, ticket_id: str, event_hash: str, routing_key: str, payload: str, status: str = "PENDING", error_message: str = None) -> int:
    with get_db_connection(db_path) as conn:
        cursor = conn.execute(
            """
            INSERT OR IGNORE INTO events (ticket_id, event_hash, routing_key, payload, status, error_message, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
            """,
            (ticket_id, event_hash, routing_key, payload, status, error_message)
        )
        conn.commit()
        return cursor.lastrowid

def get_ticket_events(db_path: str, ticket_id: str) -> list:
    search_term = ticket_id.strip()
    search_variants = [search_term]
    
    # If the search term is a number, create variants with/without leading zeros
    if search_term.isdigit():
        trimmed = search_term.lstrip('0') or '0'
        if trimmed not in search_variants:
            search_variants.append(trimmed)
        
        # Padded to standard 7 characters
        padded = search_term.zfill(7)
        if padded not in search_variants:
            search_variants.append(padded)
            
    placeholders = ",".join("?" for _ in search_variants)
    query = f"""
        SELECT * FROM events 
        WHERE ticket_id IN ({placeholders}) 
           OR json_extract(payload, '$.subject') LIKE ? 
        ORDER BY created_at ASC
    """
    
    params = search_variants + [f"%{search_term}%"]
    
    with get_db_connection(db_path) as conn:
        cursor = conn.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]

def get_database_stats(db_path: str) -> dict:
    with get_db_connection(db_path) as conn:
        # Get total count
        cursor = conn.execute("SELECT COUNT(*) FROM events")
        total = cursor.fetchone()[0]
        
        # Get count per queue (routing_key)
        cursor = conn.execute("SELECT routing_key, COUNT(*) as count FROM events GROUP BY routing_key")
        queues = {row["routing_key"]: row["count"] for row in cursor.fetchall()}
        
        return {"total": total, "queues": queues}

def get_event_by_id(db_path: str, event_id: int) -> dict | None:
    with get_db_connection(db_path) as conn:
        cursor = conn.execute("SELECT * FROM events WHERE id = ?", (event_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

def update_event_status(db_path: str, event_id: int, status: str, error_message: str = None):
    with get_db_connection(db_path) as conn:
        conn.execute(
            """
            UPDATE events 
            SET status = ?, error_message = ?, updated_at = datetime('now')
            WHERE id = ?
            """,
            (status, error_message, event_id)
        )
        conn.commit()

def update_event_by_hash(db_path: str, event_hash: str, status: str, error_message: str = None):
    with get_db_connection(db_path) as conn:
        conn.execute(
            """
            UPDATE events 
            SET status = ?, error_message = ?, updated_at = datetime('now')
            WHERE event_hash = ?
            """,
            (status, error_message, event_hash)
        )
        conn.commit()

def get_latest_events(db_path: str, queue_name: str = None, limit: int = 30) -> list:
    with get_db_connection(db_path) as conn:
        if queue_name and queue_name != "total":
            cursor = conn.execute(
                "SELECT * FROM events WHERE routing_key = ? ORDER BY created_at DESC LIMIT ?",
                (queue_name, limit)
            )
        else:
            cursor = conn.execute(
                "SELECT * FROM events ORDER BY created_at DESC LIMIT ?",
                (limit,)
            )
        return [dict(row) for row in cursor.fetchall()]

def prune_old_events(db_path: str, days: int = 30) -> int:
    with get_db_connection(db_path) as conn:
        cursor = conn.execute(
            "DELETE FROM events WHERE created_at < datetime('now', ?)",
            (f"-{days} days",)
        )
        conn.commit()
        deleted_count = cursor.rowcount
        if deleted_count > 0:
            logger.info("action=prune_old_events status=success deleted_records=%d retention_days=%d", deleted_count, days)
        return deleted_count
