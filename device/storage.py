"""
device/storage.py: SQLite WAL storage engine ensuring crash survival, power-cut bound, and zero duplication.
"""
import json
import sqlite3
import os
from contextlib import contextmanager
from typing import Dict, List, Optional, Tuple, Any, Iterator


class StorageEngine:
    def __init__(self, db_path: str = "pipeline_device.db"):
        self.db_path = db_path
        self._init_db()

    @contextmanager
    def _get_connection(self):
        con = sqlite3.connect(self.db_path, timeout=30.0)
        con.execute("PRAGMA journal_mode = WAL;")
        con.execute("PRAGMA synchronous = NORMAL;")
        try:
            yield con
            con.commit()
        finally:
            con.close()

    def _init_db(self):
        with self._get_connection() as con:

            con.executescript("""
            CREATE TABLE IF NOT EXISTS sessions (
                session_id TEXT PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS samples (
                session_id TEXT NOT NULL,
                sensor INTEGER NOT NULL,
                sample_index INTEGER NOT NULL,
                val0 INTEGER NOT NULL,
                val1 INTEGER NOT NULL,
                val2 INTEGER,
                PRIMARY KEY (session_id, sensor, sample_index)
            );

            CREATE TABLE IF NOT EXISTS losses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                sensor INTEGER NOT NULL,
                from_index INTEGER NOT NULL,
                to_index INTEGER,
                reason TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS windows (
                session_id TEXT NOT NULL,
                window_idx INTEGER NOT NULL,
                mode TEXT NOT NULL,
                complete INTEGER NOT NULL,
                missing_1 INTEGER NOT NULL,
                missing_2 INTEGER NOT NULL,
                mean_json TEXT,
                min_json TEXT,
                max_json TEXT,
                magnitude REAL,
                heart_rate REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (session_id, window_idx)
            );

            CREATE TABLE IF NOT EXISTS sync_state (
                session_id TEXT NOT NULL,
                sensor INTEGER NOT NULL,
                last_synced_index INTEGER NOT NULL DEFAULT -1,
                PRIMARY KEY (session_id, sensor)
            );
            """)

    def register_session(self, session_id: str):
        with self._get_connection() as con:
            con.execute(
                "INSERT OR IGNORE INTO sessions (session_id) VALUES (?)",
                (session_id,)
            )

    def save_samples(
        self, session_id: str, sensor: int, first_index: int, samples: List[List[int]]
    ) -> int:
        """
        Saves a batch of samples atomically. Uses INSERT OR IGNORE for deduplication.
        Returns the count of newly inserted rows.
        """
        if not samples:
            return 0
            
        rows = []
        for i, s in enumerate(samples):
            idx = first_index + i
            val0 = s[0]
            val1 = s[1]
            val2 = s[2] if len(s) > 2 else None
            rows.append((session_id, sensor, idx, val0, val1, val2))
            
        with self._get_connection() as con:
            cur = con.cursor()
            cur.executemany(
                """
                INSERT OR IGNORE INTO samples 
                (session_id, sensor, sample_index, val0, val1, val2)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                rows
            )
            inserted = cur.rowcount
            return inserted

    def record_loss(
        self, session_id: str, sensor: int, from_index: int, to_index: Optional[int], reason: str
    ):
        with self._get_connection() as con:
            con.execute(
                """
                INSERT INTO losses (session_id, sensor, from_index, to_index, reason)
                VALUES (?, ?, ?, ?, ?)
                """,
                (session_id, sensor, from_index, to_index, reason)
            )

    def get_max_sample_index(self, session_id: str, sensor: int) -> int:
        """
        Returns max sample_index saved for this sensor, or -1 if none.
        """
        with self._get_connection() as con:
            cur = con.cursor()
            cur.execute(
                "SELECT MAX(sample_index) FROM samples WHERE session_id = ? AND sensor = ?",
                (session_id, sensor)
            )
            row = cur.fetchone()
            return row[0] if row and row[0] is not None else -1

    def get_samples_in_range(
        self, session_id: str, sensor: int, start_idx: int, end_idx: int
    ) -> Dict[int, List[int]]:
        """
        Retrieves samples in [start_idx, end_idx] as {index: [values]}.
        """
        with self._get_connection() as con:
            cur = con.cursor()
            cur.execute(
                """
                SELECT sample_index, val0, val1, val2 
                FROM samples 
                WHERE session_id = ? AND sensor = ? AND sample_index BETWEEN ? AND ?
                ORDER BY sample_index ASC
                """,
                (session_id, sensor, start_idx, end_idx)
            )
            result = {}
            for row in cur.fetchall():
                idx = row[0]
                values = [row[1], row[2]] if row[3] is None else [row[1], row[2], row[3]]
                result[idx] = values
            return result

    def get_last_processed_window(self, session_id: str) -> int:
        with self._get_connection() as con:
            cur = con.cursor()
            cur.execute("SELECT MAX(window_idx) FROM windows WHERE session_id = ?", (session_id,))
            row = cur.fetchone()
            return row[0] if row and row[0] is not None else -1

    def save_window_result(self, session_id: str, res: Dict[str, Any]):
        with self._get_connection() as con:
            con.execute(
                """
                INSERT OR REPLACE INTO windows 
                (session_id, window_idx, mode, complete, missing_1, missing_2, 
                 mean_json, min_json, max_json, magnitude, heart_rate)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    session_id,
                    res["window"],
                    res["mode"],
                    1 if res["complete"] else 0,
                    res["missing_1"],
                    res["missing_2"],
                    json.dumps(res.get("mean")),
                    json.dumps(res.get("min")),
                    json.dumps(res.get("max")),
                    res.get("magnitude"),
                    res.get("heart_rate")
                )
            )

    def get_window_results(self, session_id: str) -> List[Dict[str, Any]]:
        with self._get_connection() as con:
            cur = con.cursor()
            cur.execute(
                """
                SELECT window_idx, mode, complete, missing_1, missing_2,
                       mean_json, min_json, max_json, magnitude, heart_rate
                FROM windows
                WHERE session_id = ?
                ORDER BY window_idx ASC
                """,
                (session_id,)
            )
            results = []
            for row in cur.fetchall():
                results.append({
                    "window": row[0],
                    "mode": row[1],
                    "complete": bool(row[2]),
                    "missing_1": row[3],
                    "missing_2": row[4],
                    "mean": json.loads(row[5]) if row[5] else None,
                    "min": json.loads(row[6]) if row[6] else None,
                    "max": json.loads(row[7]) if row[7] else None,
                    "magnitude": row[8],
                    "heart_rate": row[9]
                })
            return results

    def get_sync_state(self, session_id: str, sensor: int) -> int:
        with self._get_connection() as con:
            cur = con.cursor()
            cur.execute(
                "SELECT last_synced_index FROM sync_state WHERE session_id = ? AND sensor = ?",
                (session_id, sensor)
            )
            row = cur.fetchone()
            return row[0] if row else -1

    def update_sync_state(self, session_id: str, sensor: int, last_synced_index: int):
        with self._get_connection() as con:
            con.execute(
                """
                INSERT INTO sync_state (session_id, sensor, last_synced_index)
                VALUES (?, ?, ?)
                ON CONFLICT(session_id, sensor) DO UPDATE SET last_synced_index = excluded.last_synced_index
                """,
                (session_id, sensor, last_synced_index)
            )

    def get_unsynced_samples(
        self, session_id: str, sensor: int, limit: int = 2000
    ) -> List[Tuple[int, List[int]]]:
        last_synced = self.get_sync_state(session_id, sensor)
        with self._get_connection() as con:
            cur = con.cursor()
            cur.execute(
                """
                SELECT sample_index, val0, val1, val2
                FROM samples
                WHERE session_id = ? AND sensor = ? AND sample_index > ?
                ORDER BY sample_index ASC
                LIMIT ?
                """,
                (session_id, sensor, last_synced, limit)
            )
            items = []
            for row in cur.fetchall():
                idx = row[0]
                vals = [row[1], row[2]] if row[3] is None else [row[1], row[2], row[3]]
                items.append((idx, vals))
            return items

    def get_status(self, session_id: str) -> Dict[str, int]:
        with self._get_connection() as con:
            cur = con.cursor()
            cur.execute(
                "SELECT COUNT(*) FROM samples WHERE session_id = ?",
                (session_id,)
            )
            samples_saved = cur.fetchone()[0]

            s1_last = self.get_sync_state(session_id, 1)
            s2_last = self.get_sync_state(session_id, 2)

            cur.execute(
                """
                SELECT COUNT(*) FROM samples 
                WHERE session_id = ? AND (
                    (sensor = 1 AND sample_index <= ?) OR 
                    (sensor = 2 AND sample_index <= ?)
                )
                """,
                (session_id, s1_last, s2_last)
            )
            synced_row = cur.fetchone()
            samples_cloud = synced_row[0] if synced_row and synced_row[0] is not None else 0
            backlog = max(0, samples_saved - samples_cloud)
            return {
                "samples_saved": samples_saved,
                "samples_cloud": samples_cloud,
                "backlog": backlog
            }


    def export_data(self, session_id: str) -> Iterator[str]:
        """
        Yields JSON lines according to Section 4:
        First: samples: {"sensor": 1, "index": 120000, "values": [720, 1200, 1560]}
        Second: losses: {"sensor": 2, "from_index": 4000, "to_index": 4399, "reason": "..."}
        """
        sample_rows = []
        loss_rows = []
        with self._get_connection() as con:
            cur = con.cursor()
            cur.execute(
                """
                SELECT sensor, sample_index, val0, val1, val2
                FROM samples
                WHERE session_id = ?
                ORDER BY sensor ASC, sample_index ASC
                """,
                (session_id,)
            )
            sample_rows = cur.fetchall()

            cur.execute(
                """
                SELECT sensor, from_index, to_index, reason
                FROM losses
                WHERE session_id = ?
                ORDER BY sensor ASC, from_index ASC
                """,
                (session_id,)
            )
            loss_rows = cur.fetchall()

        for row in sample_rows:
            sensor = row[0]
            idx = row[1]
            vals = [row[2], row[3]] if row[4] is None else [row[2], row[3], row[4]]
            yield json.dumps({"sensor": sensor, "index": idx, "values": vals})

        for row in loss_rows:
            yield json.dumps({
                "sensor": row[0],
                "from_index": row[1],
                "to_index": row[2],
                "reason": row[3]
            })

