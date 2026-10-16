"""
cloud/storage.py: Storage engine for Cloud service managing cloud copy and online window results.
"""
import json
import sqlite3
import os
import time
from contextlib import contextmanager
from typing import Dict, List, Optional, Tuple, Any, Iterator


class CloudStorageEngine:
    def __init__(self, db_path: str = "cloud_storage.db"):
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
                arrived_at REAL NOT NULL,
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
                processed_at REAL NOT NULL,
                PRIMARY KEY (session_id, window_idx)
            );
            """)

    def register_session(self, session_id: str):
        with self._get_connection() as con:
            con.execute("INSERT OR IGNORE INTO sessions (session_id) VALUES (?)", (session_id,))

    def save_samples(
        self, session_id: str, sensor: int, first_index: int, samples: List[List[int]]
    ) -> int:
        now = time.time()
        rows = []
        for i, s in enumerate(samples):
            idx = first_index + i
            val0 = s[0]
            val1 = s[1]
            val2 = s[2] if len(s) > 2 else None
            rows.append((session_id, sensor, idx, val0, val1, val2, now))

        with self._get_connection() as con:
            cur = con.cursor()
            cur.executemany(
                """
                INSERT OR IGNORE INTO samples 
                (session_id, sensor, sample_index, val0, val1, val2, arrived_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                rows
            )
            return cur.rowcount

    def save_sync_samples(
        self, session_id: str, sensor: int, sample_items: List[Tuple[int, List[int]]]
    ) -> int:
        now = time.time()
        rows = []
        for idx, s in sample_items:
            val0 = s[0]
            val1 = s[1]
            val2 = s[2] if len(s) > 2 else None
            rows.append((session_id, sensor, idx, val0, val1, val2, now))

        with self._get_connection() as con:
            cur = con.cursor()
            cur.executemany(
                """
                INSERT OR IGNORE INTO samples 
                (session_id, sensor, sample_index, val0, val1, val2, arrived_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                rows
            )
            return cur.rowcount

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

    def get_samples_in_range(
        self, session_id: str, sensor: int, start_idx: int, end_idx: int
    ) -> Dict[int, List[int]]:
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

    def get_max_sample_index(self, session_id: str, sensor: int) -> int:
        with self._get_connection() as con:
            cur = con.cursor()
            cur.execute(
                "SELECT MAX(sample_index) FROM samples WHERE session_id = ? AND sensor = ?",
                (session_id, sensor)
            )
            row = cur.fetchone()
            return row[0] if row and row[0] is not None else -1

    def get_last_processed_window(self, session_id: str) -> int:
        with self._get_connection() as con:
            cur = con.cursor()
            cur.execute("SELECT MAX(window_idx) FROM windows WHERE session_id = ?", (session_id,))
            row = cur.fetchone()
            return row[0] if row and row[0] is not None else -1

    def save_window_result(self, session_id: str, res: Dict[str, Any]):
        now = time.time()
        with self._get_connection() as con:
            con.execute(
                """
                INSERT OR REPLACE INTO windows 
                (session_id, window_idx, mode, complete, missing_1, missing_2, 
                 mean_json, min_json, max_json, magnitude, heart_rate, processed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    res.get("heart_rate"),
                    now
                )
            )

    def get_window_results(self, session_id: str) -> List[Dict[str, Any]]:
        with self._get_connection() as con:
            cur = con.cursor()
            cur.execute(
                """
                SELECT window_idx, mode, complete, missing_1, missing_2,
                       mean_json, min_json, max_json, magnitude, heart_rate, processed_at
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
                    "heart_rate": row[9],
                    "processed_at": row[10]
                })
            return results

    def get_total_samples(self, session_id: str) -> int:
        with self._get_connection() as con:
            cur = con.cursor()
            cur.execute("SELECT COUNT(*) FROM samples WHERE session_id = ?", (session_id,))
            return cur.fetchone()[0]

    def export_data(self, session_id: str) -> Iterator[str]:
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
