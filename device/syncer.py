"""
device/syncer.py: Real-time cloud streaming and high-speed backlog catch-up synchronizer.
"""
import asyncio
import json
import logging
import urllib.request
import urllib.error
from typing import Optional, List, Tuple
from device.storage import StorageEngine
from device.mode import ModeController

logger = logging.getLogger("device.syncer")


class CloudSynchronizer:
    def __init__(
        self,
        storage: StorageEngine,
        mode_controller: ModeController,
        cloud_url: str = "http://127.0.0.1:8080",
        batch_size: int = 1000
    ):
        self.storage = storage
        self.mode_controller = mode_controller
        self.cloud_url = cloud_url.rstrip("/")
        self.batch_size = batch_size
        self.is_running = False

    def _post_json(self, endpoint: str, payload: dict, timeout: float = 3.0) -> bool:
        url = f"{self.cloud_url}{endpoint}"
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=body,
            headers={"Content-Type": "application/json", "User-Agent": "MayaCardio-Syncer"},
            method="POST"
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.status == 200
        except Exception as e:
            logger.debug(f"HTTP POST to {endpoint} failed: {e}")
            return False

    def stream_batch(self, session_id: str, sensor: int, first_index: int, samples: List[List[int]]) -> bool:
        """Called immediately after ingesting fresh samples in online mode."""
        if not self.mode_controller.is_online():
            return False
            
        payload = {
            "session_id": session_id,
            "sensor": sensor,
            "first_index": first_index,
            "samples": samples
        }
        ok = self._post_json("/api/stream", payload, timeout=2.0)
        if ok:
            last_synced = self.storage.get_sync_state(session_id, sensor)
            # Only advance sync state if this batch is strictly contiguous with last_synced
            # If there is a backlog gap (first_index > last_synced + 1), the catch-up worker will fill it
            if first_index <= last_synced + 1:
                new_last = first_index + len(samples) - 1
                if new_last > last_synced:
                    self.storage.update_sync_state(session_id, sensor, new_last)
            return True
        else:
            # Network blip encountered during stream
            self.mode_controller.consecutive_failures += 1
            return False


    def sync_backlog_step(self, session_id: str, sensor: int) -> int:
        """Syncs one batch of historical unsynced samples. Returns count synced."""
        unsynced = self.storage.get_unsynced_samples(session_id, sensor, limit=self.batch_size)
        if not unsynced:
            return 0
            
        payload = {
            "session_id": session_id,
            "sensor": sensor,
            "samples": unsynced
        }
        ok = self._post_json("/api/sync_batch", payload, timeout=5.0)
        if ok:
            last_idx = unsynced[-1][0]
            self.storage.update_sync_state(session_id, sensor, last_idx)
            return len(unsynced)
        return 0

    async def run_loop(self, get_current_session_id):
        self.is_running = True
        logger.info("CloudSynchronizer loop started.")
        while self.is_running:
            try:
                session_id = get_current_session_id()
                if session_id and self.mode_controller.is_online():
                    # Check and drain backlog for both sensors
                    synced_s1 = await asyncio.to_thread(self.sync_backlog_step, session_id, 1)
                    synced_s2 = await asyncio.to_thread(self.sync_backlog_step, session_id, 2)
                    
                    if synced_s1 > 0 or synced_s2 > 0:
                        logger.info(f"Caught up backlog: synced {synced_s1} s1, {synced_s2} s2 samples")
                        # If more backlog exists, loop quickly without sleep
                        continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in syncer loop: {e}")
                
            await asyncio.sleep(0.2)

    def stop(self):
        self.is_running = False
