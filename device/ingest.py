"""
device/ingest.py: TCP ingestion worker connecting to biosignal-mock with CRC checking,
automatic re-reads, duplicate filtering, session reset handling, and safe persistence.
"""
import asyncio
import json
import logging
from typing import Optional, Dict, Callable, Any
from common.protocol import verify_reply, format_read_cmd
from device.storage import StorageEngine

logger = logging.getLogger("device.ingest")


class IngestionWorker:
    def __init__(
        self,
        storage: StorageEngine,
        host: str = "127.0.0.1",
        port: int = 7100,
        on_samples_saved: Optional[Callable[[str, int, int, int], Any]] = None
    ):
        self.storage = storage
        self.host = host
        self.port = port
        self.on_samples_saved = on_samples_saved
        self.current_session_id: Optional[str] = None
        self.expected_index: Dict[int, int] = {1: 0, 2: 0}
        self.is_running = False
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None

    async def connect(self, max_attempts: int = 5) -> bool:
        for attempt in range(max_attempts):
            try:
                self._reader, self._writer = await asyncio.open_connection(self.host, self.port)
                return True
            except (ConnectionRefusedError, OSError) as e:
                if attempt == max_attempts - 1:
                    logger.warning(f"Connection failed after {max_attempts} attempts: {e}")
                    return False
                await asyncio.sleep(0.1)
        return False


    async def _send_cmd(self, cmd_str: str):
        if self._writer is None or self._writer.is_closing():
            raise ConnectionResetError("Socket closed")
        self._writer.write(cmd_str.encode("utf-8"))
        await self._writer.drain()

    async def _read_reply_line(self, timeout: float = 3.0) -> Optional[str]:
        if self._reader is None:
            return None
        line_bytes = await asyncio.wait_for(self._reader.readline(), timeout=timeout)
        if not line_bytes:
            raise ConnectionResetError("Connection closed by peer")
        return line_bytes.decode("utf-8")

    async def _handle_session_change(self, new_session_id: str):
        if self.current_session_id is not None and self.current_session_id != new_session_id:
            logger.warning(
                f"Mock restarted! Session changed from {self.current_session_id} to {new_session_id}"
            )
            # Record loss for unread data in old session with to_index=null
            for s in [1, 2]:
                exp = self.expected_index[s]
                self.storage.record_loss(
                    session_id=self.current_session_id,
                    sensor=s,
                    from_index=exp,
                    to_index=None,
                    reason="mock_restart"
                )
                self.expected_index[s] = 0
                
        self.current_session_id = new_session_id
        self.storage.register_session(new_session_id)
        # Restore expected index from storage if already partially recorded
        for s in [1, 2]:
            max_idx = self.storage.get_max_sample_index(new_session_id, s)
            if max_idx >= 0 and self.expected_index[s] <= max_idx:
                self.expected_index[s] = max_idx + 1

    async def poll_sensor(self, sensor: int) -> bool:
        """
        Polls next batch for the given sensor.
        Performs CRC verification and automatic re-read if corrupted.
        """
        expected = self.expected_index[sensor]
        # First attempt standard read or re-read if needed
        cmd = format_read_cmd(sensor, from_index=expected if expected > 0 else None)
        
        max_retries = 3
        for attempt in range(max_retries):
            await self._send_cmd(cmd)
            raw_line = await self._read_reply_line()
            if not raw_line:
                return False
                
            is_valid, reply_obj, reason = verify_reply(raw_line)
            if not is_valid:
                logger.warning(f"Reply corrupted ({reason}) on attempt {attempt+1}/{max_retries}. Re-reading...")
                # Request explicit re-read from expected index
                cmd = format_read_cmd(sensor, from_index=expected)
                await asyncio.sleep(0.05)
                continue
                
            # Valid reply received
            sess_id = reply_obj["session_id"]
            await self._handle_session_change(sess_id)
            
            # Record any losses reported by mock
            lost_ranges = reply_obj.get("lost", [])
            for l in lost_ranges:
                f_idx = l["from_index"]
                t_idx = l.get("to_index")
                reason_str = l.get("reason", "unknown")
                self.storage.record_loss(sess_id, sensor, f_idx, t_idx, reason_str)
                if t_idx is not None and t_idx >= self.expected_index[sensor]:
                    self.expected_index[sensor] = t_idx + 1

            first_idx = reply_obj["first_index"]
            count = reply_obj["count"]
            data = reply_obj.get("data", [])

            if count > 0 and data:
                # Save to disk
                inserted = self.storage.save_samples(sess_id, sensor, first_idx, data)
                new_next = first_idx + count
                if new_next > self.expected_index[sensor]:
                    self.expected_index[sensor] = new_next

                if self.on_samples_saved:
                    try:
                        self.on_samples_saved(sess_id, sensor, first_idx, count)
                    except Exception as e:
                        logger.error(f"Callback error in on_samples_saved: {e}")

            return True

        # All retries failed
        logger.error(f"Failed to fetch valid reply for sensor {sensor} after {max_retries} retries")
        return False

    async def run_loop(self):
        self.is_running = True
        while self.is_running:
            try:
                connected = await self.connect()
                if not connected:
                    break
                    
                while self.is_running:
                    # Poll Sensor 1 then Sensor 2
                    await self.poll_sensor(1)
                    await self.poll_sensor(2)
                    # Yield slightly to avoid burning 100% CPU when buffer empty
                    await asyncio.sleep(0.05)
            except (ConnectionResetError, asyncio.TimeoutError, OSError) as e:
                logger.warning(f"Mock connection disconnected: {e}. Reconnecting...")
                if self._writer:
                    try:
                        self._writer.close()
                    except Exception:
                        pass
                await asyncio.sleep(0.5)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"Unexpected ingestion error: {e}")
                await asyncio.sleep(1.0)

    def stop(self):
        self.is_running = False
        if self._writer:
            try:
                self._writer.close()
            except Exception:
                pass
