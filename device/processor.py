"""
device/processor.py: Device-side offline window processor executing Section 3 logic when network is down.
"""
import asyncio
import logging
from common.window_manager import WindowManager
from device.storage import StorageEngine
from device.mode import ModeController

logger = logging.getLogger("device.processor")


class DeviceProcessor:
    def __init__(
        self,
        storage: StorageEngine,
        mode_controller: ModeController,
        interval: float = 0.5
    ):
        self.storage = storage
        self.mode_controller = mode_controller
        self.interval = interval
        self.window_manager = WindowManager(storage)
        self.is_running = False

    def process_session(self, session_id: str, mode: str = "offline"):
        """Synchronously processes any pending windows for the session."""
        return self.window_manager.process_pending_windows(session_id, mode=mode)

    async def run_loop(self, get_current_session_id):
        self.is_running = True
        logger.info("DeviceProcessor loop started.")
        while self.is_running:
            try:
                session_id = get_current_session_id()
                if session_id:
                    # In offline mode, device calculates windows
                    current_mode = self.mode_controller.mode
                    if current_mode == "offline":
                        results = await asyncio.to_thread(self.process_session, session_id, "offline")
                        if results:
                            logger.info(f"Offline processor generated {len(results)} window results on device")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in device processor loop: {e}")
            await asyncio.sleep(self.interval)

    def stop(self):
        self.is_running = False
