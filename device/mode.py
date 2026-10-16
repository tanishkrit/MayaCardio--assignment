"""
device/mode.py: Automatic Online/Offline mode controller with fast health probing (< 10s switch SLA).
"""
import asyncio
import logging
import time
import urllib.request
import urllib.error
from typing import Optional, Callable, Any

logger = logging.getLogger("device.mode")


class ModeController:
    def __init__(
        self,
        cloud_url: str = "http://127.0.0.1:8080",
        probe_interval: float = 1.5,
        probe_timeout: float = 1.5,
        on_mode_change: Optional[Callable[[str], Any]] = None
    ):
        self.cloud_url = cloud_url.rstrip("/")
        self.probe_interval = probe_interval
        self.probe_timeout = probe_timeout
        self.on_mode_change = on_mode_change
        
        self.mode = "offline"  # Initial safe mode until cloud connectivity proven
        self.consecutive_failures = 0
        self.consecutive_successes = 0
        self.is_running = False

    def is_online(self) -> bool:
        return self.mode == "online"

    def check_cloud_health(self) -> bool:
        health_url = f"{self.cloud_url}/api/health"
        try:
            req = urllib.request.Request(health_url, headers={"User-Agent": "MayaCardio-Pipeline"})
            with urllib.request.urlopen(req, timeout=self.probe_timeout) as resp:
                return resp.status == 200
        except Exception:
            return False

    async def _probe_step(self):
        # Run blocking HTTP health probe in thread pool to not stall asyncio loop
        healthy = await asyncio.to_thread(self.check_cloud_health)
        
        if healthy:
            self.consecutive_successes += 1
            self.consecutive_failures = 0
            if self.mode != "online" and self.consecutive_successes >= 1:
                logger.info("Cloud reachable! Switching mode: OFFLINE -> ONLINE")
                self.mode = "online"
                if self.on_mode_change:
                    try:
                        self.on_mode_change("online")
                    except Exception as e:
                        logger.error(f"Error in on_mode_change callback: {e}")
        else:
            self.consecutive_failures += 1
            self.consecutive_successes = 0
            # Switch to offline after 2 failed probes (~3 seconds total)
            if self.mode != "offline" and self.consecutive_failures >= 2:
                logger.warning("Cloud unreachable! Switching mode: ONLINE -> OFFLINE")
                self.mode = "offline"
                if self.on_mode_change:
                    try:
                        self.on_mode_change("offline")
                    except Exception as e:
                        logger.error(f"Error in on_mode_change callback: {e}")

    async def run_loop(self):
        self.is_running = True
        logger.info(f"ModeController started probing {self.cloud_url} every {self.probe_interval}s")
        while self.is_running:
            try:
                await self._probe_step()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in mode probe step: {e}")
            await asyncio.sleep(self.probe_interval)

    def stop(self):
        self.is_running = False
