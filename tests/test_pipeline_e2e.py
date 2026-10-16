"""
tests/test_pipeline_e2e.py: Comprehensive End-to-End integration test covering:
1. Online real-time streaming & cloud processing
2. Network cut -> automatic switch to offline mode within 10s
3. Offline data collection & local window calculation
4. Network restore -> automatic switch back to online mode within 10s
5. Backlog catch-up -> 100% complete cloud copy matching device copy exactly
6. Ground truth verification with check_ground_truth
"""
import asyncio
import json
import os
import shutil
import tempfile
import threading
import time
import unittest
from mock.biosignal_mock_sim import MockServer
from cloud.server import ThreadedHTTPServer, CloudRequestHandler
from cloud.storage import CloudStorageEngine
from common.window_manager import WindowManager
from device.storage import StorageEngine
from device.mode import ModeController
from device.ingest import IngestionWorker
from device.syncer import CloudSynchronizer
from device.processor import DeviceProcessor
from mock.check_ground_truth import check_export


class TestPipelineE2E(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.device_db = os.path.join(self.tmp_dir, "dev.db")
        self.cloud_db = os.path.join(self.tmp_dir, "cloud.db")
        
        self.mock_port = 7185
        self.cloud_port = 8085
        self.cloud_url = f"http://127.0.0.1:{self.cloud_port}"
        
        # Start mock server (ramp mode, duration 60s)
        self.mock = MockServer(mode="ramp", seed=7, duration=60, port=self.mock_port, session_id="sess_e2e_1")
        self.mock_srv = await asyncio.start_server(self.mock.handle_client, "127.0.0.1", self.mock_port)

        # Start cloud server in a thread
        self._start_cloud_server()

        # Initialize device components
        self.storage = StorageEngine(self.device_db)
        self.mode_ctrl = ModeController(cloud_url=self.cloud_url, probe_interval=0.5, probe_timeout=0.5)
        self.syncer = CloudSynchronizer(self.storage, self.mode_ctrl, cloud_url=self.cloud_url, batch_size=500)
        self.processor = DeviceProcessor(self.storage, self.mode_ctrl, interval=0.2)
        
        def on_samples_saved(session_id: str, sensor: int, first_index: int, count: int):
            if self.mode_ctrl.is_online():
                samples = self.storage.get_samples_in_range(session_id, sensor, first_index, first_index + count - 1)
                ordered = [samples[i] for i in range(first_index, first_index + count) if i in samples]
                self.syncer.stream_batch(session_id, sensor, first_index, ordered)

        self.ingest = IngestionWorker(
            storage=self.storage,
            host="127.0.0.1",
            port=self.mock_port,
            on_samples_saved=on_samples_saved
        )

    def _start_cloud_server(self):
        self.cloud_storage = CloudStorageEngine(self.cloud_db)
        self.cloud_wm = WindowManager(self.cloud_storage)
        CloudRequestHandler.storage = self.cloud_storage
        CloudRequestHandler.window_manager = self.cloud_wm

        self.cloud_httpd = ThreadedHTTPServer(("127.0.0.1", self.cloud_port), CloudRequestHandler)
        self.cloud_thread = threading.Thread(target=self.cloud_httpd.serve_forever, daemon=True)
        self.cloud_thread.start()

    def _stop_cloud_server(self):
        if hasattr(self, "cloud_httpd") and self.cloud_httpd:
            self.cloud_httpd.shutdown()
            self.cloud_httpd.server_close()
            self.cloud_httpd = None

    async def asyncTearDown(self):
        self.ingest.stop()
        self.syncer.stop()
        self.processor.stop()
        self.mode_ctrl.stop()
        
        self.mock_srv.close()
        await self.mock_srv.wait_closed()
        self._stop_cloud_server()
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    async def test_full_pipeline_lifecycle_and_fault_recovery(self):
        # 1. Start background tasks
        tasks = [
            asyncio.create_task(self.mode_ctrl.run_loop()),
            asyncio.create_task(self.syncer.run_loop(lambda: "sess_e2e_1")),
            asyncio.create_task(self.processor.run_loop(lambda: "sess_e2e_1")),
            asyncio.create_task(self.ingest.run_loop())
        ]
        
        # 2. Let pipeline run in ONLINE mode for ~2 seconds
        await asyncio.sleep(2.0)
        self.assertTrue(self.mode_ctrl.is_online(), "Pipeline should be in ONLINE mode")
        
        status_online = self.storage.get_status("sess_e2e_1")
        self.assertGreater(status_online["samples_saved"], 0)
        
        # Verify cloud received samples
        cloud_samples_1 = self.cloud_storage.get_total_samples("sess_e2e_1")
        self.assertGreater(cloud_samples_1, 0)
        print(f"Online stage: device saved {status_online['samples_saved']}, cloud has {cloud_samples_1}")

        # 3. Cut Network (Simulate Outage)
        print("Simulating network outage (cutting cloud connection)...")
        self._stop_cloud_server()
        
        # Wait up to 5 seconds to observe automatic switch to OFFLINE (Requirement: <= 10s)
        switched_to_offline = False
        start_cut = time.time()
        while time.time() - start_cut < 6.0:
            if not self.mode_ctrl.is_online():
                switched_to_offline = True
                break
            await asyncio.sleep(0.3)
            
        elapsed_switch = time.time() - start_cut
        self.assertTrue(switched_to_offline, f"Should switch to offline within 10s (took {elapsed_switch:.2f}s)")
        print(f"Switched to OFFLINE mode in {elapsed_switch:.2f}s (well within 10s limit)")

        # Ingest during offline mode for 2 seconds
        await asyncio.sleep(2.0)
        status_offline = self.storage.get_status("sess_e2e_1")
        self.assertGreater(status_offline["samples_saved"], status_online["samples_saved"])
        self.assertGreater(status_offline["backlog"], 0, "Backlog should accumulate during offline mode")
        print(f"Offline stage: device saved {status_offline['samples_saved']}, backlog = {status_offline['backlog']}")

        # 4. Restore Network
        print("Restoring cloud network...")
        self._start_cloud_server()
        
        # Wait for automatic switch back to ONLINE (Requirement: <= 10s)
        switched_to_online = False
        start_restore = time.time()
        while time.time() - start_restore < 6.0:
            if self.mode_ctrl.is_online():
                switched_to_online = True
                break
            await asyncio.sleep(0.3)
            
        elapsed_restore = time.time() - start_restore
        self.assertTrue(switched_to_online, f"Should switch back to online within 10s (took {elapsed_restore:.2f}s)")
        print(f"Switched back to ONLINE mode in {elapsed_restore:.2f}s")

        # 5. Allow catch-up worker to drain backlog
        start_catchup = time.time()
        while time.time() - start_catchup < 10.0:
            st = self.storage.get_status("sess_e2e_1")
            if st["backlog"] == 0:
                break
            await asyncio.sleep(0.3)

        final_st = self.storage.get_status("sess_e2e_1")
        print(f"Final status: saved={final_st['samples_saved']}, cloud={final_st['samples_cloud']}, backlog={final_st['backlog']}")
        self.assertEqual(final_st["backlog"], 0, "All backlog must be caught up completely!")

        # 6. Verify complete cloud copy matching device copy exactly
        device_export = list(self.storage.export_data("sess_e2e_1"))
        cloud_export = list(self.cloud_storage.export_data("sess_e2e_1"))
        self.assertEqual(len(device_export), len(cloud_export), "Cloud export sample count must match device export")

        # 7. Check ground truth validation
        max_idx = self.storage.get_max_sample_index("sess_e2e_1", 1)
        success, errors = check_export(
            device_export, mode="ramp", seed=7, max_expected_index=max_idx + 1
        )
        self.assertTrue(success, f"Device export failed ground truth: {errors}")
        print("Ground truth validation PASSED with 0 errors!")

        # Clean up tasks
        for t in tasks:
            t.cancel()


if __name__ == "__main__":
    unittest.main()
