"""
tests/test_ingest.py: Integration test for IngestionWorker against MockServer.
"""
import asyncio
import os
import tempfile
import unittest
from mock.biosignal_mock_sim import MockServer
from device.storage import StorageEngine
from device.ingest import IngestionWorker


class TestIngestion(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp_dir.name, "ingest_test.db")
        self.storage = StorageEngine(self.db_path)
        
        # Start mock server on port 7199
        self.port = 7199
        self.mock = MockServer(mode="ramp", seed=7, duration=60, port=self.port, session_id="sess_ingest_1")
        self.server = await asyncio.start_server(self.mock.handle_client, "127.0.0.1", self.port)
        
        self.worker = IngestionWorker(self.storage, host="127.0.0.1", port=self.port)

    async def asyncTearDown(self):
        self.worker.stop()
        self.server.close()
        await self.server.wait_closed()
        self.tmp_dir.cleanup()

    async def test_normal_ingestion(self):
        # Allow mock to accumulate ~100 samples
        await asyncio.sleep(0.3)
        connected = await self.worker.connect()
        self.assertTrue(connected)
        
        # Poll sensor 1 and 2
        p1 = await self.worker.poll_sensor(1)
        p2 = await self.worker.poll_sensor(2)
        self.assertTrue(p1)
        self.assertTrue(p2)
        
        status = self.storage.get_status("sess_ingest_1")
        self.assertGreater(status["samples_saved"], 0)

    async def test_crc_corruption_triggers_automatic_reread(self):
        await asyncio.sleep(0.2)
        await self.worker.connect()
        
        # Tell mock to corrupt next CRC
        self.mock.corrupt_next_crc = True
        
        # Poll sensor 1 -> should detect corrupt CRC and immediately re-read successfully!
        ok = await self.worker.poll_sensor(1)
        self.assertTrue(ok)
        
        status = self.storage.get_status("sess_ingest_1")
        self.assertGreater(status["samples_saved"], 0)

    async def test_duplicate_replies_suppressed(self):
        await asyncio.sleep(0.2)
        await self.worker.connect()
        
        # Tell mock to send duplicate
        self.mock.duplicate_next_reply = True
        
        ok = await self.worker.poll_sensor(1)
        self.assertTrue(ok)
        
        # Verify deduplication
        max_idx = self.storage.get_max_sample_index("sess_ingest_1", 1)
        samples = self.storage.get_samples_in_range("sess_ingest_1", 1, 0, max_idx)
        self.assertEqual(len(samples), max_idx + 1)


if __name__ == "__main__":
    unittest.main()
