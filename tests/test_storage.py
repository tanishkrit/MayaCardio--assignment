"""
tests/test_storage.py: Unit tests for SQLite storage engine.
"""
import os
import unittest
import tempfile
import json
from device.storage import StorageEngine


class TestStorage(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp_dir.name, "test_pipeline.db")
        self.storage = StorageEngine(self.db_path)

    def tearDown(self):
        self.tmp_dir.cleanup()

    def test_save_samples_and_deduplication(self):
        session_id = "test_sess_1"
        self.storage.register_session(session_id)
        
        # Insert 10 samples for sensor 1
        samples = [[i, i*2, i*3] for i in range(10)]
        inserted = self.storage.save_samples(session_id, sensor=1, first_index=0, samples=samples)
        self.assertEqual(inserted, 10)
        
        # Query max index
        max_idx = self.storage.get_max_sample_index(session_id, sensor=1)
        self.assertEqual(max_idx, 9)

        # Attempt to insert same samples again -> deduplication must ignore duplicates
        reinserted = self.storage.save_samples(session_id, sensor=1, first_index=0, samples=samples)
        self.assertEqual(reinserted, 0)
        
        # Verify samples count in range
        data = self.storage.get_samples_in_range(session_id, sensor=1, start_idx=0, end_idx=9)
        self.assertEqual(len(data), 10)
        self.assertEqual(data[0], [0, 0, 0])
        self.assertEqual(data[5], [5, 10, 15])

    def test_record_losses_and_export(self):
        session_id = "test_sess_2"
        self.storage.register_session(session_id)
        
        # Save sample
        self.storage.save_samples(session_id, sensor=1, first_index=0, samples=[[10, 20, 30]])
        # Record loss
        self.storage.record_loss(session_id, sensor=2, from_index=100, to_index=150, reason="buffer_overflow")
        
        export_lines = list(self.storage.export_data(session_id))
        self.assertEqual(len(export_lines), 2)
        
        sample_line = json.loads(export_lines[0])
        self.assertEqual(sample_line["sensor"], 1)
        self.assertEqual(sample_line["index"], 0)
        self.assertEqual(sample_line["values"], [10, 20, 30])
        
        loss_line = json.loads(export_lines[1])
        self.assertEqual(loss_line["sensor"], 2)
        self.assertEqual(loss_line["from_index"], 100)
        self.assertEqual(loss_line["to_index"], 150)
        self.assertEqual(loss_line["reason"], "buffer_overflow")

    def test_sync_state_and_backlog(self):
        session_id = "test_sess_3"
        self.storage.register_session(session_id)
        
        # Save 20 samples for sensor 1 and 20 for sensor 2
        s1 = [[i, i, i] for i in range(20)]
        s2 = [[i, i] for i in range(20)]
        self.storage.save_samples(session_id, 1, 0, s1)
        self.storage.save_samples(session_id, 2, 0, s2)
        
        # Initially sync state is -1 for both
        status = self.storage.get_status(session_id)
        self.assertEqual(status["samples_saved"], 40)
        self.assertEqual(status["samples_cloud"], 0)
        self.assertEqual(status["backlog"], 40)
        
        # Fetch unsynced for sensor 1 (limit 10)
        unsynced = self.storage.get_unsynced_samples(session_id, sensor=1, limit=10)
        self.assertEqual(len(unsynced), 10)
        self.assertEqual(unsynced[0][0], 0)
        self.assertEqual(unsynced[-1][0], 9)
        
        # Update sync state to 9
        self.storage.update_sync_state(session_id, sensor=1, last_synced_index=9)
        
        # Next unsynced batch should start from 10
        next_unsynced = self.storage.get_unsynced_samples(session_id, sensor=1, limit=10)
        self.assertEqual(len(next_unsynced), 10)
        self.assertEqual(next_unsynced[0][0], 10)


if __name__ == "__main__":
    unittest.main()
