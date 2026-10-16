"""
tests/test_mock_sim.py: Tests for mock simulator generator and check_ground_truth validator.
"""
import unittest
import json
from mock.biosignal_mock_sim import BiosignalGenerator
from mock.check_ground_truth import check_export
from common.protocol import extract_data_substring, verify_reply


class TestMockSimAndValidator(unittest.TestCase):
    def test_ramp_mode_generator(self):
        gen = BiosignalGenerator(mode="ramp", seed=7)
        s1 = gen.get_sample_s1(10)
        # s1: (3*10 + c) % 8388608 -> [30, 31, 32]
        self.assertEqual(s1, [30, 31, 32])
        
        s2 = gen.get_sample_s2(10)
        # s2: (2*10 + c) % 8388608 -> [20, 21]
        self.assertEqual(s2, [20, 21])

    def test_validator_clean_export(self):
        gen = BiosignalGenerator(mode="ramp", seed=7)
        lines = []
        for i in range(10):
            lines.append(json.dumps({"sensor": 1, "index": i, "values": gen.get_sample_s1(i)}))
            lines.append(json.dumps({"sensor": 2, "index": i, "values": gen.get_sample_s2(i)}))
            
        success, errors = check_export(lines, mode="ramp", seed=7, max_expected_index=10)
        self.assertTrue(success)
        self.assertEqual(len(errors), 0)

    def test_validator_detects_corrupt_value(self):
        gen = BiosignalGenerator(mode="ramp", seed=7)
        lines = []
        for i in range(5):
            val1 = gen.get_sample_s1(i)
            if i == 2:
                val1 = [999, 999, 999]  # Corrupted!
            lines.append(json.dumps({"sensor": 1, "index": i, "values": val1}))
            lines.append(json.dumps({"sensor": 2, "index": i, "values": gen.get_sample_s2(i)}))
            
        success, errors = check_export(lines, mode="ramp", seed=7, max_expected_index=5)
        self.assertFalse(success)
        self.assertTrue(any("Value mismatch" in e for e in errors))

    def test_validator_detects_silent_loss_and_allows_reported_loss(self):
        gen = BiosignalGenerator(mode="ramp", seed=7)
        lines = []
        # Index 0, 1 present, index 2 missing, index 3 present
        for i in [0, 1, 3]:
            lines.append(json.dumps({"sensor": 1, "index": i, "values": gen.get_sample_s1(i)}))
            lines.append(json.dumps({"sensor": 2, "index": i, "values": gen.get_sample_s2(i)}))
            
        # First: missing without report -> fails
        success, errors = check_export(lines, mode="ramp", seed=7, max_expected_index=4)
        self.assertFalse(success)
        self.assertTrue(any("Silent data loss" in e for e in errors))

        # Second: add honest loss report for index 2
        lines.append(json.dumps({"sensor": 1, "from_index": 2, "to_index": 2, "reason": "buffer_overflow"}))
        lines.append(json.dumps({"sensor": 2, "from_index": 2, "to_index": 2, "reason": "buffer_overflow"}))
        success2, errors2 = check_export(lines, mode="ramp", seed=7, max_expected_index=4)
        self.assertTrue(success2)
        self.assertEqual(len(errors2), 0)

    def test_crc_verification(self):
        valid_line = (
            '{"session_id": "test_sess", "sensor": 1, "first_index": 0, "count": 2, '
            '"lost": [], "session_end": false, "crc32": "d0e199f1", '
            '"data": [[10, 20, 30], [11, 21, 31]]}'
        )
        # Compute exact crc of '[[10, 20, 30], [11, 21, 31]]'
        import zlib
        dt = "[[10, 20, 30], [11, 21, 31]]"
        crc = format(zlib.crc32(dt.encode("utf-8")) & 0xFFFFFFFF, "08x")
        valid_line = valid_line.replace("d0e199f1", crc)
        
        ok, parsed, reason = verify_reply(valid_line)
        self.assertTrue(ok)
        self.assertEqual(reason, "ok")
        self.assertEqual(parsed["sensor"], 1)

        # Corrupt line
        corrupt_line = valid_line.replace(crc, "badcrc00")
        ok2, parsed2, reason2 = verify_reply(corrupt_line)
        self.assertFalse(ok2)
        self.assertTrue("crc_mismatch" in reason2)


if __name__ == "__main__":
    unittest.main()
