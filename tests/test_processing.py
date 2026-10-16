"""
tests/test_processing.py: Unit tests for processing logic and ECG heart-rate rules.
"""
import unittest
import math
from common.processing import detect_peaks_sensor1_ch2, compute_heart_rate, process_window


class TestProcessing(unittest.TestCase):
    def test_window_statistics_complete(self):
        # 400 samples of sensor 1: channel 0 = 10, channel 1 = 20, channel 2 = 30
        sensor1_samples = {
            i: [10, 20, 30] for i in range(400)
        }
        # 400 samples of sensor 2: channel 0 = 300, channel 1 = 400 (hypot = 500.0)
        sensor2_samples = {
            i: [300, 400] for i in range(400)
        }
        res = process_window(
            window=0,
            sensor1_samples=sensor1_samples,
            sensor2_samples=sensor2_samples,
            peaks_in_5s=[],
            missing_s1_in_5s=0,
            mode="offline"
        )
        self.assertTrue(res["complete"])
        self.assertEqual(res["missing_1"], 0)
        self.assertEqual(res["missing_2"], 0)
        self.assertEqual(res["mean"]["1"], [10.0, 20.0, 30.0])
        self.assertEqual(res["min"]["1"], [10, 20, 30])
        self.assertEqual(res["max"]["1"], [10, 20, 30])
        self.assertEqual(res["mean"]["2"], [300.0, 400.0])
        self.assertEqual(res["magnitude"], 500.0)
        self.assertIsNone(res["heart_rate"]) # w=0 < 4

    def test_window_statistics_incomplete(self):
        # Only 200 samples in window 0
        sensor1_samples = {i: [10, 20, 30] for i in range(200)}
        sensor2_samples = {i: [300, 400] for i in range(300)}
        res = process_window(
            window=0,
            sensor1_samples=sensor1_samples,
            sensor2_samples=sensor2_samples,
            peaks_in_5s=[],
            missing_s1_in_5s=0,
            mode="online"
        )
        self.assertFalse(res["complete"])
        self.assertEqual(res["missing_1"], 200)
        self.assertEqual(res["missing_2"], 100)
        self.assertEqual(res["mode"], "online")

    def test_peak_detection_rules(self):
        # Build baseline signal of 1000 samples with value 1000
        signal = {i: 1000 for i in range(1500)}
        
        # Test 1: peak candidate at 850 with value 2000
        # prior 800 max is 1000, 0.5 * 1000 = 500. 2000 > 500.
        signal[849] = 1000
        signal[850] = 2000
        signal[851] = 1000
        
        peaks, last_peak = detect_peaks_sensor1_ch2(signal, 0, 1000)
        self.assertIn(850, peaks)
        self.assertEqual(last_peak, 850)
        
        # Test 2: candidate at 900 (within 100 samples from 850)
        signal[899] = 1000
        signal[900] = 2500
        signal[901] = 1000
        peaks2, _ = detect_peaks_sensor1_ch2(signal, 851, 950, last_peak_idx=850)
        self.assertNotIn(900, peaks2) # Refractory period violation
        
        # Test 3: candidate at 951 (101 samples after 850)
        signal[950] = 1000
        signal[951] = 2500
        signal[952] = 1000
        peaks3, last_peak3 = detect_peaks_sensor1_ch2(signal, 851, 1000, last_peak_idx=850)
        self.assertIn(951, peaks3)
        self.assertEqual(last_peak3, 951)

    def test_heart_rate_calculation(self):
        # 60 bpm: peaks every 400 samples (1.0 sec)
        # In 5-second interval [0, 1999], say peaks at 400, 800, 1200, 1600
        peaks_60bpm = [400, 800, 1200, 1600]
        hr_60 = compute_heart_rate(window=4, peaks_in_5s=peaks_60bpm, missing_samples_in_5s=0)
        self.assertEqual(hr_60, 60.0)
        
        # 90 bpm: peaks every 266.67 samples (gap = 266.6667 / 400 = 0.66667 s)
        peaks_90bpm = [400, 667, 933, 1200, 1467, 1733]
        hr_90 = compute_heart_rate(window=4, peaks_in_5s=peaks_90bpm, missing_samples_in_5s=0)
        self.assertAlmostEqual(hr_90, 90.0, places=0)

        # Missing sample in 5s window => heart_rate must be null
        hr_missing = compute_heart_rate(window=4, peaks_in_5s=peaks_60bpm, missing_samples_in_5s=1)
        self.assertIsNone(hr_missing)
        
        # w < 4 => null
        hr_w3 = compute_heart_rate(window=3, peaks_in_5s=peaks_60bpm, missing_samples_in_5s=0)
        self.assertIsNone(hr_w3)
        
        # Fewer than 2 peaks => null
        hr_single = compute_heart_rate(window=4, peaks_in_5s=[800], missing_samples_in_5s=0)
        self.assertIsNone(hr_single)


if __name__ == "__main__":
    unittest.main()
