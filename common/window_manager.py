"""
common/window_manager.py: Window processing coordinator shared between edge device and cloud.
"""
from typing import Dict, List, Optional, Any
from common.processing import process_window, detect_peaks_sensor1_ch2


class WindowManager:
    def __init__(self, storage):
        self.storage = storage

    def process_pending_windows(self, session_id: str, mode: str = "offline") -> List[Dict[str, Any]]:
        """
        Processes any windows that are ready to be computed but haven't been saved yet.
        """
        last_processed = self.storage.get_last_processed_window(session_id)
        max_s1 = self.storage.get_max_sample_index(session_id, 1)
        max_s2 = self.storage.get_max_sample_index(session_id, 2)

        if max_s1 < 399 and max_s2 < 399:
            return []

        # A window w needs samples up to 400*w + 399
        # Max candidate window index is max(max_s1, max_s2) // 400
        max_avail_w = max(max_s1, max_s2) // 400
        next_w = last_processed + 1

        computed_results = []
        for w in range(next_w, max_avail_w + 1):
            w_start = 400 * w
            w_end = w_start + 399
            
            # Fetch samples for this window
            s1_win = self.storage.get_samples_in_range(session_id, 1, w_start, w_end)
            s2_win = self.storage.get_samples_in_range(session_id, 2, w_start, w_end)
            
            # Peaks and heart rate in 5-second interval [400*(w-4), 400*(w+1)-1]
            peaks_5s = []
            missing_s1_5s = 0
            if w >= 4:
                five_sec_start = 400 * (w - 4)
                five_sec_end = 400 * (w + 1) - 1
                
                # Fetch sensor 1 channel 2 samples across the 5 seconds plus buffer for peak detection
                # Peak detection for sample i requires [i-800, i-1] and i+1
                # So we query [five_sec_start - 800, five_sec_end + 1]
                s1_ecg_range = self.storage.get_samples_in_range(
                    session_id, 1, max(0, five_sec_start - 800), five_sec_end + 1
                )
                
                # Check missing sensor 1 samples in the exact 5-second interval
                count_in_5s = sum(1 for i in range(five_sec_start, five_sec_end + 1) if i in s1_ecg_range)
                missing_s1_5s = 2000 - count_in_5s
                
                if missing_s1_5s == 0:
                    # Extract channel 2 (index 1) dictionary
                    ch2_dict = {i: vals[1] for i, vals in s1_ecg_range.items() if len(vals) > 1}
                    
                    # Detect all peaks in [five_sec_start, five_sec_end]
                    all_peaks, _ = detect_peaks_sensor1_ch2(
                        ch2_dict,
                        start_idx=max(800, five_sec_start),
                        end_idx=five_sec_end
                    )
                    peaks_5s = [p for p in all_peaks if five_sec_start <= p <= five_sec_end]

            res = process_window(
                window=w,
                sensor1_samples=s1_win,
                sensor2_samples=s2_win,
                peaks_in_5s=peaks_5s,
                missing_s1_in_5s=missing_s1_5s,
                mode=mode
            )
            self.storage.save_window_result(session_id, res)
            computed_results.append(res)

        return computed_results
