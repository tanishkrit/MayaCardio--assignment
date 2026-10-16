"""
common/processing.py: Biosignal Processing Engine according to Section 3 specifications.
"""
from typing import Dict, List, Optional, Any, Tuple
import math


def detect_peaks_sensor1_ch2(
    samples_s1_c2: Dict[int, int],
    start_idx: int,
    end_idx: int,
    last_peak_idx: Optional[int] = None
) -> Tuple[List[int], Optional[int]]:
    """
    Detect peaks in Sensor 1 Channel 2 (ECG) within the range [start_idx, end_idx].
    
    A sample i is a peak when:
    1. The 800 samples before it [i-800, i-1] and the sample after it (i+1) are available.
    2. val[i] > val[i-1] and val[i] > val[i+1].
    3. val[i] > 0.5 * max(val[i-800 ... i-1]).
    4. i >= previous_peak + 100 (or no previous peak).
    
    Returns (list_of_peaks_in_range, updated_last_peak_idx).
    """
    peaks = []
    current_last_peak = last_peak_idx

    for i in range(start_idx, end_idx + 1):
        if i < 800:
            continue
        
        # Check if sample i, sample i+1, and sample i-1 are available
        if i not in samples_s1_c2 or (i + 1) not in samples_s1_c2 or (i - 1) not in samples_s1_c2:
            continue
        
        val_i = samples_s1_c2[i]
        
        # Condition 2: larger than both neighbors
        if val_i <= samples_s1_c2[i - 1] or val_i <= samples_s1_c2[i + 1]:
            continue
        
        # Condition 4: at least 100 samples after previous peak
        if current_last_peak is not None and (i - current_last_peak) < 100:
            continue
        
        # Condition 1: 800 samples before it must all be available
        # Find max of 800 samples before it
        all_800_avail = True
        max_prior = -float("inf")
        for j in range(i - 800, i):
            if j not in samples_s1_c2:
                all_800_avail = False
                break
            v = samples_s1_c2[j]
            if v > max_prior:
                max_prior = v
        
        if not all_800_avail:
            continue
        
        # Condition 3: larger than half of the max of the 800 samples before it
        if val_i <= 0.5 * max_prior:
            continue
        
        # Peak confirmed!
        peaks.append(i)
        current_last_peak = i

    return peaks, current_last_peak


def compute_heart_rate(
    window: int,
    peaks_in_5s: List[int],
    missing_samples_in_5s: int
) -> Optional[float]:
    """
    Compute heart rate for window w:
    - null if w < 4
    - null if any sample in last 5 seconds (400(w-4) to 400(w+1)-1) is missing
    - null if fewer than 2 peaks in last 5 seconds
    - heart_rate = 60 / (mean gap between consecutive peaks, in seconds)
    - rounded to 1 decimal place
    """
    if window < 4:
        return None
    if missing_samples_in_5s > 0:
        return None
    if len(peaks_in_5s) < 2:
        return None
    
    # Calculate gaps in seconds
    gaps_sec = [(peaks_in_5s[k + 1] - peaks_in_5s[k]) / 400.0 for k in range(len(peaks_in_5s) - 1)]
    mean_gap_sec = sum(gaps_sec) / len(gaps_sec)
    
    if mean_gap_sec <= 0:
        return None
        
    rate = 60.0 / mean_gap_sec
    return round(rate, 1)


def process_window(
    window: int,
    sensor1_samples: Dict[int, List[int]],
    sensor2_samples: Dict[int, List[int]],
    peaks_in_5s: List[int],
    missing_s1_in_5s: int,
    mode: str = "offline"
) -> Dict[str, Any]:
    """
    Process a single 1-second window w (samples 400*w to 400*w + 399).
    
    Outputs:
    - missing_1, missing_2
    - complete
    - mean, min, max (per channel, null if no samples)
    - magnitude (mean of sqrt(ch1^2 + ch2^2) over sensor 2 samples)
    - heart_rate
    - mode
    """
    w_start = 400 * window
    w_end = w_start + 399
    
    # Filter available samples in window w
    s1_in_window = [sensor1_samples[i] for i in range(w_start, w_end + 1) if i in sensor1_samples]
    s2_in_window = [sensor2_samples[i] for i in range(w_start, w_end + 1) if i in sensor2_samples]
    
    missing_1 = 400 - len(s1_in_window)
    missing_2 = 400 - len(s2_in_window)
    complete = (missing_1 == 0 and missing_2 == 0)
    
    # Sensor 1 statistics (3 channels)
    if len(s1_in_window) > 0:
        s1_mean = [
            round(sum(sample[c] for sample in s1_in_window) / len(s1_in_window), 1)
            for c in range(3)
        ]
        s1_min = [min(sample[c] for sample in s1_in_window) for c in range(3)]
        s1_max = [max(sample[c] for sample in s1_in_window) for c in range(3)]
    else:
        s1_mean = None
        s1_min = None
        s1_max = None
        
    # Sensor 2 statistics (2 channels)
    if len(s2_in_window) > 0:
        s2_mean = [
            round(sum(sample[c] for sample in s2_in_window) / len(s2_in_window), 1)
            for c in range(2)
        ]
        s2_min = [min(sample[c] for sample in s2_in_window) for c in range(2)]
        s2_max = [max(sample[c] for sample in s2_in_window) for c in range(2)]
        
        # Magnitude: Mean of √(ch1² + ch2²) over available sensor 2 samples
        mags = [math.hypot(sample[0], sample[1]) for sample in s2_in_window]
        magnitude = round(sum(mags) / len(mags), 1)
    else:
        s2_mean = None
        s2_min = None
        s2_max = None
        magnitude = None
        
    heart_rate = compute_heart_rate(window, peaks_in_5s, missing_s1_in_5s)
    
    # Format per channel structures
    # Provide both clean dict and sensor maps for full compatibility
    mean_out = {"1": s1_mean, "2": s2_mean}
    min_out = {"1": s1_min, "2": s2_min}
    max_out = {"1": s1_max, "2": s2_max}
    
    return {
        "window": window,
        "mode": mode,
        "complete": complete,
        "missing_1": missing_1,
        "missing_2": missing_2,
        "mean": mean_out,
        "min": min_out,
        "max": max_out,
        "magnitude": magnitude,
        "heart_rate": heart_rate
    }
