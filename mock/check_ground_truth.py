"""
mock/check_ground_truth.py: Validator tool that compares a pipeline export against ground truth.
"""
import argparse
import json
import sys
from typing import Dict, List, Set, Tuple
from mock.biosignal_mock_sim import BiosignalGenerator


def check_export(
    export_lines: List[str],
    mode: str = "signal",
    seed: int = 7,
    max_expected_index: int = 400
) -> Tuple[bool, List[str]]:
    generator = BiosignalGenerator(mode=mode, seed=seed)
    
    samples_found: Dict[Tuple[int, int], List[int]] = {}
    losses: List[Dict] = []
    errors: List[str] = []
    
    for line_idx, line in enumerate(export_lines):
        line = line.strip()
        if not line:
            continue
        try:
            item = json.loads(line)
        except Exception as e:
            errors.append(f"Line {line_idx}: invalid JSON: {e}")
            continue
            
        if "index" in item:
            # Sample line: {"sensor": 1, "index": 120000, "values": [720, 1200, 1560]}
            sensor = item.get("sensor")
            idx = item.get("index")
            values = item.get("values")
            
            key = (sensor, idx)
            if key in samples_found:
                errors.append(f"Duplicate sample: sensor {sensor}, index {idx}")
            else:
                samples_found[key] = values
                
                # Verify exact sample value against ground truth
                if sensor == 1:
                    expected = generator.get_sample_s1(idx)
                elif sensor == 2:
                    expected = generator.get_sample_s2(idx)
                else:
                    expected = None
                    errors.append(f"Unknown sensor {sensor}")
                    
                if expected is not None and values != expected:
                    errors.append(f"Value mismatch at sensor {sensor}, index {idx}: got {values}, expected {expected}")
        elif "from_index" in item:
            # Loss line: {"sensor": 2, "from_index": 4000, "to_index": 4399, "reason": "..."}
            losses.append(item)
        else:
            errors.append(f"Unrecognized export line structure: {item}")
            
    # Check for missing indices without reported loss
    max_s = {}
    for s in [1, 2]:
        indices = [idx for (sensor_id, idx) in samples_found if sensor_id == s]
        max_s[s] = max(indices) if indices else -1

    for sensor in [1, 2]:
        limit = min(max_expected_index, max_s[sensor] + 1) if max_expected_index is not None else (max_s[sensor] + 1)
        for idx in range(limit):
            if (sensor, idx) not in samples_found:
                # Must be accounted for in losses
                accounted = False
                for l in losses:
                    if l.get("sensor") == sensor:
                        f_idx = l.get("from_index")
                        t_idx = l.get("to_index")
                        if t_idx is None:
                            if idx >= f_idx:
                                accounted = True
                                break
                        else:
                            if f_idx <= idx <= t_idx:
                                accounted = True
                                break
                if not accounted:
                    errors.append(f"Silent data loss: sensor {sensor}, index {idx} neither present nor accounted in losses")

                    
    success = (len(errors) == 0)
    return success, errors


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check export file against ground truth")
    parser.add_argument("export_file", help="Path to export file with JSON lines")
    parser.add_argument("--mode", default="signal", choices=["signal", "ramp"])
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--max-index", type=int, default=400)
    args = parser.parse_args()

    with open(args.export_file, "r", encoding="utf-8") as f:
        lines = f.readlines()

    success, errs = check_export(lines, mode=args.mode, seed=args.seed, max_expected_index=args.max_index)
    if success:
        print(f"PASSED: 100% ground truth match, zero silent loss, zero duplicate samples.")
        sys.exit(0)
    else:
        print(f"FAILED with {len(errs)} errors:")
        for e in errs[:20]:
            print(" -", e)
        sys.exit(1)
