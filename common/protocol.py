"""
common/protocol.py: TCP protocol helpers, CRC-32 verification, and message serialization.
"""
import json
import zlib
from typing import Tuple, Optional, Any, Dict


def extract_data_substring(raw_line: str) -> Optional[str]:
    """
    Extracts the exact data text from [ to the matching ] corresponding to "data".
    """
    data_key_pos = raw_line.find('"data"')
    if data_key_pos == -1:
        data_key_pos = raw_line.find("'data'")
    if data_key_pos == -1:
        return None
    
    bracket_start = raw_line.find('[', data_key_pos)
    if bracket_start == -1:
        return None
    
    depth = 0
    in_string = False
    escape = False
    bracket_end = -1
    
    for idx in range(bracket_start, len(raw_line)):
        ch = raw_line[idx]
        if escape:
            escape = False
            continue
        if ch == '\\':
            escape = True
            continue
        if ch == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
            
        if ch == '[':
            depth += 1
        elif ch == ']':
            depth -= 1
            if depth == 0:
                bracket_end = idx
                break
                
    if bracket_end == -1:
        return None
    
    return raw_line[bracket_start : bracket_end + 1]


def verify_reply(raw_line: str) -> Tuple[bool, Optional[Dict[str, Any]], str]:
    """
    Validates a raw JSON reply line from biosignal-mock:
    1. Parses JSON structure.
    2. Extracts data text exactly as sent.
    3. Computes zlib CRC-32.
    4. Compares with reply['crc32'].
    
    Returns (is_valid, parsed_dict, reason).
    """
    clean_line = raw_line.strip()
    if not clean_line:
        return False, None, "empty_line"
        
    try:
        data_obj = json.loads(clean_line)
    except Exception as e:
        return False, None, f"json_decode_error: {str(e)}"
        
    expected_crc = data_obj.get("crc32")
    if expected_crc is None:
        return False, data_obj, "missing_crc32"
        
    data_text = extract_data_substring(clean_line)
    if data_text is None:
        return False, data_obj, "data_text_not_found"
        
    calc_crc = format(zlib.crc32(data_text.encode("utf-8")) & 0xFFFFFFFF, "08x")
    if calc_crc.lower() != str(expected_crc).lower().strip():
        return False, data_obj, f"crc_mismatch: got {calc_crc} expected {expected_crc}"
        
    return True, data_obj, "ok"


def format_read_cmd(sensor: int, from_index: Optional[int] = None) -> str:
    """
    Formats read command newline-delimited JSON.
    {"cmd": "read", "sensor": 1} or {"cmd": "read", "sensor": 1, "from_index": 120000}
    """
    cmd = {"cmd": "read", "sensor": sensor}
    if from_index is not None:
        cmd["from_index"] = from_index
    return json.dumps(cmd) + "\n"
