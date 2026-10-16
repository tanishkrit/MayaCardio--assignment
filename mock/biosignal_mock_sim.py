"""
mock/biosignal_mock_sim.py: Mock biosignal service matching Section 6 specification.
"""
import argparse
import asyncio
import json
import math
import random
import time
import zlib
from typing import Dict, List, Tuple, Optional


class BiosignalGenerator:
    def __init__(self, mode: str = "signal", seed: int = 7):
        self.mode = mode
        self.seed = seed
        self.rng = random.Random(seed)
        
    def get_sample_s1(self, i: int) -> List[int]:
        if self.mode == "ramp":
            c0 = (3 * i + 0) % 8388608
            c1 = (3 * i + 1) % 8388608
            c2 = (3 * i + 2) % 8388608
            return [c0, c1, c2]
            
        # Signal mode: realistic synthetic data
        # Heart rate repeats every 6 minutes (360 seconds = 144,000 samples)
        cycle_sample = i % 144000
        sec_in_cycle = (cycle_sample / 400.0)
        
        if sec_in_cycle < 120.0:
            # 60 bpm: 1 beat per sec = period 400 samples
            period = 400.0
            phase = cycle_sample % int(period)
        elif sec_in_cycle < 240.0:
            # 90 bpm: 1.5 beats per sec = period 266.6667 samples
            # offset from sample 48000
            s_rel = cycle_sample - 48000
            period = 400.0 / 1.5  # 266.6666...
            phase = s_rel % period
        else:
            # 72 bpm: 1.2 beats per sec = period 333.3333 samples
            s_rel = cycle_sample - 96000
            period = 400.0 / 1.2  # 333.3333...
            phase = s_rel % period

        # Synthetic ECG wave for ch2 (ECG)
        # Baseline = 200
        # Peak (R) occurs at phase around 0.2 * period
        r_center = 0.2 * period
        diff = phase - r_center
        
        # Sharp R peak: Gaussian with amplitude ~2400
        r_peak = 2400.0 * math.exp(-0.5 * (diff / 2.5) ** 2)
        # Small Q dip right before R
        q_dip = -250.0 * math.exp(-0.5 * ((diff + 6.0) / 3.0) ** 2)
        # Small S dip right after R
        s_dip = -350.0 * math.exp(-0.5 * ((diff - 6.0) / 4.0) ** 2)
        # T wave later
        t_wave = 350.0 * math.exp(-0.5 * ((diff - 45.0) / 18.0) ** 2)
        # P wave before Q
        p_wave = 180.0 * math.exp(-0.5 * ((diff + 35.0) / 12.0) ** 2)
        
        ch2_val = int(round(200.0 + r_peak + q_dip + s_dip + t_wave + p_wave))
        ch1_val = int(round(ch2_val * 0.6))
        ch3_val = int(round(ch2_val * 1.3))
        
        return [ch1_val, ch2_val, ch3_val]

    def get_sample_s2(self, i: int) -> List[int]:
        if self.mode == "ramp":
            c0 = (2 * i + 0) % 8388608
            c1 = (2 * i + 1) % 8388608
            return [c0, c1]
            
        # Sensor 2: channel 1 near 400,000, channel 2 near 150,000, varying slowly
        c0 = int(round(400000 + 500 * math.sin(2 * math.pi * i / 4000.0)))
        c1 = int(round(150000 + 300 * math.cos(2 * math.pi * i / 4000.0)))
        return [c0, c1]


class MockServer:
    def __init__(
        self,
        mode: str = "signal",
        seed: int = 7,
        duration: int = 3600,
        port: int = 7100,
        session_id: Optional[str] = None
    ):
        self.mode = mode
        self.seed = seed
        self.duration = duration
        self.port = port
        self.session_id = session_id or f"{random.randint(0x10000000, 0xffffffff):08x}"
        self.generator = BiosignalGenerator(mode, seed)
        self.start_time = None
        self.last_delivered: Dict[int, int] = {1: 0, 2: 0} # sensor -> next index
        self.corrupt_next_crc = False
        self.duplicate_next_reply = False
        self.injected_losses: Dict[int, List[Dict[str, Any]]] = {1: [], 2: []}
        self.is_running = True

    def get_current_sample_index(self) -> int:
        if self.start_time is None:
            return 0
        elapsed = time.time() - self.start_time
        curr = int(elapsed * 400)
        max_samples = self.duration * 400
        return min(curr, max_samples)

    def generate_reply(
        self, sensor: int, from_index: Optional[int]
    ) -> Tuple[str, Optional[str]]:
        current_idx = self.get_current_sample_index()
        buffer_window = 4000  # 10 seconds buffer
        buffer_start = max(0, current_idx - buffer_window)

        requested_idx = from_index if from_index is not None else self.last_delivered[sensor]
        lost_ranges = []

        # If requested_idx is behind the 10-second buffer
        if requested_idx < buffer_start:
            lost_ranges.append({
                "from_index": requested_idx,
                "to_index": buffer_start - 1,
                "reason": "buffer_overflow"
            })
            start_fetch = buffer_start
        else:
            start_fetch = requested_idx

        # Check injected losses
        if self.injected_losses[sensor]:
            lost_ranges.extend(self.injected_losses[sensor])
            self.injected_losses[sensor] = []

        # At most 1 second of data (400 samples)
        count = min(400, max(0, current_idx - start_fetch))
        samples = []
        for idx in range(start_fetch, start_fetch + count):
            if sensor == 1:
                samples.append(self.generator.get_sample_s1(idx))
            else:
                samples.append(self.generator.get_sample_s2(idx))

        self.last_delivered[sensor] = start_fetch + count
        session_end = (start_fetch + count >= self.duration * 400)

        data_text = json.dumps(samples, separators=(',', ':'))
        if self.corrupt_next_crc:
            crc32_val = "deadbeef"
            self.corrupt_next_crc = False
        else:
            crc32_val = format(zlib.crc32(data_text.encode("utf-8")) & 0xFFFFFFFF, "08x")

        reply_obj = {
            "session_id": self.session_id,
            "sensor": sensor,
            "first_index": start_fetch,
            "count": count,
            "lost": lost_ranges,
            "session_end": session_end,
            "crc32": crc32_val
        }
        
        reply_json = (
            f'{{"session_id": "{reply_obj["session_id"]}", '
            f'"sensor": {reply_obj["sensor"]}, '
            f'"first_index": {reply_obj["first_index"]}, '
            f'"count": {reply_obj["count"]}, '
            f'"lost": {json.dumps(reply_obj["lost"])}, '
            f'"session_end": {str(reply_obj["session_end"]).lower()}, '
            f'"crc32": "{reply_obj["crc32"]}", '
            f'"data": {data_text}}}\n'
        )

        dup_reply = None
        if self.duplicate_next_reply:
            dup_reply = reply_json
            self.duplicate_next_reply = False

        return reply_json, dup_reply

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        if self.start_time is None:
            self.start_time = time.time()
            
        try:
            while self.is_running and not reader.at_eof():
                line = await reader.readline()
                if not line:
                    break
                text = line.decode("utf-8").strip()
                if not text:
                    continue
                try:
                    cmd_obj = json.loads(text)
                except Exception:
                    continue

                if cmd_obj.get("cmd") == "read":
                    sensor = int(cmd_obj.get("sensor", 1))
                    from_idx = cmd_obj.get("from_index")
                    reply, dup = self.generate_reply(sensor, from_idx)
                    writer.write(reply.encode("utf-8"))
                    if dup:
                        writer.write(dup.encode("utf-8"))
                    await writer.drain()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            import traceback
            traceback.print_exc()
        finally:
            writer.close()



async def run_server(mode="signal", seed=7, duration=3600, port=7100):
    server = MockServer(mode=mode, seed=seed, duration=duration, port=port)
    srv = await asyncio.start_server(server.handle_client, "127.0.0.1", port)
    print(f"Mock service started on port {port} (mode={mode}, seed={seed}, session={server.session_id})")
    async with srv:
        await srv.serve_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="biosignal-mock simulator")
    parser.add_argument("--mode", default="signal", choices=["signal", "ramp"])
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--duration", type=int, default=3600)
    parser.add_argument("--port", type=int, default=7100)
    args = parser.parse_args()

    asyncio.run(run_server(args.mode, args.seed, args.duration, args.port))
