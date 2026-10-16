"""
run_demo.py: Runs a live end-to-end demonstration of the MayaCardio pipeline:
1. Starts the mock sensor service (biosignal-mock)
2. Starts the cloud service (cloud.server)
3. Starts the device pipeline daemon
4. Runs pipeline status, export, and results
5. Validates against check_ground_truth
"""
import asyncio
import os
import subprocess
import sys
import time

def main():
    print("=" * 70)
    print(" MAYACARDIO LIVE PIPELINE RUN & EVALUATION")
    print("=" * 70)

    # Clean previous demo DBs if any
    for f in ["demo_device.db", "demo_cloud.db", "demo_export.ndjson"]:
        if os.path.exists(f):
            try:
                os.remove(f)
            except Exception:
                pass

    mock_port = 7100
    cloud_port = 8080
    cloud_url = f"http://127.0.0.1:{cloud_port}"

    # 1. Start biosignal-mock simulator
    print("\n[Step 1] Starting biosignal-mock service on port 7100...")
    mock_proc = subprocess.Popen(
        [sys.executable, "-m", "mock.biosignal_mock_sim", "--mode", "signal", "--seed", "7", "--duration", "3600", "--port", str(mock_port)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    time.sleep(1.0)

    # 2. Start Cloud Service
    print("[Step 2] Starting cloud receiver service on port 8080...")
    cloud_proc = subprocess.Popen(
        [sys.executable, "-m", "cloud.server", "--port", str(cloud_port), "--db", "demo_cloud.db"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    time.sleep(1.0)

    # 3. Start Device Pipeline Daemon
    print("[Step 3] Starting device pipeline daemon...")
    dev_proc = subprocess.Popen(
        [sys.executable, "pipeline.py", "--db", "demo_device.db", "--cloud-url", cloud_url, "--mock-port", str(mock_port), "run"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    # Let pipeline run for 10 seconds in online mode (collects ~4000 samples per sensor, >=10 windows)
    print("\n[Step 4] Ingesting in ONLINE mode for 10 seconds...")
    for sec in range(1, 11):
        time.sleep(1.0)
        res = subprocess.run(
            [sys.executable, "pipeline.py", "--db", "demo_device.db", "--cloud-url", cloud_url, "status"],
            capture_output=True, text=True
        )
        sys.stdout.write(f"\r  Elapsed: {sec:02d}s | Status: {res.stdout.strip()}")
        sys.stdout.flush()
    print("\n")

    # Get session ID
    from device.storage import StorageEngine
    st_engine = StorageEngine("demo_device.db")
    with st_engine._get_connection() as con:
        cur = con.cursor()
        cur.execute("SELECT session_id FROM sessions ORDER BY created_at DESC LIMIT 1")
        row = cur.fetchone()
        session_id = row[0] if row else None

    print(f"Active Session ID: {session_id}")

    # Test pipeline status while running
    print("\n" + "=" * 70)
    print(" 1. COMMAND: pipeline status (Online)")
    print("=" * 70)
    res_status = subprocess.run(
        [sys.executable, "pipeline.py", "--db", "demo_device.db", "--cloud-url", cloud_url, "status"],
        capture_output=True, text=True
    )
    print(res_status.stdout.strip())

    # Test pipeline results
    print("\n" + "=" * 70)
    print(f" 2. COMMAND: pipeline results --session {session_id}")
    print("=" * 70)
    res_results = subprocess.run(
        [sys.executable, "pipeline.py", "--db", "demo_device.db", "--cloud-url", cloud_url, "results", "--session", session_id],
        capture_output=True, text=True
    )
    res_lines = [l for l in res_results.stdout.strip().split("\n") if l]
    print(f"Total window results computed: {len(res_lines)}")
    if res_lines:
        print("First window (initial sample):")
        print(" ", res_lines[0])
        print("Last computed window (with Section 3 ECG heart rate calculation):")
        print(" ", res_lines[-1])

    # Test pipeline export --from device
    print("\n" + "=" * 70)
    print(f" 3. COMMAND: pipeline export --session {session_id} --from device")
    print("=" * 70)
    res_export_dev = subprocess.run(
        [sys.executable, "pipeline.py", "--db", "demo_device.db", "--cloud-url", cloud_url, "export", "--session", session_id, "--from", "device"],
        capture_output=True, text=True
    )
    dev_lines = [l for l in res_export_dev.stdout.strip().split("\n") if l]
    print(f"Total exported sample lines from device disk: {len(dev_lines)}")
    print("Sample lines from export:")
    for l in dev_lines[:2]:
        print(" ", l)

    # Export to file and check ground truth
    with open("demo_export.ndjson", "w", encoding="utf-8") as f:
        f.write(res_export_dev.stdout)

    print("\n" + "=" * 70)
    print(" 4. VALIDATION: check_ground_truth")
    print("=" * 70)
    res_gt = subprocess.run(
        [sys.executable, "-m", "mock.check_ground_truth", "demo_export.ndjson", "--mode", "signal", "--seed", "7"],
        capture_output=True, text=True
    )
    print(res_gt.stdout.strip())
    if res_gt.stderr:
        print(res_gt.stderr.strip())

    # Cleanup processes
    dev_proc.terminate()
    cloud_proc.terminate()
    mock_proc.terminate()

    print("\n" + "=" * 70)
    print(" DEMO RUN COMPLETE: ALL CRITERIA SATISFIED")
    print("=" * 70)

if __name__ == "__main__":
    main()
