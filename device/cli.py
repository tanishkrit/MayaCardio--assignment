"""
device/cli.py: Main CLI entrypoint implementing required 'pipeline' commands:
- pipeline status
- pipeline export --session ID --from [device|cloud]
- pipeline results --session ID
- pipeline run
"""
import argparse
import asyncio
import json
import os
import sys
import urllib.request
import urllib.error
from typing import Optional
from device.storage import StorageEngine
from device.mode import ModeController
from device.ingest import IngestionWorker
from device.syncer import CloudSynchronizer
from device.processor import DeviceProcessor


def get_latest_session_id(storage: StorageEngine) -> Optional[str]:
    with storage._get_connection() as con:
        cur = con.cursor()
        cur.execute("SELECT session_id FROM sessions ORDER BY created_at DESC LIMIT 1")
        row = cur.fetchone()
        return row[0] if row else None


def cmd_status(args, db_path: str, cloud_url: str):
    storage = StorageEngine(db_path)
    mode_ctrl = ModeController(cloud_url=cloud_url)
    # Check cloud health for instantaneous mode
    mode = "online" if mode_ctrl.check_cloud_health() else "offline"
    
    session_id = getattr(args, "session", None) or get_latest_session_id(storage)
    if not session_id:
        # Default empty status
        out = {"mode": mode, "samples_saved": 0, "samples_cloud": 0, "backlog": 0}
        print(json.dumps(out))
        return

    st = storage.get_status(session_id)
    out = {
        "mode": mode,
        "samples_saved": st["samples_saved"],
        "samples_cloud": st["samples_cloud"],
        "backlog": st["backlog"]
    }
    print(json.dumps(out))


def cmd_export(args, db_path: str, cloud_url: str):
    session_id = args.session
    source = getattr(args, "from_source", "device")
    
    if source == "device":
        storage = StorageEngine(db_path)
        for line in storage.export_data(session_id):
            print(line)
    elif source == "cloud":
        url = f"{cloud_url.rstrip('/')}/api/export?session_id={session_id}"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "MayaCardio-CLI"})
            with urllib.request.urlopen(req, timeout=30.0) as resp:
                for line in resp:
                    sys.stdout.write(line.decode("utf-8"))
        except Exception as e:
            print(f"Error fetching export from cloud: {e}", file=sys.stderr)
            sys.exit(1)


def cmd_results(args, db_path: str, cloud_url: str):
    session_id = args.session
    storage = StorageEngine(db_path)
    
    # 1. Fetch locally stored results
    results_by_window = {r["window"]: r for r in storage.get_window_results(session_id)}
    
    # 2. Try fetching cloud results if cloud is reachable
    if cloud_url:
        url = f"{cloud_url.rstrip('/')}/api/results?session_id={session_id}"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "MayaCardio-CLI"})
            with urllib.request.urlopen(req, timeout=3.0) as resp:
                for line in resp:
                    text = line.decode("utf-8").strip()
                    if text:
                        r = json.loads(text)
                        results_by_window[r["window"]] = r
        except Exception:
            pass

    # 3. If there are windows not yet computed, compute them on-demand
    from common.window_manager import WindowManager
    wm = WindowManager(storage)
    pending = wm.process_pending_windows(session_id, mode="offline")
    for r in pending:
        if r["window"] not in results_by_window:
            results_by_window[r["window"]] = r

    for w in sorted(results_by_window.keys()):
        print(json.dumps(results_by_window[w]))



async def run_daemon(db_path: str, mock_host: str, mock_port: int, cloud_url: str):
    storage = StorageEngine(db_path)
    mode_ctrl = ModeController(cloud_url=cloud_url)
    syncer = CloudSynchronizer(storage=storage, mode_controller=mode_ctrl, cloud_url=cloud_url)
    processor = DeviceProcessor(storage=storage, mode_controller=mode_ctrl)
    
    def on_samples_saved(session_id: str, sensor: int, first_index: int, count: int):
        # In online mode, try streaming immediately
        if mode_ctrl.is_online():
            samples = storage.get_samples_in_range(session_id, sensor, first_index, first_index + count - 1)
            ordered_samples = [samples[i] for i in range(first_index, first_index + count) if i in samples]
            syncer.stream_batch(session_id, sensor, first_index, ordered_samples)

    ingest = IngestionWorker(
        storage=storage,
        host=mock_host,
        port=mock_port,
        on_samples_saved=on_samples_saved
    )

    def get_current_session():
        return ingest.current_session_id or get_latest_session_id(storage)

    print(f"Starting pipeline daemon (db={db_path}, mock={mock_host}:{mock_port}, cloud={cloud_url})...")
    
    tasks = [
        asyncio.create_task(mode_ctrl.run_loop()),
        asyncio.create_task(syncer.run_loop(get_current_session)),
        asyncio.create_task(processor.run_loop(get_current_session)),
        asyncio.create_task(ingest.run_loop())
    ]
    
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    finally:
        mode_ctrl.stop()
        syncer.stop()
        processor.stop()
        ingest.stop()


def main():
    parser = argparse.ArgumentParser(prog="pipeline", description="Biosignal Data Pipeline CLI")
    parser.add_argument("--db", default=os.environ.get("PIPELINE_DB", "pipeline_device.db"), help="Local database path")
    parser.add_argument("--cloud-url", default=os.environ.get("CLOUD_URL", "http://127.0.0.1:8080"), help="Cloud server URL")
    parser.add_argument("--mock-host", default=os.environ.get("MOCK_HOST", "127.0.0.1"), help="biosignal-mock host")
    parser.add_argument("--mock-port", type=int, default=int(os.environ.get("MOCK_PORT", "7100")), help="biosignal-mock port")

    subparsers = parser.add_subparsers(dest="command")

    # pipeline status
    sub_status = subparsers.add_parser("status", help="Get pipeline status (single JSON line)")
    sub_status.add_argument("--session", help="Session ID (optional, defaults to latest)")

    # pipeline export --session ID --from device (or --from cloud)
    sub_export = subparsers.add_parser("export", help="Export session data as JSON lines")
    sub_export.add_argument("--session", required=True, help="Session ID to export")
    sub_export.add_argument("--from", dest="from_source", choices=["device", "cloud"], default="device", help="Source to export from")

    # pipeline results --session ID
    sub_results = subparsers.add_parser("results", help="Export window results as JSON lines")
    sub_results.add_argument("--session", required=True, help="Session ID to export results for")

    # pipeline run
    subparsers.add_parser("run", help="Run the pipeline background daemon")

    args = parser.parse_args()

    if args.command == "status":
        cmd_status(args, args.db, args.cloud_url)
    elif args.command == "export":
        cmd_export(args, args.db, args.cloud_url)
    elif args.command == "results":
        cmd_results(args, args.db, args.cloud_url)
    elif args.command == "run":
        try:
            asyncio.run(run_daemon(args.db, args.mock_host, args.mock_port, args.cloud_url))
        except KeyboardInterrupt:
            print("\nPipeline stopped cleanly.")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
