"""
cloud/server.py: Cloud service API for real-time biosignal streaming, backlog catch-up,
online window processing, and cloud export.
"""
import argparse
import json
import logging
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from urllib.parse import urlparse, parse_qs
from cloud.storage import CloudStorageEngine
from common.window_manager import WindowManager

logger = logging.getLogger("cloud.server")


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


class CloudRequestHandler(BaseHTTPRequestHandler):
    storage: CloudStorageEngine = None
    window_manager: WindowManager = None

    def _send_json_response(self, code: int, data: dict):
        body = json.dumps(data).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        if path == "/api/health" or path == "/":
            self._send_json_response(200, {"status": "ok", "service": "biosignal-cloud", "time": time.time()})
            return

        session_id = query.get("session_id", [None])[0]

        if path == "/api/status":
            if not session_id:
                self._send_json_response(400, {"error": "session_id is required"})
                return
            count = self.storage.get_total_samples(session_id)
            last_w = self.storage.get_last_processed_window(session_id)
            self._send_json_response(200, {
                "session_id": session_id,
                "samples_saved": count,
                "last_processed_window": last_w
            })
            return

        if path == "/api/export":
            if not session_id:
                self._send_json_response(400, {"error": "session_id is required"})
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/x-ndjson")
            self.end_headers()
            for line in self.storage.export_data(session_id):
                self.wfile.write((line + "\n").encode("utf-8"))
            return

        if path == "/api/results":
            if not session_id:
                self._send_json_response(400, {"error": "session_id is required"})
                return
            results = self.storage.get_window_results(session_id)
            self.send_response(200)
            self.send_header("Content-Type", "application/x-ndjson")
            self.end_headers()
            for r in results:
                line = json.dumps(r) + "\n"
                self.wfile.write(line.encode("utf-8"))
            return

        self._send_json_response(404, {"error": "Not found"})

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path
        content_length = int(self.headers.get("Content-Length", 0))
        raw_body = self.rfile.read(content_length)

        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except Exception as e:
            self._send_json_response(400, {"error": f"Invalid JSON body: {e}"})
            return

        session_id = payload.get("session_id")
        if not session_id:
            self._send_json_response(400, {"error": "session_id is required"})
            return

        self.storage.register_session(session_id)

        if path == "/api/stream":
            sensor = payload.get("sensor")
            first_idx = payload.get("first_index", 0)
            samples = payload.get("samples", [])
            losses = payload.get("losses", [])

            inserted = self.storage.save_samples(session_id, sensor, first_idx, samples)
            for l in losses:
                self.storage.record_loss(
                    session_id, sensor, l["from_index"], l.get("to_index"), l.get("reason", "unknown")
                )

            # Online window processing (must appear in cloud within 5s)
            self.window_manager.process_pending_windows(session_id, mode="online")

            self._send_json_response(200, {"status": "ok", "inserted": inserted})
            return

        if path == "/api/sync_batch":
            sensor = payload.get("sensor")
            sample_items = payload.get("samples", [])  # list of [idx, [val0, val1, val2]]
            losses = payload.get("losses", [])

            inserted = self.storage.save_sync_samples(session_id, sensor, sample_items)
            for l in losses:
                self.storage.record_loss(
                    session_id, sensor, l["from_index"], l.get("to_index"), l.get("reason", "unknown")
                )

            # Check if any caught-up windows need processing
            self.window_manager.process_pending_windows(session_id, mode="online")

            self._send_json_response(200, {"status": "ok", "inserted": inserted})
            return

        self._send_json_response(404, {"error": "Not found"})

    def log_message(self, format, *args):
        # Suppress routine access logs for clean console
        pass


def run_cloud_server(port: int = 8080, db_path: str = "cloud_storage.db"):
    storage = CloudStorageEngine(db_path)
    window_manager = WindowManager(storage)
    CloudRequestHandler.storage = storage
    CloudRequestHandler.window_manager = window_manager

    server = ThreadedHTTPServer(("0.0.0.0", port), CloudRequestHandler)
    print(f"Cloud server started on port {port} (storage={db_path})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="biosignal cloud server")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--db", default="cloud_storage.db")
    args = parser.parse_args()

    run_cloud_server(args.port, args.db)
