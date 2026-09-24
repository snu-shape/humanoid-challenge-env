"""Loopback-only, latest-frame MJPEG viewer for SSH forwarding."""
import io
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PAGE = b'''<!doctype html><html lang="ko"><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>MHC live camera</title><style>
body{background:#111820;color:#eee;font:16px system-ui;margin:32px auto;max-width:1000px;padding:0 20px}
img{width:100%;image-rendering:auto;background:#000;border-radius:12px}
p{color:#b7c5d4}#status{font-variant-numeric:tabular-nums}
</style><h1>MHC / Task A / Head camera</h1>
<p>Live rendered camera via SSH. Initial-scene demo: the robot holds its pose; no policy is running.</p>
<p id="status">Waiting for simulator and first frame...</p><img src="/stream.mjpg" alt="Live robot camera">
<script>async function update(){try{let s=await(await fetch('/status',{cache:'no-store'})).json();
document.getElementById('status').textContent=s.frame?'Frame '+s.frame+' | '+s.fps.toFixed(1)+' fps | last frame '+s.age.toFixed(1)+'s ago'+(s.age>5?' | STALE':' | LIVE'):'Loading simulator...';
}catch(e){document.getElementById('status').textContent='Disconnected: check SSH tunnel';}}setInterval(update,1000);update();</script></html>'''


class CameraStream:
    def __init__(self, port=18080):
        self.condition = threading.Condition()
        self.jpeg = None
        self.sequence = 0
        self.updated = 0.0
        self.started = None
        self.closed = False
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def reply(self, data, content_type, status=200):
                self.send_response(status)
                self.send_header('Content-Type', content_type)
                self.send_header('Content-Length', str(len(data)))
                self.send_header('Cache-Control', 'no-store')
                self.send_header('X-Content-Type-Options', 'nosniff')
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self):
                try:
                    if self.path == '/':
                        return self.reply(PAGE, 'text/html; charset=utf-8')
                    if self.path == '/status':
                        with owner.condition:
                            now = time.monotonic()
                            state = dict(frame=owner.sequence,
                                         age=now-owner.updated if owner.sequence else 0,
                                         fps=(owner.sequence-1)/max(.001,owner.updated-owner.started) if owner.sequence > 1 else 0)
                        return self.reply(json.dumps(state).encode(), 'application/json')
                    if self.path == '/frame.jpg':
                        with owner.condition:
                            data = owner.jpeg
                        return self.reply(data or b'Frame not ready', 'image/jpeg' if data else 'text/plain', 200 if data else 503)
                    if self.path != '/stream.mjpg':
                        return self.reply(b'Not found', 'text/plain', 404)
                    self.send_response(200)
                    self.send_header('Content-Type', 'multipart/x-mixed-replace; boundary=frame')
                    self.send_header('Cache-Control', 'no-store')
                    self.end_headers()
                    seen = 0
                    while not owner.closed:
                        with owner.condition:
                            owner.condition.wait_for(lambda: owner.sequence != seen or owner.closed, timeout=10)
                            if owner.closed:
                                break
                            if owner.sequence == seen:
                                continue
                            data, seen = owner.jpeg, owner.sequence
                        self.wfile.write(b'--frame\r\nContent-Type: image/jpeg\r\nContent-Length: ' + str(len(data)).encode() + b'\r\nX-Frame-Number: ' + str(seen).encode() + b'\r\n\r\n' + data + b'\r\n')
                        self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError, TimeoutError):
                    pass

        self.server = ThreadingHTTPServer(('127.0.0.1', port), Handler)
        self.server.daemon_threads = True
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        print(f'[stream] Listening on http://127.0.0.1:{port} (SSH only)', flush=True)

    def publish(self, rgb):
        from PIL import Image
        buffer = io.BytesIO()
        Image.fromarray(rgb).save(buffer, format='JPEG', quality=80)
        now = time.monotonic()
        with self.condition:
            self.jpeg = buffer.getvalue()
            self.sequence += 1
            self.updated = now
            if self.started is None:
                self.started = now
            self.condition.notify_all()

    def close(self):
        with self.condition:
            self.closed = True
            self.condition.notify_all()
        self.server.shutdown()
        self.server.server_close()
