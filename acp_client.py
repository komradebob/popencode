#!/usr/bin/env python3
"""
ACP (Agent Client Protocol) client for opencode.
Communicates via JSON-RPC over TCP/stdin-stdout.
"""

import json
import socket
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Dict, Any, Optional, Callable, List
from dataclasses import dataclass, field
from queue import Queue, Empty


@dataclass
class ACPMessage:
    jsonrpc: str = "2.0"
    id: Optional[str] = None
    method: Optional[str] = None
    params: Optional[Dict] = None
    result: Optional[Any] = None
    error: Optional[Dict] = None


class ACPClient:
    def __init__(self, host: str = "127.0.0.1", port: int = 0, use_stdio: bool = False):
        self.host = host
        self.port = port
        self.use_stdio = use_stdio
        self.sock: Optional[socket.socket] = None
        self.proc: Optional[subprocess.Popen] = None
        self.request_id = 0
        self.pending: Dict[str, threading.Event] = {}
        self.responses: Dict[str, ACPMessage] = {}
        self.notifications: List[ACPMessage] = []
        self.running = False
        self.reader_thread: Optional[threading.Thread] = None
        self.lock = threading.Lock()

    def start(self, model: str = None) -> bool:
        import subprocess
        if self.use_stdio:
            cmd = ["opencode", "acp"]
            if model:
                cmd.extend(["-m", model])
            self.proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )
            self.sock = self.proc.stdout
        else:
            if self.port == 0:
                self.port = self._find_free_port()
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.sock.bind((self.host, self.port))
            self.sock.listen(1)
            print(f"[ACP] Listening on {self.host}:{self.port}")

            cmd = ["opencode", "acp", "--port", str(self.port)]
            if model:
                cmd.extend(["-m", model])
            self.proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            self.conn, _ = self.sock.accept()
            self.sock = self.conn

        self.running = True
        self.reader_thread = threading.Thread(target=self._read_loop, daemon=True)
        self.reader_thread.start()

        time.sleep(1)
        return self.initialize(model)

    def _find_free_port(self) -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('', 0))
            return s.getsockname()[1]

    def _read_loop(self):
        buffer = ""
        while self.running:
            try:
                if self.use_stdio:
                    line = self.sock.readline()
                    if not line:
                        break
                    buffer += line
                else:
                    data = self.sock.recv(4096)
                    if not data:
                        break
                    buffer += data.decode()

                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    line = line.strip()
                    if not line:
                        continue
                    self._handle_message(line)
            except Exception as e:
                if self.running:
                    print(f"[ACP] Read error: {e}")
                break

    def _handle_message(self, line: str):
        try:
            msg = json.loads(line)
            acp_msg = ACPMessage(**msg)

            if acp_msg.id and acp_msg.id in self.pending:
                with self.lock:
                    self.responses[acp_msg.id] = acp_msg
                    self.pending[acp_msg.id].set()
            elif acp_msg.method:
                self.notifications.append(acp_msg)
        except json.JSONDecodeError:
            pass

    def _send(self, method: str, params: Dict = None) -> ACPMessage:
        self.request_id += 1
        req_id = str(self.request_id)
        msg = ACPMessage(id=req_id, method=method, params=params or {})
        event = threading.Event()
        with self.lock:
            self.pending[req_id] = event

        data = json.dumps(msg.__dict__) + "\n"
        if self.use_stdio:
            self.proc.stdin.write(data)
            self.proc.stdin.flush()
        else:
            self.sock.sendall(data.encode())

        event.wait(timeout=30)
        with self.lock:
            return self.responses.pop(req_id, ACPMessage(error={"code": -1, "message": "Timeout"}))

    def initialize(self, model: str = None) -> bool:
        params = {"protocolVersion": "0.1"}
        if model:
            params["model"] = model
        resp = self._send("initialize", params)
        return resp.error is None

    def send_message(self, content: str, session_id: str = None) -> str:
        params = {"content": content}
        if session_id:
            params["sessionId"] = session_id
        resp = self._send("message/send", params)
        if resp.error:
            raise RuntimeError(f"ACP error: {resp.error}")
        return resp.result.get("sessionId", "") if resp.result else ""

    def get_session_stats(self, session_id: str) -> Dict:
        resp = self._send("session/stats", {"sessionId": session_id})
        return resp.result or {}

    def close(self):
        self.running = False
        if self.proc:
            self.proc.terminate()
            self.proc.wait(timeout=5)
        if self.sock and not self.use_stdio:
            self.sock.close()


class ACPWorker:
    """Worker that uses ACP protocol for precise token tracking."""

    def __init__(self, worker_id: str, model: str, coord_dir: Path, port: int = 0):
        self.worker_id = worker_id
        self.model = model
        self.coord_dir = coord_dir
        self.port = port
        self.client: Optional[ACPClient] = None
        self.session_id: Optional[str] = None
        self.tokens_used = 0
        self.context_limit = 128000
        self.running = True

    def start(self):
        self.client = ACPClient(port=self.port)
        if not self.client.start(self.model):
            raise RuntimeError("Failed to start ACP client")
        print(f"[{self.worker_id}] ACP connected on port {self.client.port}")

    def process_task(self, task: str) -> str:
        self.session_id = self.client.send_message(task)
        if not self.session_id:
            return "[ERROR] No session created"

        stats = self.client.get_session_stats(self.session_id)
        self.tokens_used = stats.get("totalTokens", 0)
        self.context_limit = stats.get("contextLimit", 128000)

        return stats.get("lastResponse", "No response")

    def get_token_usage(self) -> Dict:
        if self.client and self.session_id:
            stats = self.client.get_session_stats(self.session_id)
            self.tokens_used = stats.get("totalTokens", self.tokens_used)
            self.context_limit = stats.get("contextLimit", self.context_limit)
        return {
            "tokens_used": self.tokens_used,
            "context_limit": self.context_limit,
            "usage_ratio": self.tokens_used / self.context_limit if self.context_limit > 0 else 0,
        }

    def shutdown(self):
        self.running = False
        if self.client:
            self.client.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True)
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--worker-id", required=True)
    args = parser.parse_args()

    worker = ACPWorker(args.worker_id, args.model, Path(os.path.expanduser("~/src/popencode")), args.port)
    worker.start()
    print(f"Worker {args.worker_id} ready")
    while True:
        time.sleep(1)