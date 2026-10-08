#!/usr/bin/env python3
"""
Worker: Picks up tasks, runs opencode with assigned model, reports token usage.
"""

import json
import os
import sys
import time
import uuid
import subprocess
import threading
import signal
from pathlib import Path
from datetime import datetime
from typing import Optional
import fcntl

COORD_DIR = Path(os.environ.get("COORD_DIR", os.path.expanduser("~/src/popencode")))
QUEUE_DIR = COORD_DIR / "queue"
PROCESSING_DIR = COORD_DIR / "processing"
RESULTS_DIR = COORD_DIR / "results"
LOCKS_DIR = COORD_DIR / "locks"
STATUS_DIR = COORD_DIR / "status"

WORKER_ID = os.environ.get("WORKER_ID", f"worker-{uuid.uuid4().hex[:8]}")
MODEL = os.environ.get("OPENCODE_MODEL", "meta-llama/llama-3.1-70b-instruct")


class FileLock:
    def __init__(self, path: Path):
        self.path = path
        self.fd = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.fd = open(self.path, "w")
        fcntl.flock(self.fd, fcntl.LOCK_EX)
        return self

    def __exit__(self, *args):
        if self.fd:
            fcntl.flock(self.fd, fcntl.LOCK_UN)
            self.fd.close()


def write_status(data: dict):
    status_file = STATUS_DIR / f"{WORKER_ID}.json"
    data["timestamp"] = datetime.now().isoformat()
    data["worker_id"] = WORKER_ID
    with FileLock(status_file.with_suffix(".lock")):
        with open(status_file, "w") as f:
            json.dump(data, f)


def estimate_tokens(text: str) -> int:
    return len(text) // 4


class Worker:
    def __init__(self):
        self.running = True
        self.tokens_used = 0
        self.context_limit = 128000
        self.current_task = None
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def get_next_task(self) -> Optional[dict]:
        for task_file in sorted(QUEUE_DIR.glob("*.json")):
            processing_file = PROCESSING_DIR / task_file.name
            try:
                task_file.rename(processing_file)
                with open(processing_file) as f:
                    return json.load(f)
            except Exception:
                continue
        return None

    def run_opencode(self, task: str) -> str:
        cmd = [
            "opencode",
            "run",
            "-m", MODEL,
            task
        ]

        env = os.environ.copy()
        env["OPENCODE_MODEL"] = MODEL

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )

        stdout_lines = []
        stderr_lines = []

        def read_stream(stream, lines_list, prefix):
            for line in stream:
                lines_list.append(line)
                self.tokens_used += estimate_tokens(line)
                write_status({
                    "state": "working",
                    "model": MODEL,
                    "tokens_used": self.tokens_used,
                    "context_limit": self.context_limit,
                    "current_task": self.current_task,
                })

        stdout_thread = threading.Thread(target=read_stream, args=(proc.stdout, stdout_lines, "OUT"))
        stderr_thread = threading.Thread(target=read_stream, args=(proc.stderr, stderr_lines, "ERR"))
        stdout_thread.start()
        stderr_thread.start()

        proc.wait()
        stdout_thread.join()
        stderr_thread.join()

        output = "".join(stdout_lines)
        if proc.returncode != 0:
            error = "".join(stderr_lines)
            output += f"\n[ERROR] {error}"

        return output

    def save_result(self, task_id: str, output: str):
        result_file = RESULTS_DIR / f"{task_id}.json"
        with open(result_file, "w") as f:
            json.dump({
                "task_id": task_id,
                "worker_id": WORKER_ID,
                "model": MODEL,
                "output": output,
                "tokens_used": self.tokens_used,
                "completed": datetime.now().isoformat(),
            }, f)

    def run(self):
        write_status({
            "state": "idle",
            "model": MODEL,
            "tokens_used": 0,
            "context_limit": self.context_limit,
        })
        print(f"[{WORKER_ID}] Started with model {MODEL}")

        while self.running:
            task = self.get_next_task()
            if not task:
                write_status({
                    "state": "idle",
                    "model": MODEL,
                    "tokens_used": self.tokens_used,
                    "context_limit": self.context_limit,
                })
                time.sleep(1)
                continue

            self.current_task = task["id"]
            write_status({
                "state": "working",
                "model": MODEL,
                "tokens_used": self.tokens_used,
                "context_limit": self.context_limit,
                "current_task": self.current_task,
            })
            print(f"[{WORKER_ID}] Processing task: {task['id']}")

            output = self.run_opencode(task["task"])

            self.save_result(task["id"], output)

            processing_file = PROCESSING_DIR / f"{task['id']}.json"
            if processing_file.exists():
                processing_file.unlink()

            self.current_task = None
            print(f"[{WORKER_ID}] Completed task: {task['id']}")

    def shutdown(self, *args):
        self.running = False
        write_status({
            "state": "shutdown",
            "model": MODEL,
            "tokens_used": self.tokens_used,
            "context_limit": self.context_limit,
        })
        print(f"[{WORKER_ID}] Shutting down")
        sys.exit(0)


if __name__ == "__main__":
    worker = Worker()
    worker.run()