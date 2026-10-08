#!/usr/bin/env python3
"""
Supervisor: Manages worker pool (local ACP + distributed), monitors token usage, spawns new workers.
Communicates via file system in ~/src/popencode/
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
from typing import Dict, List, Optional
import fcntl

COORD_DIR = Path(os.path.expanduser("~/src/popencode"))
QUEUE_DIR = COORD_DIR / "queue"
PROCESSING_DIR = COORD_DIR / "processing"
RESULTS_DIR = COORD_DIR / "results"
LOCKS_DIR = COORD_DIR / "locks"
STATUS_DIR = COORD_DIR / "status"
MODELS_FILE = COORD_DIR / "models" / "free_models.json"
MACHINES_FILE = COORD_DIR / "machines.json"

QUEUE_DIR.mkdir(parents=True, exist_ok=True)
PROCESSING_DIR.mkdir(parents=True, exist_ok=True)
RESULTS_DIR.mkdir(parents=True, exist_ok=True)
LOCKS_DIR.mkdir(parents=True, exist_ok=True)
STATUS_DIR.mkdir(parents=True, exist_ok=True)


class FileLock:
    def __init__(self, path: Path):
        self.path = path
        self.fd = None

    def __enter__(self):
        self.fd = open(self.path, "w")
        fcntl.flock(self.fd, fcntl.LOCK_EX)
        return self

    def __exit__(self, *args):
        if self.fd:
            fcntl.flock(self.fd, fcntl.LOCK_UN)
            self.fd.close()


try:
    from distributed import DistributedSupervisor, Machine, load_machines
    DISTRIBUTED_AVAILABLE = True
except ImportError as e:
    print(f"[SUPERVISOR] Distributed module not available: {e}")
    DISTRIBUTED_AVAILABLE = False


class Supervisor:
    def __init__(self):
        self.workers: Dict[str, dict] = {}
        self.available_models: List[dict] = []
        self.used_models: set = set()
        self.running = True
        self.task_counter = 0
        self.load_models()
        self.distributed = None
        self.machines: List[Machine] = []
        self.worker_model_map: Dict[str, str] = {}
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def load_models(self):
        with open(MODELS_FILE) as f:
            data = json.load(f)
        self.available_models = [m for m in data["models"] if m["free"]]
        self.token_threshold = data.get("token_warning_threshold", 0.9)

    def load_machines(self):
        if MACHINES_FILE.exists() and DISTRIBUTED_AVAILABLE:
            self.machines = load_machines(MACHINES_FILE)
            self.distributed = DistributedSupervisor(self.machines, COORD_DIR)
            print(f"[SUPERVISOR] Loaded {len(self.machines)} machines")
            for m in self.machines:
                print(f"  - {m.name} ({m.host}) {'shared_fs' if m.shared_fs else 'tunnel'} max_workers={m.max_workers}")
        else:
            print("[SUPERVISOR] No machine config, using local only")

    def get_next_model(self) -> Optional[dict]:
        for model in self.available_models:
            if model["name"] not in self.used_models:
                self.used_models.add(model["name"])
                return model
        return None

    def release_model(self, model_name: str):
        self.used_models.discard(model_name)

    def write_status(self, worker_id: str, data: dict):
        status_file = STATUS_DIR / f"{worker_id}.json"
        data["timestamp"] = datetime.now().isoformat()
        with FileLock(status_file.with_suffix(".lock")):
            with open(status_file, "w") as f:
                json.dump(data, f)

    def read_status(self, worker_id: str) -> Optional[dict]:
        status_file = STATUS_DIR / f"{worker_id}.json"
        if not status_file.exists():
            return None
        with FileLock(status_file.with_suffix(".lock")):
            with open(status_file) as f:
                return json.load(f)

    def spawn_local_acp_worker(self, model: dict) -> str:
        if not self.distributed:
            return self._spawn_local_cli_worker(model)

        worker_id = f"worker-{uuid.uuid4().hex[:8]}"
        worker_info = self.distributed.spawn_local_acp_worker(model["name"], worker_id)

        self.workers[worker_id] = {
            "type": "local_acp",
            "model": model["name"],
            "provider": model["provider"],
            "context": model["context"],
            "worker": worker_info["worker"],
            "started": datetime.now().isoformat(),
            "tokens_used": 0,
            "warning_sent": False,
        }
        self.worker_model_map[worker_id] = model["name"]
        self.write_status(worker_id, {
            "state": "starting",
            "model": model["name"],
            "tokens_used": 0,
            "context_limit": model["context"],
        })
        print(f"[SUPERVISOR] Spawned LOCAL ACP {worker_id} with model {model['name']}")
        return worker_id

    def _spawn_local_cli_worker(self, model: dict) -> str:
        worker_id = f"worker-{uuid.uuid4().hex[:8]}"
        env = os.environ.copy()
        env["OPENCODE_MODEL"] = model["name"]
        env["WORKER_ID"] = worker_id
        env["COORD_DIR"] = str(COORD_DIR)

        proc = subprocess.Popen(
            [sys.executable, str(Path(__file__).parent / "worker.py")],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        self.workers[worker_id] = {
            "type": "local_cli",
            "pid": proc.pid,
            "model": model["name"],
            "provider": model["provider"],
            "context": model["context"],
            "process": proc,
            "started": datetime.now().isoformat(),
            "tokens_used": 0,
            "warning_sent": False,
        }
        self.worker_model_map[worker_id] = model["name"]
        self.write_status(worker_id, {
            "state": "starting",
            "model": model["name"],
            "tokens_used": 0,
            "context_limit": model["context"],
        })
        print(f"[SUPERVISOR] Spawned LOCAL CLI {worker_id} with model {model['name']} (PID: {proc.pid})")
        return worker_id

    def spawn_remote_worker(self, machine_name: str, model: dict) -> Optional[str]:
        if not self.distributed:
            return None

        machine = next((m for m in self.machines if m.name == machine_name), None)
        if not machine:
            print(f"[SUPERVISOR] Machine {machine_name} not found")
            return None

        if len([w for w in self.workers.values() if w.get("machine") == machine_name]) >= machine.max_workers:
            print(f"[SUPERVISOR] Machine {machine_name} at max workers ({machine.max_workers})")
            return None

        worker_id = f"worker-{uuid.uuid4().hex[:8]}"
        worker_info = self.distributed.spawn_remote_worker(machine, model["name"], worker_id)

        self.workers[worker_id] = {
            "type": "remote",
            "machine": machine_name,
            "model": model["name"],
            "provider": model["provider"],
            "context": model["context"],
            "worker_info": worker_info,
            "started": datetime.now().isoformat(),
            "tokens_used": 0,
            "warning_sent": False,
        }
        self.worker_model_map[worker_id] = model["name"]
        self.write_status(worker_id, {
            "state": "starting",
            "model": model["name"],
            "tokens_used": 0,
            "context_limit": model["context"],
        })
        print(f"[SUPERVISOR] Spawned REMOTE {worker_id} on {machine_name} with model {model['name']}")
        return worker_id

    def check_worker_health(self, worker_id: str) -> bool:
        worker = self.workers.get(worker_id)
        if not worker:
            return False

        if worker["type"] == "local_cli":
            proc = worker["process"]
            if proc.poll() is not None:
                print(f"[SUPERVISOR] Worker {worker_id} exited with code {proc.returncode}")
                self.release_model(worker["model"])
                del self.workers[worker_id]
                return False
            return True

        elif worker["type"] == "local_acp":
            if not self.distributed.check_worker(worker_id):
                print(f"[SUPERVISOR] ACP Worker {worker_id} died")
                self.release_model(worker["model"])
                del self.workers[worker_id]
                return False
            return True

        elif worker["type"] == "remote":
            if not self.distributed.check_worker(worker_id):
                print(f"[SUPERVISOR] Remote Worker {worker_id} died")
                self.release_model(worker["model"])
                del self.workers[worker_id]
                return False
            return True

        return False

    def monitor_tokens(self):
        for worker_id, worker in list(self.workers.items()):
            if worker["type"] == "local_acp" or worker["type"] == "remote":
                usage = self.distributed.get_worker_token_usage(worker_id)
                tokens_used = usage.get("tokens_used", 0)
                context_limit = usage.get("context_limit", worker["context"])
            else:
                status = self.read_status(worker_id)
                if not status:
                    continue
                tokens_used = status.get("tokens_used", 0)
                context_limit = worker["context"]

            worker["tokens_used"] = tokens_used

            usage_ratio = tokens_used / context_limit if context_limit > 0 else 0

            if usage_ratio >= self.token_threshold and not worker["warning_sent"]:
                print(f"[SUPERVISOR] WARNING: {worker_id} at {usage_ratio:.1%} token usage ({tokens_used}/{context_limit})")
                worker["warning_sent"] = True
                self.spawn_replacement_worker(worker)

            elif usage_ratio >= 1.0:
                print(f"[SUPERVISOR] CRITICAL: {worker_id} exhausted tokens, terminating")
                self.terminate_worker(worker_id)

    def terminate_worker(self, worker_id: str):
        worker = self.workers.get(worker_id)
        if not worker:
            return

        if worker["type"] == "local_cli":
            worker["process"].terminate()
        elif worker["type"] == "local_acp":
            worker["worker"].shutdown()
        elif worker["type"] == "remote":
            pass

        self.release_model(worker["model"])
        del self.workers[worker_id]

    def spawn_replacement_worker(self, exhausted_worker: dict):
        new_model = self.get_next_model()
        if not new_model:
            print("[SUPERVISOR] No more free models available!")
            return

        if exhausted_worker["type"] == "local_acp" or exhausted_worker["type"] == "local_cli":
            self.spawn_local_acp_worker(new_model)
        elif exhausted_worker["type"] == "remote":
            machine_name = exhausted_worker.get("machine")
            if machine_name:
                self.spawn_remote_worker(machine_name, new_model)

    def submit_task(self, task: str):
        self.task_counter += 1
        task_id = f"task-{self.task_counter}-{uuid.uuid4().hex[:8]}"
        task_file = QUEUE_DIR / f"{task_id}.json"
        with open(task_file, "w") as f:
            json.dump({
                "id": task_id,
                "task": task,
                "submitted": datetime.now().isoformat(),
            }, f)
        print(f"[SUPERVISOR] Submitted task: {task_id}")
        return task_id

    def collect_results(self):
        for result_file in RESULTS_DIR.glob("*.json"):
            try:
                with open(result_file) as f:
                    result = json.load(f)
                print(f"\n[RESULT] {result['task_id']} (by {result['worker_id']}):")
                print(result["output"][:2000])
                if len(result["output"]) > 2000:
                    print(f"... ({len(result['output'])} chars total)")
                result_file.unlink()
            except Exception as e:
                print(f"[SUPERVISOR] Error reading result: {e}")

    def run_interactive(self):
        print("\n=== OpenCode Distributed Supervisor ===")
        print(f"Available models: {len(self.available_models)}")
        self.load_machines()
        print("Commands: <task> | 'status' | 'workers' | 'spawn <machine> <model>' | 'quit'")
        print()

        for _ in range(3):
            model = self.get_next_model()
            if model:
                self.spawn_local_acp_worker(model)
            time.sleep(0.5)

        while self.running:
            self.monitor_tokens()
            self.collect_results()

            for wid in list(self.workers.keys()):
                self.check_worker_health(wid)

            try:
                cmd = input("\n> ").strip()
            except (EOFError, KeyboardInterrupt):
                break

            if cmd == "quit":
                break
            elif cmd == "status":
                self.print_status()
            elif cmd == "workers":
                self.print_workers()
            elif cmd.startswith("spawn "):
                parts = cmd.split()
                if len(parts) >= 3:
                    machine_name = parts[1]
                    model_name = " ".join(parts[2:])
                    model = next((m for m in self.available_models if m["name"] == model_name), None)
                    if model:
                        self.spawn_remote_worker(machine_name, model)
                    else:
                        print(f"Model {model_name} not found")
                else:
                    print("Usage: spawn <machine> <model>")
            elif cmd:
                self.submit_task(cmd)

        self.shutdown()

    def print_status(self):
        print(f"\nActive workers: {len(self.workers)}")
        for wid, w in self.workers.items():
            status = self.read_status(wid)
            state = status.get("state", "unknown") if status else "no status"
            tokens = w["tokens_used"]
            ctx = w["context"]
            type_str = w["type"]
            if type_str == "remote":
                type_str += f"@{w['machine']}"
            print(f"  {wid}: {w['model']} [{type_str}] | {state} | tokens: {tokens}/{ctx} ({tokens/ctx:.1%})")

    def print_workers(self):
        print(f"\nWorker pool ({len(self.workers)} active):")
        for wid, w in self.workers.items():
            if w["type"] == "local_cli":
                print(f"  {wid}: PID={w['pid']} model={w['model']} provider={w['provider']} [CLI]")
            elif w["type"] == "local_acp":
                print(f"  {wid}: model={w['model']} provider={w['provider']} [ACP]")
            elif w["type"] == "remote":
                print(f"  {wid}: model={w['model']} provider={w['provider']} [REMOTE@{w['machine']}]")

    def shutdown(self, *args):
        print("\n[SUPERVISOR] Shutting down...")
        self.running = False

        for wid, w in self.workers.items():
            if w["type"] == "local_cli":
                w["process"].terminate()
            elif w["type"] == "local_acp":
                w["worker"].shutdown()

        for wid, w in self.workers.items():
            if w["type"] == "local_cli":
                try:
                    w["process"].wait(timeout=5)
                except subprocess.TimeoutExpired:
                    w["process"].kill()

        if self.distributed:
            self.distributed.shutdown()

        sys.exit(0)


if __name__ == "__main__":
    sup = Supervisor()
    sup.run_interactive()