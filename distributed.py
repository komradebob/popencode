#!/usr/bin/env python3
"""
Distributed worker management:
- SSH tunnels for remote coordination directory access
- ACP (Agent Client Protocol) over TCP for local opencode instances
- File-based coordination for remote workers
"""

import json
import os
import sys
import subprocess
import threading
import time
import socket
from pathlib import Path
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, field
import paramiko
from paramiko import SSHClient, AutoAddPolicy


@dataclass
class Machine:
    name: str
    host: str
    user: str = "auto"
    key_path: Optional[str] = None
    port: int = 22
    coord_mount: str = os.path.expanduser("~/src/popencode")
    shared_fs: bool = False
    use_tunnel: bool = True
    max_workers: int = 2
    models: List[str] = field(default_factory=list)
    env: Dict[str, str] = field(default_factory=dict)
    acp_port_range: tuple = (10000, 11000)

    def __post_init__(self):
        if self.user == "auto":
            self.user = os.environ.get("USER", "ubuntu")


class SSHManager:
    def __init__(self):
        self.connections: Dict[str, SSHClient] = {}
        self.tunnels: Dict[str, List[subprocess.Popen]] = {}

    def connect(self, machine: Machine) -> SSHClient:
        if machine.name in self.connections:
            return self.connections[machine.name]

        client = SSHClient()
        client.set_missing_host_key_policy(AutoAddPolicy())

        connect_kwargs = {
            "hostname": machine.host,
            "username": machine.user,
            "port": machine.port,
            "timeout": 10,
        }
        if machine.key_path:
            connect_kwargs["key_filename"] = os.path.expanduser(machine.key_path)

        client.connect(**connect_kwargs)
        self.connections[machine.name] = client
        return client

    def run_command(self, machine: Machine, cmd: str, env: Dict[str, str] = None, background: bool = False) -> tuple:
        client = self.connect(machine)
        full_env = {**os.environ, **(env or {})}
        env_str = " ".join(f'{k}="{v}"' for k, v in full_env.items())
        full_cmd = f"{env_str} {cmd}"

        if background:
            stdin, stdout, stderr = client.exec_command(full_cmd, get_pty=False)
            return 0, stdout.read().decode(), stderr.read().decode()
        else:
            stdin, stdout, stderr = client.exec_command(full_cmd, get_pty=True)
            exit_code = stdout.channel.recv_exit_status()
            return exit_code, stdout.read().decode(), stderr.read().decode()

    def start_tunnel(self, machine: Machine, local_port: int, remote_port: int) -> subprocess.Popen:
        tunnel_cmd = [
            "ssh", "-N", "-L", f"{local_port}:localhost:{remote_port}",
            f"{machine.user}@{machine.host}", "-p", str(machine.port)
        ]
        if machine.key_path:
            tunnel_cmd.extend(["-i", os.path.expanduser(machine.key_path)])

        proc = subprocess.Popen(tunnel_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if machine.name not in self.tunnels:
            self.tunnels[machine.name] = []
        self.tunnels[machine.name].append(proc)
        time.sleep(0.5)
        return proc

    def start_reverse_tunnel(self, machine: Machine, remote_port: int, local_port: int) -> subprocess.Popen:
        tunnel_cmd = [
            "ssh", "-N", "-R", f"{remote_port}:localhost:{local_port}",
            f"{machine.user}@{machine.host}", "-p", str(machine.port)
        ]
        if machine.key_path:
            tunnel_cmd.extend(["-i", os.path.expanduser(machine.key_path)])

        proc = subprocess.Popen(tunnel_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if machine.name not in self.tunnels:
            self.tunnels[machine.name] = []
        self.tunnels[machine.name].append(proc)
        time.sleep(0.5)
        return proc

    def sync_directory(self, machine: Machine, local_path: Path, remote_path: str, direction: str = "push"):
        rsync_opts = "-avz --delete --exclude='*.pyc' --exclude='__pycache__'"
        ssh_opts = f"-p {machine.port}"
        if machine.key_path:
            ssh_opts += f" -i {os.path.expanduser(machine.key_path)}"

        if direction == "push":
            cmd = f"rsync {rsync_opts} -e 'ssh {ssh_opts}' {local_path}/ {machine.user}@{machine.host}:{remote_path}/"
        else:
            cmd = f"rsync {rsync_opts} -e 'ssh {ssh_opts}' {machine.user}@{machine.host}:{remote_path}/ {local_path}/"

        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"rsync failed: {result.stderr}")

    def check_path_exists(self, machine: Machine, path: str) -> bool:
        _, stdout, _ = self.run_command(machine, f"test -d {path} && echo EXISTS || echo MISSING")
        return "EXISTS" in stdout

    def close(self):
        for procs in self.tunnels.values():
            for proc in procs:
                proc.terminate()
        for client in self.connections.values():
            client.close()


class DistributedSupervisor:
    def __init__(self, machines: List[Machine], local_coord: Path):
        self.machines = {m.name: m for m in machines}
        self.ssh = SSHManager()
        self.local_coord = local_coord
        self.remote_workers: Dict[str, Dict] = {}
        self.local_acp_ports: Dict[str, int] = {}
        self.port_counter = 10000

    def get_next_acp_port(self) -> int:
        port = self.port_counter
        self.port_counter += 1
        return port

    def setup_machine(self, machine: Machine) -> Path:
        if machine.host == "localhost" or machine.host == "127.0.0.1":
            print(f"[{machine.name}] Local machine, using {self.local_coord}")
            return self.local_coord

        if machine.shared_fs:
            print(f"[{machine.name}] Using shared filesystem at {machine.coord_mount}")
            coord_path = Path(machine.coord_mount)
            if not coord_path.exists():
                raise RuntimeError(f"Shared FS not mounted at {machine.coord_mount}")
            return coord_path

        if machine.use_tunnel:
            print(f"[{machine.name}] Setting up SSH tunnel for coordination directory")
            return self._setup_tunnel_coordination(machine)
        else:
            print(f"[{machine.name}] Syncing coordination directory via rsync")
            self.ssh.sync_directory(machine, self.local_coord, machine.coord_mount, "push")
            return self.local_coord

    def _setup_tunnel_coordination(self, machine: Machine) -> Path:
        local_mount = self.local_coord / f"mnt_{machine.name}"
        local_mount.mkdir(exist_ok=True)

        remote_port = self.get_next_acp_port()
        local_port = self.get_next_acp_port()

        self.ssh.start_tunnel(machine, local_port, remote_port)

        sshfs_cmd = [
            "sshfs", f"{machine.user}@{machine.host}:{machine.coord_mount}",
            str(local_mount),
            "-o", f"port={machine.port}",
            "-o", "allow_other",
            "-o", "reconnect",
            "-o", "ServerAliveInterval=15",
        ]
        if machine.key_path:
            sshfs_cmd.extend(["-o", f"IdentityFile={os.path.expanduser(machine.key_path)}"])

        result = subprocess.run(sshfs_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"sshfs mount failed: {result.stderr}")

        print(f"[{machine.name}] Mounted {machine.coord_mount} at {local_mount} via tunnel localhost:{local_port}")
        return local_mount

    def spawn_local_acp_worker(self, model: str, worker_id: str) -> Dict:
        port = self.get_next_acp_port()
        self.local_acp_ports[worker_id] = port

        from acp_client import ACPWorker
        worker = ACPWorker(worker_id, model, self.local_coord, port)
        worker.start()

        thread = threading.Thread(target=self._run_acp_worker_loop, args=(worker,), daemon=True)
        thread.start()

        self.remote_workers[worker_id] = {
            "type": "local_acp",
            "model": model,
            "worker": worker,
            "thread": thread,
            "port": port,
        }
        print(f"[LOCAL] Spawned ACP worker {worker_id} with {model} on port {port}")
        return self.remote_workers[worker_id]

    def _run_acp_worker_loop(self, worker: 'ACPWorker'):
        from worker import Worker
        file_worker = Worker()
        file_worker.running = True
        file_worker.worker_id = worker.worker_id

        while worker.running:
            task = file_worker.get_next_task()
            if not task:
                time.sleep(1)
                continue

            file_worker.current_task = task["id"]
            output = worker.process_task(task["task"])

            usage = worker.get_token_usage()
            file_worker.tokens_used = usage["tokens_used"]
            file_worker.context_limit = usage["context_limit"]

            file_worker.save_result(task["id"], output)

            processing_file = file_worker.PROCESSING_DIR / f"{task['id']}.json"
            if processing_file.exists():
                processing_file.unlink()

            file_worker.current_task = None

    def spawn_remote_worker(self, machine: Machine, model: str, worker_id: str) -> Dict:
        coord_path = self.setup_machine(machine)

        env = {
            "WORKER_ID": worker_id,
            "OPENCODE_MODEL": model,
            "COORD_DIR": str(coord_path),
            **machine.env,
        }

        worker_script = f"""#!/usr/bin/env python3
import sys
sys.path.insert(0, '{coord_path}')
from worker import Worker
Worker().run()
"""

        remote_script = Path(machine.coord_mount) / f"worker_{worker_id}.py"
        stdin, stdout, stderr = self.ssh.connect(machine).exec_command(f"cat > {remote_script} << 'EOF'\n{worker_script}\nEOF")

        cmd = f"cd {machine.coord_mount} && nohup python3 worker_{worker_id}.py > worker_{worker_id}.log 2>&1 & echo $!"
        exit_code, stdout, stderr = self.ssh.run_command(machine, cmd, env)
        pid = stdout.strip() if stdout.strip().isdigit() else None

        self.remote_workers[worker_id] = {
            "type": "remote",
            "machine": machine.name,
            "model": model,
            "pid": pid,
            "remote_script": str(remote_script),
        }
        print(f"[{machine.name}] Spawned remote worker {worker_id} with {model} (PID: {pid})")
        return self.remote_workers[worker_id]

    def check_worker(self, worker_id: str) -> bool:
        worker = self.remote_workers.get(worker_id)
        if not worker:
            return False

        if worker["type"] == "local_acp":
            return worker["worker"].running

        machine = self.machines[worker["machine"]]
        pid = worker.get("pid")
        if pid:
            _, stdout, _ = self.ssh.run_command(machine, f"kill -0 {pid} 2>/dev/null && echo ALIVE || echo DEAD")
            return "ALIVE" in stdout
        return False

    def get_worker_token_usage(self, worker_id: str) -> Dict:
        worker = self.remote_workers.get(worker_id)
        if not worker:
            return {}

        if worker["type"] == "local_acp":
            return worker["worker"].get_token_usage()

        return {"tokens_used": 0, "context_limit": 128000, "usage_ratio": 0}

    def shutdown(self):
        for worker_id, worker in self.remote_workers.items():
            if worker["type"] == "local_acp":
                worker["worker"].shutdown()
            else:
                machine = self.machines[worker["machine"]]
                pid = worker.get("pid")
                if pid:
                    try:
                        self.ssh.run_command(machine, f"kill {pid}")
                    except:
                        pass
        self.ssh.close()


def load_machines(config_path: Path) -> List[Machine]:
    with open(config_path) as f:
        data = json.load(f)
    return [Machine(**m) for m in data.get("machines", [])]


def create_example_config(path: Path):
    config = {
        "machines": [
            {
                "name": "local",
                "host": "localhost",
                "shared_fs": True,
                "coord_mount": os.path.expanduser("~/src/popencode"),
                "max_workers": 3,
                "use_tunnel": False,
            },
            {
                "name": "gpu-box",
                "host": "192.168.1.50",
                "user": "ubuntu",
                "key_path": "~/.ssh/id_ed25519",
                "shared_fs": False,
                "use_tunnel": True,
                "coord_mount": os.path.expanduser("~/src/popencode"),
                "max_workers": 2,
                "models": ["meta-llama/llama-3.1-70b-instruct", "qwen/qwen-2.5-72b-instruct"],
            },
            {
                "name": "mac-studio",
                "host": "192.168.1.60",
                "user": "user",
                "shared_fs": True,
                "coord_mount": "/Volumes/Shared/opencode-coord",
                "max_workers": 4,
                "use_tunnel": False,
            }
        ]
    }
    with open(path, "w") as f:
        json.dump(config, f, indent=2)
    print(f"Created example config at {path}")


if __name__ == "__main__":
    config_path = Path("~/src/popencode/machines.json")
    if not config_path.exists():
        create_example_config(config_path)
        sys.exit(0)

    machines = load_machines(config_path)
    dist = DistributedSupervisor(machines, Path(os.path.expanduser("~/src/popencode")))

    for machine in machines:
        try:
            dist.setup_machine(machine)
            print(f"✓ {machine.name} ready")
        except Exception as e:
            print(f"✗ {machine.name} failed: {e}")

    dist.shutdown()