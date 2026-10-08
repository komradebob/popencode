#!/usr/bin/env python3
"""
Launcher: Starts the supervisor with initial worker pool.
Supports distributed mode with SSH tunnels and ACP protocol.
"""

import os
import sys
import subprocess
import time
import argparse
from pathlib import Path

COORD_DIR = Path(os.path.expanduser("~/src/popencode"))


def check_opencode():
    try:
        result = subprocess.run(["opencode", "--version"], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"Found opencode: {result.stdout.strip()}")
            return True
    except FileNotFoundError:
        pass
    print("ERROR: opencode not found in PATH")
    print("Install with: npm install -g @opencode-ai/opencode")
    return False


def check_openrouter_auth():
    auth_file = Path.home() / ".local" / "share" / "opencode" / "auth.json"
    if auth_file.exists():
        print("OpenRouter credentials found")
        return True
    print("OpenRouter credentials not configured")
    print("Run: python3 ~/src/popencode/setup_openrouter.py")
    print("Or set OPENROUTER_API_KEY and run setup")
    return False


def cleanup():
    import shutil
    if COORD_DIR.exists():
        print(f"Cleaning up {COORD_DIR}")
        shutil.rmtree(COORD_DIR)
    COORD_DIR.mkdir(parents=True, exist_ok=True)
    (COORD_DIR / "queue").mkdir(exist_ok=True)
    (COORD_DIR / "processing").mkdir(exist_ok=True)
    (COORD_DIR / "results").mkdir(exist_ok=True)
    (COORD_DIR / "locks").mkdir(exist_ok=True)
    (COORD_DIR / "status").mkdir(exist_ok=True)
    (COORD_DIR / "models").mkdir(exist_ok=True)


def create_machine_config():
    config = {
        "machines": [
            {
                "name": "local",
                "host": "localhost",
                "shared_fs": True,
                "coord_mount": os.path.expanduser("~/src/popencode"),
                "max_workers": 3,
                "use_tunnel": False,
            }
        ]
    }
    import json
    with open(COORD_DIR / "machines.json", "w") as f:
        json.dump(config, f, indent=2)
    print(f"Created default machine config at {COORD_DIR / 'machines.json'}")


def test_ssh_connection(machine_config: dict) -> bool:
    host = machine_config.get("host", "localhost")
    user = machine_config.get("user", os.environ.get("USER", "ubuntu"))
    port = machine_config.get("port", 22)
    key_path = machine_config.get("key_path")

    if host in ("localhost", "127.0.0.1"):
        return True

    cmd = ["ssh", "-o", "ConnectTimeout=5", "-o", "BatchMode=yes", "-p", str(port)]
    if key_path:
        cmd.extend(["-i", os.path.expanduser(key_path)])
    cmd.append(f"{user}@{host}")
    cmd.append("echo OK")

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            print(f"  ✓ SSH to {user}@{host}:{port} OK")
            return True
        else:
            print(f"  ✗ SSH to {user}@{host}:{port} failed: {result.stderr.strip()}")
            return False
    except Exception as e:
        print(f"  ✗ SSH to {user}@{host}:{port} error: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="OpenCode Distributed Supervisor Launcher")
    parser.add_argument("--no-cleanup", action="store_true", help="Don't clean previous runs")
    parser.add_argument("--config", help="Path to machines.json config")
    parser.add_argument("--test-ssh", action="store_true", help="Test SSH connections and exit")
    args = parser.parse_args()

    print("=== OpenCode Distributed Supervisor ===\n")

    if not check_opencode():
        sys.exit(1)

    if not check_openrouter_auth():
        sys.exit(1)

    if not args.no_cleanup:
        print("\nCleaning up previous runs...")
        cleanup()

    if args.config:
        import shutil
        shutil.copy(args.config, COORD_DIR / "machines.json")
        print(f"Using config: {args.config}")
    elif not (COORD_DIR / "machines.json").exists():
        create_machine_config()

    if args.test_ssh:
        import json
        with open(COORD_DIR / "machines.json") as f:
            data = json.load(f)
        print("\nTesting SSH connections...")
        for m in data.get("machines", []):
            test_ssh_connection(m)
        return

    print("\nStarting supervisor...")
    supervisor_path = COORD_DIR / "supervisor.py"

    try:
        subprocess.run([sys.executable, str(supervisor_path)], check=True)
    except KeyboardInterrupt:
        print("\nShutdown requested")
    except subprocess.CalledProcessError as e:
        print(f"Supervisor exited with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()