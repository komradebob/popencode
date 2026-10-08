#!/usr/bin/env python3
"""
SSH Host Discovery Utility for Popencode

Discovers passwordless SSH hosts from ~/.ssh, validates them,
builds jump host chains, and updates machines.json.
"""

import os
import sys
import json
import subprocess
import paramiko
from pathlib import Path
from typing import Dict, List, Set, Optional, Tuple
from dataclasses import dataclass, asdict
import re


@dataclass
class SSHHost:
    name: str
    hostname: str
    user: str
    port: int = 22
    key_path: Optional[str] = None
    jump_host: Optional[str] = None
    jump_user: Optional[str] = None
    jump_port: int = 22
    source: str = "discovered"


class SSHConfigParser:
    """Parses SSH config files and extracts host information."""

    def __init__(self):
        self.config_path = Path.home() / ".ssh" / "config"
        self.known_hosts_path = Path.home() / ".ssh" / "known_hosts"
        self.authorized_keys_path = Path.home() / ".ssh" / "authorized_keys"

    def parse_config(self) -> Dict[str, Dict]:
        """Parse ~/.ssh/config and return host configurations."""
        if not self.config_path.exists():
            return {}

        hosts = {}
        current_host = None
        current_config = {}

        with open(self.config_path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue

                parts = line.split(None, 1)
                if len(parts) < 2:
                    continue

                keyword, value = parts[0].lower(), parts[1]

                if keyword == 'host':
                    if current_host:
                        hosts[current_host] = current_config
                    current_host = value
                    current_config = {'host_patterns': value.split()}
                elif current_host:
                    current_config[keyword] = value

        if current_host:
            hosts[current_host] = current_config

        return hosts

    def parse_known_hosts(self) -> Set[str]:
        """Extract unique hostnames from known_hosts."""
        hosts = set()
        if not self.known_hosts_path.exists():
            return hosts

        with open(self.known_hosts_path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                parts = line.split()
                if parts:
                    hostnames = parts[0].split(',')
                    for h in hostnames:
                        h = h.split(']')[0].lstrip('[')
                        hosts.add(h)
        return hosts

    def get_all_configured_hosts(self) -> Dict[str, Dict]:
        """Get all hosts from config with resolved parameters."""
        raw_hosts = self.parse_config()
        resolved = {}

        for pattern, config in raw_hosts.items():
            if '*' in pattern or '?' in pattern:
                continue

            hostname = config.get('hostname', pattern)
            user = config.get('user', os.environ.get('USER', 'ubuntu'))
            port = int(config.get('port', 22))
            key_path = config.get('identityfile')
            proxy_jump = config.get('proxyjump')

            if key_path:
                key_path = os.path.expanduser(key_path.replace('~', str(Path.home())))

            resolved[pattern] = {
                'hostname': hostname,
                'user': user,
                'port': port,
                'key_path': key_path,
                'proxy_jump': proxy_jump,
            }

        return resolved


class SSHKeyDetector:
    """Detects available SSH keys for passwordless auth."""

    def __init__(self):
        self.ssh_dir = Path.home() / ".ssh"

    def find_private_keys(self) -> List[Path]:
        """Find all private key files."""
        keys = []
        for pattern in ['id_*', '*.pem', '*.key']:
            keys.extend(self.ssh_dir.glob(pattern))
        private_keys = []
        for k in keys:
            if not k.name.endswith('.pub') and k.name not in ['known_hosts', 'config', 'authorized_keys', 'environment']:
                private_keys.append(k)
        return private_keys

    def test_key_auth(self, host: str, user: str, port: int, key_path: Path,
                      jump_host: Optional[str] = None) -> bool:
        """Test if a key works for passwordless auth."""
        try:
            cmd = [
                'ssh', '-o', 'BatchMode=yes',
                '-o', 'ConnectTimeout=5',
                '-o', 'PasswordAuthentication=no',
                '-o', 'StrictHostKeyChecking=no',
                '-o', 'UserKnownHostsFile=/dev/null',
                '-i', str(key_path),
                '-p', str(port),
            ]
            if jump_host:
                cmd.extend(['-J', jump_host])

            cmd.append(f'{user}@{host}')
            cmd.append('echo OK')

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
            return result.returncode == 0 and 'OK' in result.stdout
        except Exception:
            return False

    def find_working_keys(self, host: str, user: str, port: int,
                          jump_host: Optional[str] = None) -> List[Path]:
        """Find all keys that work for a host."""
        working = []
        for key in self.find_private_keys():
            if self.key_detector.test_key_auth(host, user, port, key, jump_host):
                working.append(key)
        return working


class HostValidator:
    """Validates SSH connectivity and discovers reachable hosts."""

    def __init__(self):
        self.key_detector = SSHKeyDetector()
        self.config_parser = SSHConfigParser()

    def test_direct_connection(self, host: str, user: str, port: int,
                                key_path: Optional[Path] = None) -> Tuple[bool, Optional[Path]]:
        """Test direct SSH connection, return (success, working_key)."""
        keys_to_try = [key_path] if key_path else self.key_detector.find_private_keys()

        for key in keys_to_try:
            if self.key_detector.test_key_auth(host, user, port, key):
                return True, key
        return False, None

    def test_jump_connection(self, target_host: str, target_user: str, target_port: int,
                              jump_host: str, jump_user: str, jump_port: int,
                              target_key: Optional[Path] = None,
                              jump_key: Optional[Path] = None) -> Tuple[bool, Optional[Path], Optional[Path]]:
        """Test connection via jump host."""
        jump_keys = [jump_key] if jump_key else self.key_detector.find_private_keys()
        target_keys = [target_key] if target_key else self.key_detector.find_private_keys()

        for jkey in jump_keys:
            if not self.key_detector.test_key_auth(jump_host, jump_user, jump_port, jkey):
                continue

            jump_spec = f'{jump_user}@{jump_host}:{jump_port}'
            for tkey in target_keys:
                if self.key_detector.test_key_auth(target_host, target_user, target_port, tkey, jump_spec):
                    return True, tkey, jkey

        return False, None, None

    def discover_reachable_from(self, jump_host: SSHHost) -> List[SSHHost]:
        """From a jump host, discover what other hosts it can reach."""
        discovered = []

        try:
            cmd = [
                'ssh', '-o', 'BatchMode=yes',
                '-o', 'ConnectTimeout=10',
                '-o', 'StrictHostKeyChecking=no',
            ]
            if jump_host.key_path:
                cmd.extend(['-i', str(jump_host.key_path)])
            cmd.extend(['-p', str(jump_host.port)])
            cmd.append(f'{jump_host.user}@{jump_host.hostname}')
            cmd.append('cat ~/.ssh/config ~/.ssh/known_hosts 2>/dev/null || true')

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if result.returncode != 0:
                return discovered

            remote_hosts = self._parse_remote_config(result.stdout)
            for rh in remote_hosts:
                success, tkey, jkey = self.test_jump_connection(
                    rh['hostname'], rh['user'], rh['port'],
                    jump_host.hostname, jump_host.user, jump_host.port,
                    rh.get('key_path'), jump_host.key_path
                )
                if success:
                    discovered.append(SSHHost(
                        name=f"{jump_host.name}-{rh['hostname']}",
                        hostname=rh['hostname'],
                        user=rh['user'],
                        port=rh['port'],
                        key_path=str(tkey) if tkey else None,
                        jump_host=jump_host.hostname,
                        jump_user=jump_host.user,
                        jump_port=jump_host.port,
                        source=f"discovered via {jump_host.name}"
                    ))
        except Exception as e:
            print(f"  Error discovering from {jump_host.name}: {e}")

        return discovered

    def _parse_remote_config(self, output: str) -> List[Dict]:
        """Parse SSH config/known_hosts output from remote host."""
        hosts = []
        current = {}
        for line in output.split('\n'):
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts = line.split(None, 1)
            if len(parts) < 2:
                continue
            keyword, value = parts[0].lower(), parts[1]
            if keyword == 'host':
                if current and 'hostname' in current:
                    hosts.append(current)
                current = {'host': value}
            elif keyword == 'hostname':
                current['hostname'] = value
            elif keyword == 'user':
                current['user'] = value
            elif keyword == 'port':
                current['port'] = int(value)
            elif keyword == 'identityfile':
                current['key_path'] = value
        if current and 'hostname' in current:
            hosts.append(current)
        return hosts


class JumpChainBuilder:
    """Builds SSH jump host chains recursively."""

    def __init__(self, validator: HostValidator):
        self.validator = validator
        self.discovered_hosts: Dict[str, SSHHost] = {}
        self.chains: Dict[str, List[str]] = {}

    def build_chains(self, initial_hosts: List[SSHHost]) -> Dict[str, SSHHost]:
        """Recursively build jump chains from initial hosts."""
        for host in initial_hosts:
            self.discovered_hosts[host.name] = host
            self.chains[host.name] = []

        queue = list(initial_hosts)
        visited = set(h.name for h in initial_hosts)

        while queue:
            current = queue.pop(0)
            if current.name in visited and len(self.chains[current.name]) > 0:
                continue

            print(f"  Discovering from {current.name} ({current.hostname})...")
            new_hosts = self.validator.discover_reachable_from(current)

            for nh in new_hosts:
                chain_name = nh.name
                if chain_name not in self.discovered_hosts:
                    self.discovered_hosts[chain_name] = nh
                    self.chains[chain_name] = self.chains[current.name] + [current.name]
                    queue.append(nh)
                    print(f"    Found: {chain_name} via {current.name}")

        return self.discovered_hosts


class MachineConfigUpdater:
    """Updates machines.json with discovered hosts."""

    def __init__(self, machines_path: Path):
        self.machines_path = machines_path

    def load_existing(self) -> List[Dict]:
        if self.machines_path.exists():
            with open(self.machines_path) as f:
                return json.load(f).get('machines', [])
        return []

    def save(self, machines: List[Dict]):
        config = {'machines': machines}
        with open(self.machines_path, 'w') as f:
            json.dump(config, f, indent=2)

    def merge_hosts(self, existing: List[Dict], discovered: Dict[str, SSHHost]) -> List[Dict]:
        """Merge discovered hosts with existing config."""
        existing_names = {m['name'] for m in existing}
        merged = list(existing)

        for name, host in discovered.items():
            if name in existing_names:
                continue

            machine = {
                'name': name,
                'host': host.hostname,
                'user': host.user,
                'port': host.port,
                'shared_fs': False,
                'use_tunnel': True,
                'coord_mount': '~/src/popencode',
                'max_workers': 2,
            }
            if host.key_path:
                machine['key_path'] = host.key_path
            if host.jump_host:
                machine['jump_host'] = host.jump_host
                machine['jump_user'] = host.jump_user
                machine['jump_port'] = host.jump_port
                self._ensure_jump_config(host)

            merged.append(machine)

        return merged

    def _ensure_jump_config(self, host: SSHHost):
        """Add ProxyJump entry to SSH config for jump chains."""
        config_path = Path.home() / ".ssh" / "config"
        config_path.parent.mkdir(exist_ok=True)

        existing = ""
        if config_path.exists():
            with open(config_path) as f:
                existing = f.read()

        if f"Host {host.name}" in existing:
            return

        jump_chain = []
        if host.jump_host:
            jump_chain.append(f"{host.jump_user}@{host.jump_host}:{host.jump_port}")

        entry = f"""
Host {host.name}
    HostName {host.hostname}
    User {host.user}
    Port {host.port}
"""
        if host.key_path:
            entry += f"    IdentityFile {host.key_path}\n"
        if jump_chain:
            entry += f"    ProxyJump {','.join(jump_chain)}\n"

        with open(config_path, 'a') as f:
            f.write(entry)
        print(f"  Added SSH config entry for {host.name}")


def main():
    print("=== Popencode SSH Host Discovery ===\n")

    machines_path = Path.home() / "src" / "popencode" / "machines.json"

    print("1. Parsing ~/.ssh/config...")
    parser = SSHConfigParser()
    configured = parser.get_all_configured_hosts()
    print(f"   Found {len(configured)} configured hosts")

    print("\n2. Testing direct passwordless connections...")
    validator = HostValidator()
    direct_hosts = []

    for name, config in configured.items():
        if name == '*':
            continue
        print(f"   Testing {name} ({config['hostname']})...", end=' ')
        success, key = validator.test_direct_connection(
            config['hostname'], config['user'], config['port'],
            Path(config['key_path']) if config.get('key_path') else None
        )
        if success:
            print("✓")
            direct_hosts.append(SSHHost(
                name=name,
                hostname=config['hostname'],
                user=config['user'],
                port=config['port'],
                key_path=str(key) if key else None,
                source="direct"
            ))
        else:
            print("✗")

    print(f"\n   Directly accessible: {len(direct_hosts)} hosts")

    print("\n3. Building jump host chains...")
    builder = JumpChainBuilder(validator)
    all_hosts = builder.build_chains(direct_hosts)
    print(f"   Total discovered: {len(all_hosts)} hosts")

    print("\n4. Updating machines.json...")
    updater = MachineConfigUpdater(machines_path)
    existing = updater.load_existing()
    merged = updater.merge_hosts(existing, all_hosts)
    updater.save(merged)
    print(f"   Updated with {len(merged)} total machines")

    print("\n=== Discovered Machines ===")
    for m in merged:
        jump_info = ""
        if m.get('jump_host'):
            jump_info = f" via {m['jump_user']}@{m['jump_host']}:{m['jump_port']}"
        print(f"  {m['name']}: {m['user']}@{m['host']}:{m['port']}{jump_info}")

    print(f"\nRun: ./popencode test-ssh  # To verify all connections")
    print(f"Run: ./popencode start   # To launch with new machines")


if __name__ == "__main__":
    main()