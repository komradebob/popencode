#!/usr/bin/env python3
"""
Setup script: Configures OpenRouter credentials for opencode.
"""

import json
import os
import subprocess
from pathlib import Path

AUTH_FILE = Path.home() / ".local" / "share" / "opencode" / "auth.json"

def setup_openrouter():
    api_key = os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        print("OPENROUTER_API_KEY not set in environment")
        print("Get a free key at: https://openrouter.ai/keys")
        api_key = input("Enter OpenRouter API key: ").strip()

    if not api_key:
        print("No API key provided")
        return False

    auth_data = {
        "credentials": [
            {
                "provider": "openrouter",
                "method": "api_key",
                "value": api_key
            }
        ]
    }

    AUTH_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(AUTH_FILE, "w") as f:
        json.dump(auth_data, f, indent=2)

    print(f"Saved credentials to {AUTH_FILE}")

    result = subprocess.run(["opencode", "providers", "list"], capture_output=True, text=True)
    print(result.stdout)
    return True

if __name__ == "__main__":
    setup_openrouter()