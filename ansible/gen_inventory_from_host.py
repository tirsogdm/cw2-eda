#!/usr/bin/env python3
import sys, json
import Path

script_dir = Path(__file__).resolve().parent
inventory_path = script_dir.parent / "build_cluster" / "inventory.json"

with open(inventory_path, "r") as f:
    inventory = json.load(f)

if "--list" in sys.argv:
    print(json.dumps(inventory))
elif "--host" in sys.argv:
    print(json.dumps({}))
else:
    print(json.dumps(inventory))