#!/usr/bin/env python3

import json
import sys 
from subprocess import run, PIPE
from pathlib import Path

def generate_inventory():
    tf_dir = Path(__file__).resolve().parent

    # 1) Get terraform outputs
    cmd = ["terraform", f"-chdir={tf_dir}", "output", "--json"]
    out = run(cmd, capture_output=True, text=True, check=True)
    data = json.loads(out.stdout)

    worker_ips = data["worker_ips"]["value"]
    controller_ip = data["host_ips"]["value"]
    if isinstance(controller_ip, list):
        controller_ip = controller_ip[0]

    hostvars = {}
    groups = {}

    # Controller group
    host_name = "controller-node"
    hostvars[host_name] = {
        "ansible_host": controller_ip,
        "ansible_user": "almalinux",
    }
    groups["controller"] = {"hosts": [host_name]}

    # Workers group
    worker_names = []
    for i, ip in enumerate(worker_ips, start=1):
        name = f"worker-node{i}"
        worker_names.append(name)
        hostvars[name] = {
            "ansible_host": ip,
            "ansible_user": "almalinux",
        }
    groups["workers"] = {"hosts": worker_names}

    # All group
    groups["all"] = {"children": ["controller", "workers"]}

    inventory = {
        "_meta": {"hostvars": hostvars},
        **groups,
    }

    return json.dumps(inventory, indent=4)

if __name__ == "__main__":
    inv = generate_inventory()
    print(inv)