# Literature Semantic Search - Distributed Data Analysis System

This repository contains a distributed semantic search system for academic literature, deployed on a small cluster consisting of one `controller-node` and four `worker-node` instances. Developed for the UCL module COMP0239 – Engineering for Data Analysis 2, the system follows a controller/worker architecture using Dask, Prefect, and MinIO as its core components.

The system is designed to:
- distribute PDF fetching (via GROBID ML tool), text extraction, and embedding generation (via Sentence Transformers ML model) across multiple worker nodes,
- orchestrate the full indexing pipeline from the controller node via Prefect with live monitoring,
- store embeddings persistently in MinIO object storage,
- build a FAISS semantic search index from the generated embeddings,
- expose a user-facing Streamlit interface for indexing and querying without writing code,
- and support the user-friendly submission of new data from which to build an index as well as result retrieval.

## Pipeline Overview

The system implements a two-phase pipeline:

**Phase 1 — Indexing** (distributed, compute-intensive):
A user submits an indexing run via the Streamlit UI or Prefect UI, specifying a dataset of arXiv paper IDs and a maximum paper count. The Prefect server receives and schedules the flow run, persisting its state and logs. The Prefect worker picks up the scheduled run and submits individual paper processing tasks to the Dask scheduler, which distributes them across the 20 Dask worker processes. Each worker independently fetches a PDF from arXiv, posts it to a local GROBID instance for ML-based text extraction, generates a semantic embedding using the `all-MiniLM-L6-v2` Sentence Transformers model, and uploads the resulting embedding vector to MinIO object storage. Once all tasks complete, the controller builds a FAISS index from the collected embeddings and stores it on disk.

**Phase 2 — Querying** (lightweight, runs on controller):
A user submits a natural language research question via the Streamlit UI. The Prefect server schedules the query flow run, which the Prefect worker executes locally on the controller — no Dask distribution. The query is embedded using the same Sentence Transformers model and compared against the FAISS index using cosine similarity. The *top-k* most semantically similar papers are returned with their arXiv links and similarity scores, and stored in MinIO for retrieval.

## Cluster Overview

The system is deployed across 5 virtual machines:

| Node | Role | Specs | Services |
|---|---|---|---|
| `controller-node` | Scheduler, Orchestrator, and Store | 2 CPU, 8GB RAM, 52GB disk | Dask Scheduler, Prefect Server, Prefect Worker, MinIO, Prometheus, Streamlit |
| `worker-node1` | Compute | 5 CPU, 33GB RAM, 112GB disk | Dask Worker (×5), GROBID, Node Exporter |
| `worker-node2` | Compute | 5 CPU, 33GB RAM, 112GB disk | Dask Worker (×5), GROBID, Node Exporter |
| `worker-node3` | Compute | 5 CPU, 33GB RAM, 112GB disk | Dask Worker (×5), GROBID, Node Exporter |
| `worker-node4` | Compute | 5 CPU, 33GB RAM, 112GB disk | Dask Worker (×5), GROBID, Node Exporter |

Throughout this **README**, commands are prefixed with the node they should be run on:
- `local` — your local machine or VM used to provision the mini-cluster
- `controller-node` — the controller/host VM
- `worker-nodeX` — any worker VM

## 1. Provisioning the cluster

### 1.1 Install prerequisites

From `local` — install **Git** and **Terraform**:
```bash
# macOS (Homebrew)
brew install git terraform

# AlmaLinux/RHEL
dnf install -y git
dnf install -y yum-utils
yum-config-manager --add-repo https://rpm.releases.hashicorp.com/RHEL/hashicorp.repo
dnf install -y terraform
```

### 1.2 Clone the repository

From `local` — clone and navigate into the repository:
```bash
git clone <gitlab-repo-url>
cd cw2-eda
```

Repository structure:
```
cw2-eda/
├── ansible/          # configuration & service setup
├── build_cluster/    # terraform infrastructure
├── code/             # pipeline code, flows, and UI
├── README.md
└── .gitignore
```

### 1.3 Execute Terraform

From `local` — within the `build_cluster/` directory:
```bash
cd build_cluster
terraform init
terraform apply
```

This creates:
- 1 `controller-node` (controller/host) VM — 2 CPU, 8GB RAM, 52GB disk
- 4 `worker-node` VMs — 5 CPU, 33GB RAM, 112GB disk each

### 1.4 Generate inventory

From `local` — within the `build_cluster/` directory:
```bash
python3 generate_inventory.py
```

This writes `inventory.json` directly to the `build_cluster/` directory.

### 1.5 Commit and push inventory

From `local` — commit the generated inventory so it is available on the `controller-node` after cloning:
```bash
git add build_cluster/inventory.json
git commit -m "Add newly generated inventory"
git push
```

## 2. Setting up the repository

### 2.1 SSH into `controller-node`

From `local` — connect with agent forwarding enabled:
```bash
ssh -A -J condenser almalinux@<controller-node-ip> -i ~/.ssh/key
```

Where `condenser` is defined in `~/.ssh/config`:
```
Host condenser
  HostName ssh.condenser.arc.ucl.ac.uk
  User cloud-user
  CertificateFile ~/.ssh/comp0235_signed
  IdentityFile ~/.ssh/comp0235
  ForwardAgent yes
```

The `-A` flag enables SSH agent forwarding, allowing the `controller-node` to authenticate to worker nodes and GitHub using keys already loaded in your local SSH agent.

### 2.2 Install prerequisites

From `controller-node` — install **Git**, **pip**, and **Ansible**:
```bash
dnf install -y git python3-pip
pip3 install ansible
```

### 2.3 Clone the repository

From `controller-node` — clone and navigate into the repository:
```bash
cd ~
git clone <gitlab-repo-url>
cd cw2-eda
```

The repository includes the latest `inventory.json` generated in step 1.4.

## 3. Configuring the cluster

### 3.1 Set vault password

From `controller-node` — the repository uses Ansible Vault to store sensitive credentials (Kaggle API key, MinIO password). Create the vault password file:
```bash
echo "vault_password" > ~/.vault_pass
chmod 600 ~/.vault_pass
```

> **Note:** The vault password is shared separately and is not stored in the repository. `ansible.cfg` is configured to read it automatically from this path — no flag or prompt is required when running playbooks.

### 3.2 Run the master playbook

From `controller-node` — within the `ansible/` directory:
```bash
cd ~/cw2-eda/ansible
ansible-playbook playbooks/site.yml
```

This single playbook configures all 5 machines, ensuring latest packages, syncing pipeline code, fetching the default dataset, and installing and starting the following services:

| Service | Node | Purpose |
|---|---|---|
| Node Exporter | all | Hardware metrics collection |
| Dask Scheduler | controller | Distributes tasks to workers |
| MinIO | controller | Object storage for embeddings and results |
| Prefect Server | controller | Orchestration backend + UI |
| Prefect Worker | controller | Picks up and runs flow deployments |
| Prometheus | controller | Centralised metrics storage |
| Streamlit | controller | User-facing web interface |
| Dask Workers (×20) | workers | Execute pipeline tasks |
| GROBID | workers | ML-based PDF text extraction |

> **Note:** GROBID builds from source on first startup using Gradle, taking approximately 5–10 minutes per worker node. The playbook will complete before GROBID finishes initialising. Verify with `systemctl status grobid-server` on any worker node.

## 4. Running the pipeline

### 4.1 Default dataset

The Ansible playbook automatically downloads the arXiv metadata dataset from Kaggle and uploads it to MinIO during setup at `papers/arxiv-metadata.json`. When submitting an indexing run, the flow streams this file and extracts the `id` field from each paper entry.

- Kaggle dataset: https://www.kaggle.com/datasets/Cornell-University/arxiv

### 4.2 Custom dataset

To index a custom set of arXiv papers, prepare a JSON file containing a list of arXiv paper IDs:
```json
["2301.00001", "2301.00002", "2301.00003"]
```

**Via Streamlit (recommended):** Navigate to the **Index Management** tab, select **Upload custom paper IDs**, upload the file, then launch the indexing flow.

**Via MinIO console:** Upload directly to the MinIO bucket (see Section 5.5), note the object key, and specify it when triggering the flow.

### 4.3 Submit an indexing run

**Via Streamlit (recommended):** Navigate to the **Index Management** tab, choose the dataset source, set **Max papers to index**, and click **Launch Indexing Flow**.

**Via CLI:** From `controller-node`:
```bash
export PREFECT_API_URL=http://controller-node:4200/api
prefect deployment run 'indexing-flow/indexing-flow' \
    -p "paper_ids_key=papers/arxiv-metadata.json" \
    -p "max_papers=50000"
```

> Override `paper_ids_key` to point to a custom uploaded file.

### 4.4 Submit a query

**Via Streamlit (recommended):** Navigate to the **Semantic Search** tab, enter a natural language research question, set the number of results, and click **Search**.

**Via CLI:** From `controller-node`:
```bash
export PREFECT_API_URL=http://controller-node:4200/api
prefect deployment run 'query-flow/query-flow' \
    -p "query=distributed computing for machine learning" \
    -p "top_k=10"
```

## 5. Monitoring and Interfaces

The system exposes several web-based interfaces for monitoring and interaction.

### 5.1 Prometheus
Collects hardware metrics (CPU, RAM, disk, network) from all 5 machines via Node Exporter.
 
> Access URL: https://prometheus-ucabtg2.comp0235.condenser.arc.ucl.ac.uk
 
Useful queries:
- CPU usage per node: `100 - (avg by(node) (rate(node_cpu_seconds_total{mode="idle"}[1m])) * 100)`
- Memory available: `node_memory_MemAvailable_bytes`
- Disk usage: `node_filesystem_avail_bytes`

### 5.2 Prefect UI
Provides pipeline-level monitoring: flow run history, task states, logs, and deployment management.
 
> Access URL: https://prefect-ucabtg2.comp0235.condenser.arc.ucl.ac.uk
 
### 5.3 Streamlit UI
User-facing interface for submitting indexing runs, querying the index, and retrieving results.
 
> Access URL: https://literature-sem-search-ucabtg2.comp0235.condenser.arc.ucl.ac.uk
 
Features:
- Launch indexing flow with default or custom paper IDs
- Monitor indexing progress with auto-refresh
- Download FAISS index and metadata
- Submit semantic search queries
- View ranked results with arXiv links
- Download results as CSV
- Persistent search history

### 5.4 Dask Dashboard
Provides real-time visibility into worker utilisation, task stream, and memory usage across all 20 worker processes.
 
> Access URL: https://dask-ucabtg2.comp0235.condenser.arc.ucl.ac.uk
 
### 5.5 MinIO Console
Object storage browser for inspecting stored embeddings, results, and dataset files.
 
> Access URL: https://minio-ucabtg2.comp0235.condenser.arc.ucl.ac.uk
 
Default credentials:
- Username: `minioadmin`
- Password: (see vault)

## 6. Troubleshooting

Different services run on different node types:
- `controller-node`: Dask Scheduler, Prefect Server, Prefect Worker, MinIO, Prometheus, Streamlit
- `worker-nodeX`: Dask Workers, GROBID, Node Exporter

### Check service status

From `controller-node`:
```bash
systemctl status dask-scheduler
systemctl status prefect-server
systemctl status prefect-worker
systemctl status minio-server
systemctl status prometheus
systemctl status streamlit
```

From `worker-nodeX`:
```bash
systemctl status dask-worker
systemctl status grobid-server
systemctl status node-exporter
```

### Check logs

From `controller-node`:
```bash
journalctl -u dask-scheduler -f
journalctl -u prefect-server -f
journalctl -u prefect-worker -f
journalctl -u streamlit -f
```

From `worker-nodeX`:
```bash
journalctl -u dask-worker -f
journalctl -u grobid-server -f
```

### Verify Dask cluster

From `controller-node`:
```bash
source /opt/literature-sem-search/venv/bin/activate
python3 -c "
from dask.distributed import Client
client = Client('tcp://controller-node:8786')
print(client)
"
```

Expected output: `<Client: 'tcp://controller-node:8786' processes=20 threads=20, memory=111.76 GiB>`

### Verify Prefect deployments

From `controller-node`:
```bash
export PREFECT_API_URL=http://controller-node:4200/api
prefect deployment ls
```

### Verify MinIO and embeddings

From `controller-node`:
```bash
source /opt/literature-sem-search/venv/bin/activate
python3 -c "
from minio import Minio
client = Minio('controller-node:9000', access_key='minioadmin', secret_key='<minio-password>', secure=False)
objects = list(client.list_objects('lit-sem-search-bucket', prefix='embeddings/'))
print(f'{len(objects)} embeddings in MinIO')
"
```

### Verify FAISS index

From `controller-node`:
```bash
source /opt/literature-sem-search/venv/bin/activate
python3 -c "
import faiss
index = faiss.read_index('/opt/literature-sem-search/data/faiss.index')
print(f'Index contains {index.ntotal} vectors')
"
```

### Restart all services

From `controller-node` — Ansible handles remote execution on the appropriate nodes:
```bash
# Controller services
ansible controller -m systemd -a "name=dask-scheduler state=restarted" --become
ansible controller -m systemd -a "name=minio-server state=restarted" --become
ansible controller -m systemd -a "name=prefect-server state=restarted" --become
ansible controller -m systemd -a "name=prefect-worker state=restarted" --become
ansible controller -m systemd -a "name=prometheus state=restarted" --become
ansible controller -m systemd -a "name=streamlit state=restarted" --become

# Worker services
ansible workers -m systemd -a "name=dask-worker state=restarted" --become
ansible workers -m systemd -a "name=grobid-server state=restarted" --become
ansible workers -m systemd -a "name=node-exporter state=restarted" --become
```