#!/usr/bin/env bash
set -euo pipefail

CLUSTER_DIR="${SLURM_DOCKER_CLUSTER_DIR:-/mnt/pikachu/slurm-docker-cluster}"
REPO_URL="${SLURM_DOCKER_CLUSTER_REPO:-https://github.com/giovtorres/slurm-docker-cluster.git}"

if [ ! -d "${CLUSTER_DIR}/.git" ]; then
  git clone "${REPO_URL}" "${CLUSTER_DIR}"
fi

cd "${CLUSTER_DIR}"

if [ ! -f .env ]; then
  cp .env.example .env
fi

python3 - <<'PY'
from pathlib import Path
p = Path(".env")
text = p.read_text()
if "SSH_ENABLE=false" in text:
    text = text.replace("SSH_ENABLE=false", "SSH_ENABLE=true")
elif "SSH_ENABLE=true" not in text:
    text += "\nSSH_ENABLE=true\n"
p.write_text(text)
PY

docker pull giovtorres/slurm-docker-cluster:latest
docker tag giovtorres/slurm-docker-cluster:latest slurm-docker-cluster:25.11.2
make up
docker exec slurmctld bash -lc 'command -v rsync >/dev/null 2>&1 || dnf install -y rsync'
make status

# Refresh the known_hosts entry for the SLURM login node.  The container
# generates new host keys on every fresh start, so any stale entry must be
# replaced; otherwise the Go SLURM backend will refuse the SSH handshake.
ssh-keygen -R '[localhost]:3022' 2>/dev/null || true
ssh-keyscan -p 3022 -H localhost >> "${HOME}/.ssh/known_hosts" 2>/dev/null
echo "known_hosts entry refreshed for [localhost]:3022"
