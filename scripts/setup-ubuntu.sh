#!/usr/bin/env bash
# One-time host preparation for Ubuntu 22.04 / 24.04 (cloud VM). Run as a sudo-capable user:
#   bash scripts/setup-ubuntu.sh
# Installs Docker Engine + Compose v2 from Docker's official apt repository, adds a 4 GB swap
# file if the VM has none, and enables a deny-by-default firewall that only allows SSH.
set -euo pipefail

if [[ $EUID -eq 0 ]]; then SUDO=""; else SUDO="sudo"; fi

echo "==> Docker Engine"
$SUDO apt-get update -y
$SUDO apt-get install -y ca-certificates curl gnupg ufw
$SUDO install -m 0755 -d /etc/apt/keyrings
$SUDO curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
$SUDO chmod a+r /etc/apt/keyrings/docker.asc
# shellcheck disable=SC1091
. /etc/os-release
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu ${UBUNTU_CODENAME:-$VERSION_CODENAME} stable" \
  | $SUDO tee /etc/apt/sources.list.d/docker.list > /dev/null
$SUDO apt-get update -y
$SUDO apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
$SUDO systemctl enable --now docker
if [[ -n "${SUDO_USER:-${USER:-}}" && "${SUDO_USER:-${USER:-}}" != "root" ]]; then
  $SUDO usermod -aG docker "${SUDO_USER:-$USER}"
  echo "   added ${SUDO_USER:-$USER} to the docker group (log out/in to apply)"
fi

echo "==> Swap (Kafka + Connect + Airflow are memory hungry)"
if [[ -z "$(swapon --show --noheadings)" ]]; then
  $SUDO fallocate -l 4G /swapfile
  $SUDO chmod 600 /swapfile
  $SUDO mkswap /swapfile
  $SUDO swapon /swapfile
  grep -q '^/swapfile' /etc/fstab || echo '/swapfile none swap sw 0 0' | $SUDO tee -a /etc/fstab > /dev/null
else
  echo "   swap already configured"
fi

echo "==> Firewall (SSH only)"
$SUDO ufw default deny incoming
$SUDO ufw default allow outgoing
$SUDO ufw allow OpenSSH
$SUDO ufw --force enable
echo "   NOTE: Docker-published ports bypass UFW. The compose file therefore binds to 127.0.0.1"
echo "   by default; keep it that way and use an SSH tunnel (see README)."

echo "==> Done. Re-login, then: scripts/init-env.sh && docker compose up -d --build"
