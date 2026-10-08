#!/usr/bin/env bash
# Provisions the RF Alert Manager VM. Idempotent: safe to re-run with `vagrant provision`.
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

RUNNER_USER=runner
RUNNER_DIR=/opt/actions-runner
APP_DIR=/opt/rfam

echo "==> Base packages"
apt-get update -y
apt-get install -y ca-certificates curl gnupg jq ufw

echo "==> Docker Engine + compose plugin (official Docker repo)"
if ! command -v docker >/dev/null 2>&1; then
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
  chmod a+r /etc/apt/keyrings/docker.asc
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
    > /etc/apt/sources.list.d/docker.list
  apt-get update -y
  apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
fi

echo "==> Bounded container logs (10MB x 3 files per container)"
cat > /etc/docker/daemon.json <<'EOF'
{
  "log-driver": "json-file",
  "log-opts": { "max-size": "10m", "max-file": "3" }
}
EOF
systemctl enable docker
systemctl restart docker

echo "==> Bounded system journal"
mkdir -p /etc/systemd/journald.conf.d
cat > /etc/systemd/journald.conf.d/size.conf <<'EOF'
[Journal]
SystemMaxUse=200M
EOF
systemctl restart systemd-journald

echo "==> Firewall: SSH and HTTP only"
ufw default deny incoming
ufw default allow outgoing
ufw allow 22/tcp
ufw allow 80/tcp
ufw --force enable

echo "==> Runner user and app directory"
id -u "$RUNNER_USER" >/dev/null 2>&1 || useradd -m -s /bin/bash "$RUNNER_USER"
usermod -aG docker "$RUNNER_USER"
install -d -o "$RUNNER_USER" -g "$RUNNER_USER" "$APP_DIR" "$RUNNER_DIR"

echo "==> systemd unit: start the compose stack on boot"
# The deploy job puts compose.yaml, nginx/, postgres/ and .env (with the image SHAs) in $APP_DIR.
cat > /etc/systemd/system/rfam.service <<EOF
[Unit]
Description=RF Alert Manager (docker compose)
Requires=docker.service
After=docker.service network-online.target
Wants=network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=$APP_DIR
ExecCondition=/usr/bin/test -f $APP_DIR/compose.yaml
ExecStart=/usr/bin/docker compose -f compose.yaml --env-file .env up -d --no-build
ExecStop=/usr/bin/docker compose -f compose.yaml --env-file .env down
TimeoutStartSec=300

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable rfam.service

echo "==> GitHub Actions self-hosted runner"
if [ -f "$RUNNER_DIR/.runner" ]; then
  echo "Runner already registered, skipping."
elif [ -z "${GH_REPO_URL:-}" ] || [ -z "${GH_RUNNER_TOKEN:-}" ]; then
  echo "GH_REPO_URL / GH_RUNNER_TOKEN not set: skipping runner registration."
  echo "Set them on the host and run: vagrant provision"
else
  version=$(curl -fsSL https://api.github.com/repos/actions/runner/releases/latest | jq -r .tag_name | sed 's/^v//')
  curl -fsSL -o /tmp/runner.tar.gz \
    "https://github.com/actions/runner/releases/download/v${version}/actions-runner-linux-x64-${version}.tar.gz"
  tar xzf /tmp/runner.tar.gz -C "$RUNNER_DIR"
  rm -f /tmp/runner.tar.gz
  chown -R "$RUNNER_USER:$RUNNER_USER" "$RUNNER_DIR"
  "$RUNNER_DIR/bin/installdependencies.sh"
  sudo -u "$RUNNER_USER" bash -c "cd '$RUNNER_DIR' && ./config.sh --unattended --replace \
    --url '$GH_REPO_URL' --token '$GH_RUNNER_TOKEN' --name rfam-vm --labels rfam-vm"
  # Installs the runner as a systemd service, so it starts when the VM boots.
  (cd "$RUNNER_DIR" && ./svc.sh install "$RUNNER_USER" && ./svc.sh start)
fi

echo "==> Done"
docker --version
docker compose version
ufw status