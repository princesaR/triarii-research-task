# RF Alert Manager VM: Ubuntu 24.04 with Docker, ufw and a GitHub Actions self-hosted runner.
#
#   set GH_REPO_URL=https://github.com/<user>/rf-alert-manager
#   set GH_RUNNER_TOKEN=<token from repo Settings > Actions > Runners > New self-hosted runner>
#   vagrant up
#
# Re-run provisioning (idempotent): vagrant provision

Vagrant.configure("2") do |config|
  config.vm.box = "bento/ubuntu-24.04"
  config.vm.hostname = "rfam"

  # Only Nginx is reachable from the host, and only from this machine: http://localhost:8080
  # (VirtualBox only; Hyper-V ignores forwarded ports, use the VM's IP instead.)
  config.vm.network "forwarded_port", guest: 80, host: 8080, host_ip: "127.0.0.1"

  # The VM runs images pulled from GHCR, not code from the host, like a real server.
  config.vm.synced_folder ".", "/vagrant", disabled: true

  config.vm.provider "virtualbox" do |vb|
    vb.name = "rfam"
    vb.memory = 4096
    vb.cpus = 2
  end

  config.vm.provider "hyperv" do |h|
    h.vmname = "rfam"
    h.memory = 4096
    h.cpus = 2
  end

  # Secrets come from the host environment at provision time; nothing is stored in the repo.
  config.vm.provision "shell", path: "infra/provision.sh", env: {
    "GH_REPO_URL" => ENV["GH_REPO_URL"].to_s,
    "GH_RUNNER_TOKEN" => ENV["GH_RUNNER_TOKEN"].to_s,
  }
end