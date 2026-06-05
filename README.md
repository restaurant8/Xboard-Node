# xboard-node

Node backend for [restaurant8/Xboard](https://github.com/restaurant8/Xboard). Supports `sing-box` / `xray-core` dual kernels.

> **Disclaimer**: This project is for educational and learning purposes only.

## Features

- Protocols: V2Ray family, Trojan, Shadowsocks, Hysteria2, TUIC, AnyTLS
- Sync: WebSocket push + REST polling dual channel
- User controls: speed limit, device limit, alive-IP tracking, hot update
- Deploy modes: node mode, machine mode, standalone mode
- Multi-instance: single process binding multiple panels / nodes

## Install / Upgrade

### Docker

```bash
docker run -d --restart=always --network=host \
  -e apiHost=https://panel.com -e apiKey=TOKEN -e nodeID=1 \
  ghcr.io/restaurant8/xboard-node:latest
```

### Docker Compose

```bash
git clone -b compose --depth 1 https://github.com/restaurant8/Xboard-Node.git
cd xboard-node
vim config/config.yml   # set panel.url / token / node_id
docker compose up -d
```

### Installer (Linux systemd)

New node:

```bash
# Node mode
curl -fsSL https://raw.githubusercontent.com/restaurant8/Xboard-Node/main/install.sh | \
  sudo bash -s -- --mode node --panel https://panel.example.com --token TOKEN --node-id 1

# Machine mode
curl -fsSL https://raw.githubusercontent.com/restaurant8/Xboard-Node/main/install.sh | \
  sudo bash -s -- --mode machine --panel https://panel.example.com --token TOKEN --machine-id 1
```

Pin a specific release:

```bash
curl -fsSL https://raw.githubusercontent.com/restaurant8/Xboard-Node/main/install.sh | \
  sudo bash -s -- --mode node --panel https://panel.example.com --token TOKEN --node-id 1 --version v1.0.3
```

Existing installation:

```bash
# Recommended: keep config and only replace binaries/service file
sudo xbctl upgrade --version v1.0.3

# Or use the installer upgrade action
curl -fsSL https://raw.githubusercontent.com/restaurant8/Xboard-Node/main/install.sh | \
  sudo bash -s -- upgrade --version v1.0.3
```

The installer backs up the current binary/config/service before upgrading. Do not uninstall or purge an existing node unless you intentionally want to remove its local configuration.

### Verify Installed Version

```bash
xboard-node -v
xbctl version
systemctl status xboard-node
journalctl -u xboard-node -n 80 --no-pager
journalctl -u xboard-node -n 80 --no-pager | grep "report pushed"
```

## xbctl

Run `xbctl` after installation for help. Common commands:

```bash
xbctl list                          # list all instances
xbctl status                        # running status
xbctl upgrade                        # upgrade to latest GitHub Release
xbctl upgrade --version v1.0.3       # upgrade installed node to a specific release
xbctl bind add-node --panel URL --token TOKEN --node-id 1
xbctl bind add-machine --panel URL --token TOKEN --machine-id 1
xbctl bind remove-node --panel URL --node-id 1
xbctl service restart
```

## Build and Release

### Local Build

Use Docker if the local machine does not have the required Go toolchain.

```powershell
cd D:\Xboard-Node
docker run --rm -v D:\Xboard-Node:/work -w /work golang:1.26 make build-all VERSION=v1.0.3
```

Expected output files:

```text
D:\Xboard-Node\xboard-node-linux-amd64
D:\Xboard-Node\xboard-node-linux-arm64
D:\Xboard-Node\xbctl-linux-amd64
D:\Xboard-Node\xbctl-linux-arm64
```

Verify the Linux amd64 binaries:

```powershell
docker run --rm -v D:\Xboard-Node:/work -w /work debian:stable-slim ./xboard-node-linux-amd64 -v
docker run --rm -v D:\Xboard-Node:/work -w /work debian:stable-slim ./xbctl-linux-amd64 version
```

Generate SHA256 checksums:

```powershell
cd D:\Xboard-Node
$files = @('xboard-node-linux-amd64','xboard-node-linux-arm64','xbctl-linux-amd64','xbctl-linux-arm64')
$lines = foreach ($f in $files) { $h = (Get-FileHash -Algorithm SHA256 $f).Hash.ToLower(); "$h  $f" }
Set-Content -Path SHA256SUMS -Value $lines -Encoding ascii
Get-Content SHA256SUMS
```

### Manual GitHub Release Upload

Create or open the target release, for example `v1.0.3`, and upload these
files as release assets. Do not rename them and do not compress them:

```text
xboard-node-linux-amd64
xboard-node-linux-arm64
xbctl-linux-amd64
xbctl-linux-arm64
SHA256SUMS
```

The installer and `xbctl upgrade` download from URLs like:

```text
https://github.com/restaurant8/Xboard-Node/releases/latest/download/xboard-node-linux-amd64
```

### Automatic GitHub Actions Release

The workflow publishes a release when a version tag is pushed:

```powershell
cd D:\Xboard-Node
git tag -a v1.0.4 -m "v1.0.4"
git push origin v1.0.4
```

Push to `main` runs checks/builds, but release publishing should be driven by
version tags.

## Traffic Diagnostics

The node reads these settings from the panel handshake:

- `traffic_stats_mode`: `off`, `privacy`, or `diagnostic`.
- `traffic_stats_interval`: aggregation interval in minutes.

When diagnostic mode is enabled, the sing-box kernel records per-user
destination details and sends them in `traffic_stats` with the normal traffic
report. Legacy nodes that do not send `traffic_stats` remain compatible.

## Configuration

Legacy single-panel config is fully compatible. Appending bindings auto-migrates to `instances` format. See `config.yml.example`.

## Extensions

- Custom routes: [docs-custom-routes.md](docs-custom-routes.md)
- Custom outbounds: [docs-custom-outbounds.md](docs-custom-outbounds.md)
- DNS providers (ACME DNS-01): [docs-dns-providers.md](docs-dns-providers.md)

## License

MPL-2.0.
