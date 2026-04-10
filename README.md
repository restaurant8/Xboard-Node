# xboard-node

Dedicated node backend for [Xboard](https://github.com/cedar2025/Xboard). Fully compatible with Xboard API.

> **Disclaimer**: This project is for educational and learning purposes only.

## Overview

| Item | Description |
| --- | --- |
| Role | Xboard-compatible node backend |
| Kernels | `sing-box` (default), `xray-core` |
| Protocols | V2Ray family, Trojan, Shadowsocks, Hysteria2, TUIC, Naive |
| Modes | Panel-managed mode, `standalone` mode |
| Sync | WebSocket push, REST polling/report fallback |
| User controls | Per-user speed limit, device limit, alive IP tracking |
| Runtime ops | Hot user add/remove/update |
| Reporting | Traffic, online/alive-IP state, CPU, memory, swap, disk, connection count |
| Deployment | Single Go service, Docker, Docker Compose |

## Install

### Docker
```bash
docker run -d --restart=always --network=host \
  -e apiHost=https://panel.com \
  -e apiKey=TOKEN \
  -e nodeID=1 \
  ghcr.io/cedar2025/xboard-node:latest
```

### Docker Compose

**1. Get the `compose/` directory**

```bash
git clone -b compose --depth 1 https://github.com/cedar2025/xboard-node.git
cd xboard-node
```

**2. Edit local config**

```bash
vim config/config.yml
# edit config/config.yml — set panel.url, panel.token, panel.node_id
```

**3. Start**

```bash
docker compose up -d
```

## Configuration

`config.yml`:
```yaml
panel:
  url: "https://panel.com"
  token: "token"
  node_id: 1
```

## License

MPL-2.0.
