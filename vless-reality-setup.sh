#!/usr/bin/env bash
#
# VLESS + REALITY (Xray-core) 一键搭建脚本  —  Debian / Ubuntu
# 用法:
#   chmod +x vless-reality-setup.sh
#   sudo ./vless-reality-setup.sh
#
# 可选环境变量(不设就用默认):
#   PORT=443                监听端口
#   DEST=www.apple.com      借用的 SNI 站点(必须支持 TLS1.3 + H2)
#   UUID=...                指定 UUID(不设则随机生成)
#
set -euo pipefail

# ---------- 0. 基本检查 ----------
if [[ $EUID -ne 0 ]]; then
  echo "请用 root 运行:sudo $0" >&2
  exit 1
fi

PORT="${PORT:-443}"
DEST="${DEST:-www.apple.com}"
SNI="${DEST%%:*}"          # 从 DEST 中取出域名部分
[[ "$DEST" == *:* ]] || DEST="${DEST}:443"

echo "==> 安装依赖..."
export DEBIAN_FRONTEND=noninteractive
apt-get update -y >/dev/null
apt-get install -y curl openssl qrencode jq >/dev/null

# ---------- 1. 安装 Xray-core ----------
echo "==> 安装 Xray-core..."
bash -c "$(curl -L https://github.com/XTLS/Xray-install/raw/main/install-release.sh)" @ install >/dev/null

XRAY_BIN="$(command -v xray || echo /usr/local/bin/xray)"

# ---------- 2. 生成密钥 / UUID / shortId ----------
echo "==> 生成密钥..."
KEYS="$("$XRAY_BIN" x25519)"
PRIVATE_KEY="$(echo "$KEYS" | awk -F': ' '/[Pp]rivate/{print $2}')"
PUBLIC_KEY="$(echo "$KEYS"  | awk -F': ' '/[Pp]ublic/{print $2}')"
UUID="${UUID:-$("$XRAY_BIN" uuid)}"
SHORT_ID="$(openssl rand -hex 8)"

# ---------- 3. 写配置 ----------
echo "==> 写入配置 /usr/local/etc/xray/config.json ..."
cat > /usr/local/etc/xray/config.json <<EOF
{
  "log": { "loglevel": "warning" },
  "inbounds": [
    {
      "listen": "0.0.0.0",
      "port": ${PORT},
      "protocol": "vless",
      "settings": {
        "clients": [
          { "id": "${UUID}", "flow": "xtls-rprx-vision" }
        ],
        "decryption": "none"
      },
      "streamSettings": {
        "network": "tcp",
        "security": "reality",
        "realitySettings": {
          "show": false,
          "dest": "${DEST}",
          "xver": 0,
          "serverNames": ["${SNI}"],
          "privateKey": "${PRIVATE_KEY}",
          "shortIds": ["${SHORT_ID}"]
        }
      },
      "sniffing": { "enabled": true, "destOverride": ["http", "tls", "quic"] }
    }
  ],
  "outbounds": [
    { "protocol": "freedom", "tag": "direct" },
    { "protocol": "blackhole", "tag": "block" }
  ]
}
EOF

# 校验配置
"$XRAY_BIN" run -test -config /usr/local/etc/xray/config.json >/dev/null

# ---------- 4. 启动 ----------
echo "==> 启动 Xray..."
systemctl enable xray >/dev/null 2>&1 || true
systemctl restart xray
sleep 1
systemctl is-active --quiet xray && echo "Xray 运行中 ✓" || { echo "Xray 启动失败,看 journalctl -u xray"; exit 1; }

# 放行端口(若装了 ufw)
if command -v ufw >/dev/null 2>&1 && ufw status | grep -q "Status: active"; then
  ufw allow "${PORT}"/tcp >/dev/null 2>&1 || true
fi

# ---------- 5. 输出结果 ----------
IP="$(curl -s4 https://api.ipify.org || curl -s4 ifconfig.me || echo 'YOUR_SERVER_IP')"
LINK="vless://${UUID}@${IP}:${PORT}?encryption=none&flow=xtls-rprx-vision&security=reality&sni=${SNI}&fp=chrome&pbk=${PUBLIC_KEY}&sid=${SHORT_ID}&type=tcp#JP-Reality"

cat <<EOF

============================================================
  搭建完成 🎉   以下信息请保存
============================================================
  地址 (IP)     : ${IP}
  端口          : ${PORT}
  UUID          : ${UUID}
  Flow          : xtls-rprx-vision
  SNI / dest    : ${SNI}
  Public key    : ${PUBLIC_KEY}
  shortId       : ${SHORT_ID}
  指纹 fp       : chrome
------------------------------------------------------------
  分享链接(导入客户端):

${LINK}

------------------------------------------------------------
  扫码导入(终端二维码):
EOF

echo "$LINK" | qrencode -t ANSIUTF8 || true

cat <<EOF
============================================================
  常用命令:
    systemctl restart xray     # 重启
    journalctl -u xray -f      # 看日志
    nano /usr/local/etc/xray/config.json   # 改配置后记得 restart
============================================================
EOF
