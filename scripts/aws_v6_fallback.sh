#!/bin/bash
# aws_v6_fallback.sh — AWS 被墙 v6 兜底链自动化 (2026-11-19)
#
# 场景: EasyTier mesh 整体故障时, AWS 节点的降级路径 = 动态 v6 /128 直连。
# v6 地址 ~390s 租期会漂移, 手工取地址不可靠 —— 本脚本一键:
#   1. 经 mesh SSH 取两台 AWS 的当前全局 v6
#   2. Mac 视角逐项验证: v6 TCP:443 连通 + TLS 握手 (REALITY 端口活性)
#   3. 产出可直接粘贴的 Clash 代理节点片段 (logs/aws-v6-fallback-<ts>.txt)
# 注意: Mac 侧 IPv6 出口 = en0 全局 v6 (2409:8a1e::/64 移动, 默认路由在位)。
#       mesh 故障时 SSH 取地址也会失败 —— 此时人工从云控制台查询实例 v6。
set -u
cd "$(dirname "$0")/.." || exit 1
OUT="logs/aws-v6-fallback-$(date +%Y%m%d-%H%M%S).txt"
mkdir -p logs

fetch_v6() {  # $1 = ssh 别名 → 该机当前全局 v6 地址
    ssh -o ConnectTimeout=8 "$1" \
        "ip -6 addr show scope global 2>/dev/null | grep -oE 'inet6 [0-9a-f:]+/[0-9]+' | head -2" \
        2>/dev/null | awk '{print $2}' | cut -d/ -f1 | head -1
}

test_v6() {  # $1 = v6 地址 → TCP:443 + TLS 握手
    nc -z -6 -G 8 "$1" 443 >/dev/null 2>&1 || return 1
    # macOS 无 GNU timeout —— openssl 以 </dev/null 自终止
    echo | openssl s_client -connect "[$1]:443" -servername www.apple.com </dev/null 2>/dev/null \
        | grep -q "Cipher is" && return 0
    return 1
}

emit_node() {  # $1=label $2=v6 $3=uuid $4=pubkey $5=sid
    cat <<NODE
- name: '[自建] VLESS-AWS-$1-v6-fallback'
  type: vless
  server: $2
  port: 443
  uuid: $3
  network: tcp
  flow: xtls-rprx-vision
  tls: true
  servername: www.apple.com
  client-fingerprint: chrome
  reality-opts:
    public-key: $4
    short-id: $5
  udp: true
NODE
}

# pair 用 '|' 分隔 —— IPv6 地址含冒号, 不能用 ':' (首版实测踩坑)
PAIRS=(
  "Tokyo|mesh-aws|64d00fb5-4e94-4bd3-bf19-b03ec2dfe63e|aI98mXPCeEvsG3G-S1fks_XKLBLu589TCg2GFFIAGhk|7f10d18d86b1a80a"
  "Oregon|mesh-oregon|2237658b-63c3-42d3-98f4-24cffe723cdf|wGDeu9By8n-XPFp_o8k-73fBukvC4tX4ZQ0TuLDzkXI|d5d8a2a36f37541b"
)

{
    echo "# AWS v6 兜底片段 (生成于 $(date '+%F %T'))"
    echo "# ⚠️ v6 /128 为动态租期 (~390s 漂移), 使用前必须重跑本脚本刷新"
    echo "# mesh 故障时 SSH 取地址也会失败 —— 请改从云控制台查询实例 v6"
} >> "$OUT"

ok_total=0
for pair in "${PAIRS[@]}"; do
    IFS='|' read -r label alias uuid pk sid <<< "$pair"
    v6=$(fetch_v6 "$alias")
    if [ -z "$v6" ]; then
        echo "[$label] v6 获取失败 (SSH 不可达?)" | tee -a "$OUT"
        continue
    fi
    if test_v6 "$v6"; then
        verdict="TCP+TLS ✓ (可作兜底)"
        ok_total=$((ok_total+1))
    else
        verdict="不可达 ✗ (Mac 无 v6 出口或地址已漂移)"
    fi
    echo "[$label] v6=$v6 → $verdict" | tee -a "$OUT"
    emit_node "$label" "$v6" "$uuid" "$pk" "$sid" >> "$OUT"
done

ok_total_final=${ok_total}
echo "--- 片段已写入 $OUT (可用 ${ok_total_final}/2) ---" | tee -a "$OUT"
if [ "$ok_total_final" -gt 0 ]; then exit 0; else exit 1; fi
