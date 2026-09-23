#!/usr/bin/env bash
# ----------------------------------------------------------------------------
# scripts/health-check.sh
#
# Trilingual Gateway の 3 サービス (analytics-py / processor-go / usermgmt-ts)
# の /health エンドポイントに順にアクセスし、疎通結果を 1 コマンドで表示する。
#
# 使い方:
#   bash scripts/health-check.sh          # 既定ポート (8001/8002/8003) を確認
#   HEALTH_HOST=example.internal \
#     bash scripts/health-check.sh        # ホストを差し替え
#   ANALYTICS_PORT=9001 PROCESSOR_PORT=9002 USERMGMT_PORT=9003 \
#     bash scripts/health-check.sh        # ポートを差し替え
#
# 環境変数 (すべて任意):
#   HEALTH_HOST       接続先ホスト (既定: 127.0.0.1)
#   HEALTH_TIMEOUT    1 リクエストあたりのタイムアウト秒 (既定: 5)
#   ANALYTICS_PORT    analytics-py のポート (既定: 8001)
#   PROCESSOR_PORT    processor-go のポート (既定: 8002)
#   USERMGMT_PORT     usermgmt-ts のポート (既定: 8003)
#   NO_COLOR          非空なら色出力を抑止
#
# 終了コード:
#   0  すべてのサービスの /health が 2xx を返した
#   1  1 つ以上のサービスで疎通失敗 / 非 2xx / タイムアウト
#   2  必須コマンド (curl または wget) がどちらも見つからない
# ----------------------------------------------------------------------------

set -euo pipefail

HEALTH_HOST="${HEALTH_HOST:-127.0.0.1}"
HEALTH_TIMEOUT="${HEALTH_TIMEOUT:-5}"
ANALYTICS_PORT="${ANALYTICS_PORT:-8001}"
PROCESSOR_PORT="${PROCESSOR_PORT:-8002}"
USERMGMT_PORT="${USERMGMT_PORT:-8003}"

# --- 色定義 (端末でない or NO_COLOR が指定されたら無効化) ---------------------
if [[ -t 1 && -z "${NO_COLOR:-}" ]]; then
  C_GREEN=$'\033[32m'
  C_RED=$'\033[31m'
  C_DIM=$'\033[2m'
  C_BOLD=$'\033[1m'
  C_RESET=$'\033[0m'
else
  C_GREEN=""
  C_RED=""
  C_DIM=""
  C_BOLD=""
  C_RESET=""
fi

usage() {
  sed -n '2,32p' "$0" | sed 's/^# \{0,1\}//'
}

case "${1:-}" in
  -h|--help)
    usage
    exit 0
    ;;
esac

# --- HTTP クライアント選択 ---------------------------------------------------
# curl があれば curl を優先。無ければ wget にフォールバック
# (compose の healthcheck が wget --spider を使う processor-go / usermgmt-ts
# コンテナに合わせる)。どちらも無ければ終了コード 2 で異常終了する。
HTTP_TOOL=""
if command -v curl >/dev/null 2>&1; then
  HTTP_TOOL="curl"
elif command -v wget >/dev/null 2>&1; then
  HTTP_TOOL="wget"
else
  printf '%sERROR%s: curl / wget のいずれも見つかりません。どちらかをインストールしてください。\n' \
    "$C_RED" "$C_RESET" >&2
  exit 2
fi

# fetch_status URL
# 標準出力に HTTP ステータスコード相当を返す (200 / 000 のいずれか)。
# curl は %{http_code}、wget はプロセス終了コードで疎通有無を判定する。
fetch_status() {
  local url="$1"
  if [[ "$HTTP_TOOL" == "curl" ]]; then
    curl \
      --silent \
      --show-error \
      --max-time "$HEALTH_TIMEOUT" \
      --output /dev/null \
      --write-out '%{http_code}' \
      "$url" 2>/dev/null || printf '000'
  else
    if wget \
      --quiet \
      --tries=1 \
      --timeout="$HEALTH_TIMEOUT" \
      --spider \
      "$url" >/dev/null 2>&1; then
      printf '200'
    else
      printf '000'
    fi
  fi
}

check_service() {
  local label="$1" port="$2"
  local url="http://${HEALTH_HOST}:${port}/health"
  local status
  status="$(fetch_status "$url")"

  # 2xx を成功扱いにする。curl は非到達時に 000、非 2xx はそのままコードを返す。
  if [[ "$status" =~ ^2[0-9][0-9]$ ]]; then
    printf '  %s%-16s%s  %sOK%s   %s(%s %s)%s\n' \
      "$C_BOLD" "$label" "$C_RESET" \
      "$C_GREEN" "$C_RESET" \
      "$C_DIM" "$status" "$url" "$C_RESET"
    return 0
  else
    printf '  %s%-16s%s  %sFAIL%s %s(%s %s)%s\n' \
      "$C_BOLD" "$label" "$C_RESET" \
      "$C_RED" "$C_RESET" \
      "$C_DIM" "$status" "$url" "$C_RESET"
    return 1
  fi
}

printf '%sTrilingual Gateway health check%s (host=%s tool=%s timeout=%ss)\n' \
  "$C_BOLD" "$C_RESET" "$HEALTH_HOST" "$HTTP_TOOL" "$HEALTH_TIMEOUT"

failures=0
check_service "analytics-py"  "$ANALYTICS_PORT" || failures=$((failures + 1))
check_service "processor-go"  "$PROCESSOR_PORT" || failures=$((failures + 1))
check_service "usermgmt-ts"   "$USERMGMT_PORT"  || failures=$((failures + 1))

echo
if (( failures == 0 )); then
  printf '%sAll 3 services are healthy.%s\n' "$C_GREEN" "$C_RESET"
  exit 0
else
  printf '%s%d service(s) failed the health check.%s\n' \
    "$C_RED" "$failures" "$C_RESET" >&2
  exit 1
fi
