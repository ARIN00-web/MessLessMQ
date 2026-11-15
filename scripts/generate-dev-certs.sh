#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$ROOT/deploy/secrets"
command -v openssl >/dev/null 2>&1 || { echo "openssl is required" >&2; exit 1; }
mkdir -p "$OUT"
chmod 700 "$OUT"
if [[ -e "$OUT/ca.key" || -e "$OUT/ca.crt" ]]; then
  echo "Refusing to overwrite existing CA material in $OUT" >&2
  exit 1
fi
openssl genrsa -out "$OUT/ca.key" 3072 >/dev/null 2>&1
openssl req -x509 -new -nodes -key "$OUT/ca.key" -sha256 -days 3650 \
  -subj "/CN=MessLessMQ Development CA" -out "$OUT/ca.crt"

issue_cert() {
  local name="$1" san="$2"
  openssl genrsa -out "$OUT/$name.key" 2048 >/dev/null 2>&1
  openssl req -new -key "$OUT/$name.key" -subj "/CN=$name" -out "$OUT/$name.csr"
  local ext="$OUT/$name.ext"
  cat > "$ext" <<EXT
subjectAltName=$san
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth,clientAuth
basicConstraints=CA:FALSE
EXT
  openssl x509 -req -in "$OUT/$name.csr" -CA "$OUT/ca.crt" -CAkey "$OUT/ca.key" \
    -CAcreateserial -out "$OUT/$name.crt" -days 825 -sha256 -extfile "$ext" >/dev/null 2>&1
  rm -f "$OUT/$name.csr" "$ext"
  chmod 600 "$OUT/$name.key"
}

# Every broker certificate covers its own Compose name and all StatefulSet
# ordinal DNS names. In production, issue individual certificates from your CA.
K8S_SAN="DNS:messlessmq-0.messlessmq-headless.messlessmq.svc.cluster.local,DNS:messlessmq-1.messlessmq-headless.messlessmq.svc.cluster.local,DNS:messlessmq-2.messlessmq-headless.messlessmq.svc.cluster.local,DNS:messlessmq.messlessmq.svc.cluster.local,DNS:messlessmq-headless.messlessmq.svc.cluster.local"
for broker in broker-1 broker-2 broker-3; do
  ordinal="${broker##*-}"
  ordinal=$((ordinal - 1))
  issue_cert "$broker" "DNS:$broker,IP:127.0.0.1,$K8S_SAN"
  cp "$OUT/$broker.crt" "$OUT/messlessmq-$ordinal.crt"
  cp "$OUT/$broker.key" "$OUT/messlessmq-$ordinal.key"
  chmod 600 "$OUT/messlessmq-$ordinal.key"
done
issue_cert client "DNS:messlessmq-client"
chmod 600 "$OUT/ca.key"
echo "Development CA and broker/client certificates created in $OUT"
echo "These are local development credentials, not a production PKI. Keep the directory private."
