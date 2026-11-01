#!/bin/sh
# Ephemeral private CA and localhost server certificates for loopback tests.
set -eu
certs=${1:?usage: generate-tls-fixture.sh OUTPUT_DIRECTORY}
umask 077
mkdir -p "$certs" "$certs/empty-trust"
run() {
    if "$@" > "$certs/openssl.log" 2>&1; then
        :
    else
        cat "$certs/openssl.log" >&2
        return 1
    fi
}
run openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 1 \
    -subj '/CN=Zap Redis test CA' -keyout "$certs/ca.key" -out "$certs/ca.pem" \
    -addext 'basicConstraints=critical,CA:TRUE' -addext 'keyUsage=critical,keyCertSign,cRLSign'
run openssl req -new -newkey rsa:2048 -nodes -sha256 -subj '/CN=localhost' \
    -keyout "$certs/server.key" -out "$certs/server.csr"
cat > "$certs/server.ext" <<'EXT'
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:localhost
EXT
run openssl x509 -req -in "$certs/server.csr" -CA "$certs/ca.pem" \
    -CAkey "$certs/ca.key" -CAcreateserial -days 1 -sha256 \
    -extfile "$certs/server.ext" -out "$certs/server.pem"
run openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 1 \
    -subj '/CN=localhost' -keyout "$certs/untrusted.key" -out "$certs/untrusted.pem" \
    -addext 'subjectAltName=DNS:localhost'
