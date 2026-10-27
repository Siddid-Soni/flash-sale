#!/bin/sh
# Runs at container start (the official image runs every executable
# /docker-entrypoint.d/*.sh before NGINX). Chooses where NGINX takes the
# client address from, by overwriting the include file realip.conf:
#   GATEWAY_TRUST_XFF unset or 0 (default): empty file, the address is the TCP peer
#   GATEWAY_TRUST_XFF=1: realip-trusted.conf, X-Forwarded-For is believed when
#     it arrives from the bridge address (load tests only; see gateway/README.md)
set -e
dir=/etc/nginx/gateway
if [ "${GATEWAY_TRUST_XFF:-0}" = "1" ]; then
    cp "$dir/realip-trusted.conf" "$dir/realip.conf"
    echo "25-realip: GATEWAY_TRUST_XFF=1, trusting X-Forwarded-For from the bridge address (load tests only)"
else
    : > "$dir/realip.conf"
fi
