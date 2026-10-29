#!/bin/sh
# Runs at container start, like 25-realip.sh. Chooses whether NGINX writes its
# access log, by overwriting the include file access_log.conf:
#   GATEWAY_ACCESS_LOG unset or 1 (default): one buffered line per request to stdout
#   GATEWAY_ACCESS_LOG=0: no access log. Load tests only: one line per request
#     through Docker's log collection is work outside what the test measures
set -e
conf=/etc/nginx/gateway/access_log.conf
if [ "${GATEWAY_ACCESS_LOG:-1}" = "0" ]; then
    echo "access_log off;" > "$conf"
    echo "26-access-log: GATEWAY_ACCESS_LOG=0, access log off (load tests only)"
else
    echo "access_log /dev/stdout gateway buffer=64k flush=1s;" > "$conf"
fi
