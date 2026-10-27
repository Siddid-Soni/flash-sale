# The gateway: NGINX with the built frontend. Build context is the repository root:
#   docker build -f gateway/Dockerfile .      (docker compose does this)

FROM node:22-alpine AS web
WORKDIR /web
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
RUN npm run build
# Pre-compressed copies for `gzip_static on`: NGINX serves them as they are,
# so the biggest files cost no CPU per request. The originals stay for
# clients that do not accept gzip.
RUN find dist -type f \( -name '*.js' -o -name '*.css' -o -name '*.html' -o -name '*.svg' -o -name '*.json' \) \
    -exec gzip -9 -k -f {} +

# The official image ships the njs module (ngx_http_js_module.so), used to read
# the user id out of the bearer token for per-user rate limits
FROM nginx:1.28-alpine
COPY gateway/nginx.conf /etc/nginx/nginx.conf
COPY gateway/jwt.js gateway/bots.conf gateway/realip.conf gateway/realip-trusted.conf /etc/nginx/gateway/
COPY gateway/docker-entrypoint.d/ /docker-entrypoint.d/
RUN chmod +x /docker-entrypoint.d/25-realip.sh && rm -f /etc/nginx/conf.d/default.conf
COPY --from=web /web/dist /usr/share/nginx/html
EXPOSE 80
