# syntax=docker/dockerfile:1

# ---- deps -------------------------------------------------------------
FROM node:22-alpine AS deps
WORKDIR /app

ENV COREPACK_ENABLE_DOWNLOAD_PROMPT=0
RUN corepack enable

COPY package.json pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile

# ---- build ------------------------------------------------------------
FROM node:22-alpine AS build
WORKDIR /app

ENV COREPACK_ENABLE_DOWNLOAD_PROMPT=0
RUN corepack enable

COPY --from=deps /app/node_modules ./node_modules
COPY . .

# Produces .next/standalone — a self-contained server that doesn't need
# node_modules at runtime. Off by default because its tracing step needs
# symlink permissions that plain Windows accounts lack.
ENV NEXT_OUTPUT_STANDALONE=true
ENV NEXT_TELEMETRY_DISABLED=1
RUN pnpm run build

# ---- storage drivers --------------------------------------------------
# The storage backends' drivers aren't dependencies of this package — you
# install only the one you use. They're also loaded through an indirect
# dynamic import, so Next's build tracing never sees them and won't copy
# them into the standalone output. Installing them separately here and
# merging them into the runtime's node_modules is what makes them
# resolvable.
#
# Add to this list to enable another backend: mongodb, memcached,
# rethinkdbdash, @aws-sdk/client-s3, @google-cloud/datastore.
FROM node:22-alpine AS drivers
WORKDIR /drivers
RUN npm init -y >/dev/null && \
    npm install --omit=dev --no-audit --no-fund \
      redis@^4.7.0 \
      pg@^8.13.1 \
      mysql2@^3.16.0

# ---- runtime ----------------------------------------------------------
FROM node:22-alpine AS runtime
WORKDIR /app

ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1

# Server defaults. Every one of these can be overridden at `docker run` /
# compose time; they use the same names the pre-rewrite entrypoint did.
ENV HOSTNAME=0.0.0.0 \
    PORT=3000 \
    KEY_LENGTH=10 \
    MAX_LENGTH=400000

ENV STORAGE_TYPE=file \
    STORAGE_HOST= \
    STORAGE_PORT= \
    STORAGE_EXPIRE_SECONDS= \
    STORAGE_DB= \
    STORAGE_USERNAME= \
    STORAGE_PASSWORD= \
    STORAGE_FILEPATH=/app/data \
    STORAGE_AWS_BUCKET= \
    STORAGE_AWS_REGION=

ENV KEYGENERATOR_TYPE=phonetic \
    KEYGENERATOR_KEYSPACE=

ENV RATELIMITS_NORMAL_TOTAL_REQUESTS=500 \
    RATELIMITS_NORMAL_EVERY_MILLISECONDS=60000 \
    RATELIMITS_WHITELIST= \
    RATELIMITS_BLACKLIST=

ENV LOGGING_LEVEL=info

ENV DOCUMENTS=about=./about.md

COPY --from=build --chown=node:node /app/.next/standalone ./
COPY --from=build --chown=node:node /app/.next/static ./.next/static
COPY --from=build --chown=node:node /app/public ./public
COPY --from=build --chown=node:node /app/config.json /app/about.md ./
COPY --from=build --chown=node:node /app/scripts ./scripts
COPY --from=drivers --chown=node:node /drivers/node_modules ./node_modules

# The file store writes here; mount a volume over it to persist documents.
RUN mkdir -p /app/data && chown node:node /app/data

USER node:node

EXPOSE 3000
STOPSIGNAL SIGTERM

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD wget -q --spider http://127.0.0.1:${PORT}/ || exit 1

# server.js is the standalone build's entrypoint. The flag is the same one
# scripts/next.js applies in development — see its comment for why.
CMD ["node", "--no-experimental-webstorage", "server.js"]
