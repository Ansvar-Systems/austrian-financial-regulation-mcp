# MCP Server — Hetzner / Kubernetes
# Image contract: docs/superpowers/specs/2026-04-25-mcp-infrastructure-standard-design.md §3
# Profile: node-native (better-sqlite3 — native modules built in builder, pruned, copied)
# DB pattern: none

FROM node:20-alpine AS builder

RUN apk add --no-cache python3 make g++

WORKDIR /app

COPY package*.json ./
RUN npm ci --ignore-scripts && npm cache clean --force
# Native module rebuild — better-sqlite3 needs its .node binding for build:db
# to open the DB. --ignore-scripts above skipped the prebuild-fetch.
RUN npm rebuild better-sqlite3

COPY tsconfig.json ./
COPY src/ ./src/
COPY scripts/ ./scripts/
RUN npm run build
RUN npm prune --omit=dev

FROM node:20-alpine AS runtime

WORKDIR /app

RUN addgroup -g 1001 -S nodejs \
 && adduser -u 1001 -S nodejs -G nodejs

COPY --from=builder --chown=nodejs:nodejs /app/dist ./dist
COPY --from=builder --chown=nodejs:nodejs /app/node_modules ./node_modules
COPY --chown=nodejs:nodejs package.json ./

# Bake the pre-built database into the image so /app/data/fma.db resolves
# at runtime without a bind mount. The explicit `data/<name>.db` reference
# is required — `.github/workflows/publish-ghcr.yml` greps the Dockerfile
# with `COPY\s+\K(data/\S+\.db)` to decide whether to download the
# gitignored DB from a GitHub Release. `data/database.db` is provisioned
# by the workflow's "Provision database" step — it `gh release download`s
# `database.db.gz` and gunzips to that path. We then COPY it into the
# image at /app/data/fma.db (FMA_DB_PATH default).
COPY --chown=nodejs:nodejs data/database.db data/fma.db

USER nodejs

ENV NODE_ENV=production \
    PORT=3000 \
    FMA_DB_PATH=/app/data/fma.db

EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD node -e "fetch('http://localhost:3000/health').then(r=>r.ok?process.exit(0):process.exit(1)).catch(()=>process.exit(1))"

CMD ["node", "dist/src/http-server.js"]
