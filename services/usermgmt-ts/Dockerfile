# syntax=docker/dockerfile:1

FROM node:20-alpine AS builder
WORKDIR /app
COPY --chown=node:node package*.json ./
RUN npm ci
COPY --chown=node:node tsconfig.json .
COPY --chown=node:node src/ src/
RUN npm run build

FROM node:20-alpine
WORKDIR /app
COPY --chown=node:node package*.json ./
RUN npm ci --omit=dev
COPY --from=builder --chown=node:node /app/dist ./dist

# CIS Docker Benchmark 4.1 準拠: プリセットの非 root ユーザで実行
USER node

EXPOSE 8003
CMD ["node", "dist/app.js"]
