# syntax=docker/dockerfile:1
FROM node:24-alpine AS build

WORKDIR /app

ARG NPM_CONFIG_REGISTRY=https://registry.npmjs.org/

COPY web/package.json web/package-lock.json ./
RUN --mount=type=cache,target=/root/.npm \
    npm ci --no-audit --no-fund

COPY web/ ./
RUN npm run build

FROM nginx:1.28-alpine

COPY deploy/docker/web.nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist/ /usr/share/nginx/html/

EXPOSE 80
