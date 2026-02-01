###################
# BUILD STAGE
###################
FROM node:18.13.0-bullseye As build

# Install build tools for native SQLite compilation
RUN apt-get update && apt-get install -y openssl python3 build-essential
RUN npm install -g pnpm

WORKDIR /app

# Copy root manifests and configuration
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml .npmrc ./

# Copy the app-specific manifest
COPY apps/remote-storage-server/package.json ./apps/remote-storage-server/

# Install ALL dependencies (including dev)
# This uses the lockfile from the root and the hoisted linker from .npmrc
RUN pnpm install

# Copy the rest of the source code
COPY . .

# Build the specific app
RUN pnpm --filter remote-storage-server run build

# Re-install only production deps to keep image small
RUN rm -rf node_modules && \
    pnpm install --prod --filter remote-storage-server

###################
# PRODUCTION RUN
###################
FROM node:18.13.0-bullseye As production

RUN apt-get update && apt-get install -y openssl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy production node_modules and built dist
COPY --chown=node:node --from=build /app/node_modules /app/node_modules
COPY --chown=node:node --from=build /app/apps/remote-storage-server/dist /app/dist
COPY --chown=node:node --from=build /app/apps/remote-storage-server/package.json /app/package.json

USER node

# Ensure port 4000 is exposed
EXPOSE 4000

CMD [ "node", "dist/main.js" ]
