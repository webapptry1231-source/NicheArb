FROM node:18-alpine

WORKDIR /app

# Copy package files
COPY package*.json ./

# Install ALL dependencies (including devDependencies) so TypeScript can build
RUN npm install

# Copy source code and build
COPY . .
RUN npm run build

# Now remove devDependencies to keep the final image small
RUN npm prune --production

# Start the bot
CMD ["node", "dist/index.js"]
