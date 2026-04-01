FROM node:18-alpine

WORKDIR /app

# Copy package files
COPY package*.json ./

# Install ONLY production dependencies (modern syntax, no warnings)
RUN npm install --omit=dev

# Copy source code and build TypeScript
COPY . .
RUN npm run build

# Start the bot
CMD ["node", "dist/index.js"]
