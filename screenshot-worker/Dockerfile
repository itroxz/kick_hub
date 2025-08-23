FROM node:20-bullseye

# Install dependencies required by Chromium
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    fonts-liberation \
    libnss3 \
    libxss1 \
    libasound2 \
    libatk-bridge2.0-0 \
    libgtk-3-0 \
    libx11-6 \
    libx11-xcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libgbm-dev \
    libpango-1.0-0 \
    wget \
    xdg-utils \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install app
COPY package.json ./
# Use npm install when package-lock.json is not present (safer for CI-less builds)
RUN npm install --production --no-audit --no-fund

COPY capture.js ./
COPY entrypoint.sh ./
RUN chmod +x ./entrypoint.sh

ENV SCREENSHOT_DIR=/data/screenshots
ENV INTERVAL_SECONDS=300

VOLUME ["/data"]

CMD ["/app/entrypoint.sh"]
