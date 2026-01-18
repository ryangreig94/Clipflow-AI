FROM node:20-alpine

# Install ffmpeg + python/pip + fonts + ca certs
RUN apk add --no-cache \
    ffmpeg \
    python3 \
    py3-pip \
    ca-certificates \
    fontconfig \
    ttf-dejavu \
  && pip3 install --no-cache-dir -U yt-dlp \
  && fc-cache -f \
  && yt-dlp --version \
  && ffmpeg -version

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm ci --omit=dev || npm install --production

COPY src ./src
COPY fonts ./fonts

CMD ["npm", "run", "start"]
