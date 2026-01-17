FROM node:20-alpine

# Install ffmpeg and yt-dlp for video processing
RUN apk add --no-cache ffmpeg python3 py3-pip && \
    pip3 install --break-system-packages yt-dlp

WORKDIR /app

COPY package.json ./
RUN npm install --production

COPY src ./src

# Copy bundled fonts for FFmpeg drawtext (more reliable than OS fonts)
COPY fonts ./fonts

CMD ["npm", "run", "start"]
