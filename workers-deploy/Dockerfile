FROM node:20-alpine

# Install ffmpeg, yt-dlp, and fonts with fontconfig for reliable text rendering
RUN apk add --no-cache \
    ffmpeg \
    python3 \
    py3-pip \
    fontconfig \
    ttf-dejavu \
    && pip3 install --break-system-packages yt-dlp \
    && fc-cache -f

WORKDIR /app

COPY package.json ./
RUN npm install --production

COPY src ./src

# Copy bundled fonts as backup (OS fonts preferred via fontconfig)
COPY fonts ./fonts

CMD ["npm", "run", "start"]
