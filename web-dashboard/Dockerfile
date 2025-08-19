FROM node:18-alpine
WORKDIR /app

# install deps
COPY web-dashboard/package.json web-dashboard/package-lock.json* ./
RUN npm ci --silent || npm install --silent

# copy app
COPY web-dashboard/ /app/

EXPOSE 3034

ENV PORT=3034
CMD ["npm", "start"]
