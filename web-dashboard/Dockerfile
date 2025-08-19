FROM node:18-alpine
WORKDIR /app

# install deps
COPY package.json package-lock.json* ./
RUN npm ci --silent || npm install --silent

# copy app (build context is ./web-dashboard)
COPY . /app/

EXPOSE 3034

ENV PORT=3034
CMD ["npm", "start"]
