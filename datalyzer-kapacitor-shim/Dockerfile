from node:carbon-alpine

WORKDIR /app
COPY package*.json ./
RUN npm i
RUN npm i nodemon

COPY . .
EXPOSE 300

CMD ["npm", "run", "start"]
