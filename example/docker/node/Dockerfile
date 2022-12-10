FROM node:16

COPY package.json .
COPY tsconfig.json .

RUN npm install

RUN npm run build

COPY . .

CMD ["npm", "run start"]