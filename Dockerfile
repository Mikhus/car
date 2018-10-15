FROM node:8.12.0

# Environment variables
ENV PATH /opt/app/node_modules/.bin:$PATH

# Create app directory
RUN mkdir -p /opt/app
WORKDIR /opt/app

# Install app dependencies
COPY package.json /opt/app/
RUN npm i

# Bundle app package files
COPY . /opt/app
RUN npm i

# Execute service
CMD [ "npm", "start" ]
