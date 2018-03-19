FROM mhart/alpine-node:6

RUN mkdir -p /usr/local/app
COPY . /usr/local/app/
WORKDIR /usr/local/app

RUN npm install --production --no-optional

CMD ["/usr/local/app/bin/api"]