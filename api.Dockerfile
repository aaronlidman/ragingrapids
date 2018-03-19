FROM mhart/alpine-node:6

# use tini for easy signal handling
RUN apk add --no-cache tini

RUN mkdir -p /usr/local/app
COPY . /usr/local/app/
WORKDIR /usr/local/app

RUN npm install --production --no-optional

CMD ["/sbin/tini", "--", "/usr/local/app/bin/api"]