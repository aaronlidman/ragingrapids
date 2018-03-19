all: build up

build:
	docker-compose build

up:
	docker-compose up

# for development, reset the queue
reset:
	echo "del rsmq:fetch" | redis-cli
	echo "del rsmq:QUEUES" | redis-clif
	echo "del rsmq:fetch:Q" | redis-cli
