all: up

up:
	bash -c "trap 'docker-compose down' EXIT; docker-compose up --build --scale worker=2"

# for development
reset:
	echo 'flushdb' | redis-cli
