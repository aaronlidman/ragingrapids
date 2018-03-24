'use strict';

const redis = require('redis');
const log = require('fastlog')('worker', process.env.LogLevel || 'info');
const wiki = require('./wiki');

const client = redis.createClient({
    host: process.env.RedisHost || 'localhost'
});

let emptyQueueRetries = 10;

module.exports = {
    start: start,
    stop: stop
};

function start() {
    client.on('ready', () => {
        log.info('worker online...');
        getJob();
    });
}

function stop() {
    log.info('stopping worker...');
    if (client.connected) client.quit();
}

// get a job either by finding one that is already available
// or waiting for the next one to be published
function getJob() {
    client.lindex('wikiracer:jobs', 0, (err, jobId) => {
        if (err) return log.error(err);
        if (jobId !== null) return attachWorker(jobId);

        listen().then(attachWorker);
    });
}

function listen() {
    return new Promise((resolve, reject) => {
        log.debug('waiting for a job...');
        client.subscribe('wikiracer:newJobs');
        client.once('message', (channel, jobId) => {
            client.unsubscribe();
            return resolve(jobId);
        });
    });
}

// stick to a particular queue until the job is done or the queue is empty
function attachWorker(jobId) {
    client.hincrby('wikiracer:job:' + jobId, 'workers', 1, (err, workerNum) => {
        if (err) return log.error(err);

        log.info('attaching to', jobId);
        // odd = forward, even = backward
        // this insures the queues stay somewhat balanced
        const direction = ((workerNum % 2) == 0) ? 'backward' : 'forward';
        const queue = ['wikiracer:queue', jobId, direction].join(':');

        getMessage(queue, jobId);
    });
}

function getMessage(queue, jobId) {
    log.debug('getting a message from queue', queue);

    client.multi()
        .hget('wikiracer:job:' + jobId, 'active')
        .lpop(queue)
        .exec((err, results) => {
            if (err) return reject(err);

            const active = results[0];
            const message = results[1];

            // job was recently made inactive, move on to the next job
            if (active !== 'true') return getJob();

            // empty queue, but job is still active for some reason
            // try 10 times, then break out of this loop, resolve job, and get a new job
            if (message == null) {
                log.debug('empty queue hit');
                emptyQueueRetries -= 1;

                if (emptyQueueRetries === 0) {
                    // reset retries, mark the job as innactive and move on
                    emptyQueueRetries = 10;

                    // todo: resolveJob({jobId: jobId}, null, 'no path found');
                    // then(getJob)
                }

                // try again in 250ms
                return setTimeout(() => getMessage(queue, jobId), 250);
            }

            if (emptyQueueRetries !== 10) emptyQueueRetries = 10;

            log.debug('message received', message);
            processMessage(JSON.parse(message));
        });
}

function processMessage(message) {
    const title = message.path[message.path.length-1];

    wiki.getLinks(title, message.direction)
        .then(links => searchAndUpdateGraph(links, message))
        .then(result => {
            if (result.connection !== null) {
                // todo: forward/backward distinction for path formation
                return resolveJob(message, result.connection);
            }
            // not resolved, queue the next batch and continute with the queue
            return queueLinks(result.links, message);
        })
        .catch(log.error)
        .then(() => getMessage(message.queue, message.jobId));
}

function resolveJob(message, path, displayMessage) {
    if (path) log.info('found a path!', JSON.stringify(path));

    return new Promise((resolve, reject) => {
        client.multi()
            .del(message.queue.replace('forward', 'backward'))
            .del(message.queue.replace('backward', 'forward'))
            .lpop('wikiracer:jobs')
            .hmset('wikiracer:job:' + message.jobId, {
                'active': 'false',
                'path': JSON.stringify(path),
                'message': displayMessage ? displayMessage : 'null'
            })
            .exec((err, results) => {
                if (err) return reject(err);
                resolve();
            });
    });
}

function searchAndUpdateGraph(links, message) {
    return new Promise((resolve, reject) => {
        const setObj = {};
        links.forEach(link => {
            setObj[link] = JSON.stringify(message.path.concat([link]));
        });

        // todo: test empty links array for .hmget

        // request 1: set all the connections for the current set of links
        // request 2: have any of these links been connected to the destination before?
        client.multi()
            .hmset(message.source, setObj)
            .hmget(message.destination, links)
            .exec((err, results) => {
                if (err) return reject(err);

                // filter out all the null results
                const connections = results[1].filter(result => {
                    return !!result;
                });

                return resolve({
                    connection: connections.length ? JSON.parse(connections[0]) : null,
                    links: links
                });
            });
    });
}

function queueLink(message) {
    return new Promise((resolve, reject) => {
        client.rpush(message.queue, JSON.stringify(message), (err, response) => {
            if (err) return reject(err);
            return resolve();
        });
    });
}

function queueLinks(links, message) {
    // create a new message, mostly copied from the original message
    const messages = links.map(link => {
        return {
            jobId: message.jobId,
            queue: message.queue,
            direction: message.direction,
            source: message.source,
            destination: message.destination,
            path: message.path.concat([link])
        }
    });

    log.debug('queuing', messages.length, 'links');
    return Promise.all(messages.map(queueLink));
}
