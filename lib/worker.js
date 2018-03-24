'use strict';

const redis = require('redis');
const log = require('fastlog')('worker', process.env.LogLevel || 'info');
const wiki = require('./wiki');

const client = redis.createClient({
    host: process.env.RedisHost || 'localhost'
});

let job = null;

module.exports = {
    start: start,
    stop: stop
};

function start() {
    client.on('ready', () => {
        log.info('worker online');
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
    resetJob();

    client.lindex('wikiracer:jobs', 0, (err, jobId) => {
        if (err) return log.error(err);
        if (jobId !== null) return attachWorker(jobId);

        listen().then(attachWorker);
    });
}

function resetJob() {
    job = {
        id: null,
        queue: null,
        direction: null,
        source: null,
        destination: null,
        emptyQueueRetries: 10
    };
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

// attach to a particular job until the job is done or the queue is empty
function attachWorker(jobId) {
    client.multi()
        .hincrby('wikiracer:job:' + jobId, 'workers', 1)
        .hgetall('wikiracer:job:' + jobId)
        .exec((err, results) => {
            if (err) return log.error(err);

            // odd = forward, even = backward
            // this insures the queues stay somewhat balanced
            const workerNum = results[0];
            const jobDetails = results[1];

            job.id = jobId;
            job.direction = ((workerNum % 2) == 0) ? 'backward' : 'forward';
            job.queue = ['wikiracer:queue', job.id, job.direction].join(':');
            job.source = (job.direction == 'forward') ? jobDetails.source : jobDetails.destination;
            job.destination = (job.direction == 'forward') ? jobDetails.destination : jobDetails.source;

            log.info('attaching to', jobId);
            getMessage();
        });
}

function getMessage() {
    log.debug('getting a message from queue', job.queue);

    client.multi()
        .hget('wikiracer:job:' + job.id, 'active')
        .lpop(job.queue)
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
                job.emptyQueueRetries -= 1;

                if (job.emptyQueueRetries === 0) {
                    // reset retries, mark the job as innactive and move on
                    job.emptyQueueRetries = 10;

                    // todo: resolveJob(null, 'no path found');
                    // then(getJob)
                }

                // try again in 250ms
                return setTimeout(() => getMessage(), 250);
            }

            if (job.emptyQueueRetries !== 10) job.emptyQueueRetries = 10;

            log.debug('message received', message);
            processMessage(JSON.parse(message));
        });
}

function processMessage(message) {
    const title = message.path[message.path.length-1];

    wiki.getLinks(title, job.direction)
        .then(links => searchAndUpdateGraph(links, message.path))
        .then(result => {
            if (result.connection !== null) {
                // todo: forward/backward distinction for path formation
                return resolveJob(result.connection);
            }
            // not resolved, queue the next batch and continute with the queue
            return queueLinks(result.links, message.path);
        })
        .catch(log.error)
        .then(() => getMessage());
}

function resolveJob(path, displayMessage) {
    if (path) log.info('found a path!', JSON.stringify(path));

    return new Promise((resolve, reject) => {
        client.multi()
            .del(job.queue.replace('forward', 'backward'))
            .del(job.queue.replace('backward', 'forward'))
            .lpop('wikiracer:jobs')
            .hmset('wikiracer:job:' + job.id, {
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

function searchAndUpdateGraph(links, path) {
    return new Promise((resolve, reject) => {
        const setObj = {};
        links.forEach(link => {
            setObj[link] = JSON.stringify(path.concat([link]));
        });

        // todo: test empty links array for .hmget

        // request 1: set all the connections for the current set of links
        // request 2: have any of these links been connected to the destination before?
        client.multi()
            .hmset(job.source, setObj)
            .hmget(job.destination, links)
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
        client.rpush(job.queue, JSON.stringify(message), (err, response) => {
            if (err) return reject(err);
            return resolve();
        });
    });
}

// create messages out of the links we found and add them to the queue
function queueLinks(links, path) {
    const messages = links.map(link => {
        return { path: path.concat([link]) };
    });
    return Promise.all(messages.map(queueLink));
}
