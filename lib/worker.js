'use strict';

const redis = require('redis');
const log = require('fastlog')('worker', process.env.LogLevel || 'info');
const wiki = require('./wiki');

const client = redis.createClient({
    host: process.env.RedisHost || 'localhost'
});

let job = null;
let maxDuration = 120;

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
        log.info('waiting for a job...');
        client.subscribe('wikiracer:newJobs');
        client.once('message', (channel, jobId) => {
            client.unsubscribe();
            return resolve(jobId);
        });
    });
}

// attach to a particular job queue
function attachWorker(jobId) {
    client.multi()
        .hincrby('wikiracer:job:' + jobId, 'workers', 1)
        .hgetall('wikiracer:job:' + jobId)
        .rpush('wikiracer:job:' + jobId + ':times', JSON.stringify({
            'started': +new Date()
        }))
        .exec((err, results) => {
            if (err) return log.error(err);

            // odd = forward, even = backward
            // this insures workers get distributed to queues evenly
            const workerNum = results[0];
            const jobDetails = results[1];

            job.id = jobId;
            job.direction = ((workerNum % 2) == 0) ? 'backward' : 'forward';
            job.queue = ['wikiracer:queue', job.id, job.direction].join(':');
            job.source = (job.direction == 'forward') ? jobDetails.source : jobDetails.destination;
            job.destination = (job.direction == 'forward') ? jobDetails.destination : jobDetails.source;

            log.info('searching', job.source, 'to', job.destination, '(' + job.id + ')');
            getMessage();
        });
}

function getMessage() {
    client.multi()
        .hget('wikiracer:job:' + job.id, 'active')
        .lpop(job.queue)
        .lrange('wikiracer:job:' + job.id + ':times', 0, -1)
        .exec((err, results) => {
            if (err) return reject(err);

            const activeJob = results[0];
            const message = results[1];
            const timings = results[2];

            // exit early
            if (activeJob !== 'true') return getJob();
            if (getDuration(timings) > maxDuration) {
                return resolveJob(null, 'no path found within 2 minutes');
            }

            // empty queue, but job is still active, likely no connection
            // try 10 times, then resolve job as a failure and move on
            if (message == null) {
                job.emptyQueueRetries -= 1;
                if (job.emptyQueueRetries === 0) resolveJob(null, 'no path found');
                return setTimeout(() => getMessage(), 250);
            }

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
                // form the path slightly differently for forward/backward
                let path = message.path.concat(result.connection.reverse());
                if (job.direction === 'backward') {
                    path = result.connection.reverse().concat(message.path.reverse());
                }

                return resolveJob(path);
            }
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
            .rpush('wikiracer:job:' + job.id + ':times', JSON.stringify({
                'finished': +new Date()
            }))
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
        if (!links.length) return resolve({
            connection: null,
            links: links
        });

        const setObj = {};
        links.forEach(link => {
            setObj[link] = JSON.stringify(path.concat([link]));
        });

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
        return {
            path: path.concat([link])
        };
    });
    return Promise.all(messages.map(queueLink));
}

function getDuration(timings) {
    timings = timings.map(JSON.parse);
    // get the earliest start time from any worker
    const start = timings
        .filter(time => 'started' in time)
        .sort((a, b) => {
            return parseInt(a.started) > parseInt(b.started);
        })[0].started;

    return (+new Date() - parseInt(start)) / 1000;
}
