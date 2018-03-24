'use strict';

const http = require('http');
const url = require('url');
const crypto = require('crypto');
const querystring = require('querystring');
const log = require('fastlog')('api', process.env.LogLevel || 'info');
const redis = require('redis');

const client = redis.createClient({
    host: process.env.RedisHost || 'localhost'
});

let server;
const port = 20008;

module.exports = {
    start: start,
    stop: stop
};

function start() {
    client.on('ready', () => {
        log.info('online, setting up api server...');
        setupServer();
    })
}

function stop() {
    log.info('stopping api server...');
    if (server) server.stop();
}

// setup a basic http server, listen for requests at /new and /job
// everything else gets a 404 with some basic debug info
function setupServer() {
    server = http.createServer((request, response) => {
        let urlParts = url.parse(request.url);
        let jobId = querystring.parse(urlParts.query).id;

        if (urlParts.pathname === '/new' && request.method === 'POST') {
            let body = '';
            request.on('data', chunk => body += chunk);

            request.on('end', () => {
                try {
                    body = JSON.parse(body);
                } catch (e) {
                    log.error(e);
                    return respondJSON(response, {
                        received: false,
                        error: e
                    });
                }

                log.info('got', JSON.stringify(body));
                setupJob(body, response);
            });
        } else if (urlParts.pathname === '/job' && jobId && Object.keys(jobId).length) {
            return getJobDetails(jobId).then(result => {
                respondJSON(response, result)
            });
        } else {
            // todo: something more informational about other possible requests
            // or just the frontend
            return respondJSON(response, {
                method: request.method,
                url: request.url
            }, 404);
        }
    });

    log.info('serving on port', port);
    server.listen(port);
}

function setupJob(request, responseHandler) {
    let jobId = [
        request.source,
        request.destination,
        +new Date()
    ].join('');

    jobId = crypto.createHash('md5').update(jobId).digest('hex');

    // queue two messages
        // from the start heading toward the end
        // from the end heading toward the start
    const forwardMessage = { path: [request.source] };
    const backwardMessage = { path: [request.destination] };

    Promise.all([
        createJob(jobId, request.source, request.destination),
        queueMessage(['wikiracer:queue', jobId, 'forward'].join(':'), forwardMessage),
        queueMessage(['wikiracer:queue', jobId, 'backward'].join(':'), backwardMessage)
    ]).then(() => {
        // let the workers know that a job is available
        client.publish('wikiracer:newJobs', jobId);

        // return a jobId that the client polls for updates
        return respondJSON(responseHandler, {
            received: true,
            jobId: jobId,
            message: '/job?id=' + jobId
        });
    }).catch(err => {
        log.error(err);
        return respondJSON(responseHandler, {
            received: false,
            error: err
        });
    });
}

function createJob(jobId, source, destination) {
    return new Promise((resolve, reject) => {
        client.multi()
            .rpush('wikiracer:jobs', jobId)
            .hmset('wikiracer:job:' + jobId, {
                'active': 'true',
                'workers': '0',
                'source': source,
                'destination': destination
            })
            .rpush('wikiracer:job:' + jobId + ':times', JSON.stringify({
                'created': +new Date()
            }))
            .exec((err, replies) => {
                if (err) return reject(err);
                resolve();
            });
    });
}

function queueMessage(queue, message) {
    return new Promise((resolve, reject) => {
        client.rpush(queue, JSON.stringify(message), (err, result) => {
            if (err) return reject(err);
            resolve();
        });
    });
}

function getJobDetails(jobId) {
    return new Promise((resolve, reject) => {
        client.multi()
            .hgetall('wikiracer:job:' + jobId)
            .lrange('wikiracer:job:' + jobId + ':times', 0, -1)
            .exec((err, results) => {
                if (err) return resolve({error: err});

                const jobDetails = results[0];
                const duration = getDuration(results[1]);

                if (jobDetails === null) {
                    return resolve({ message: 'Job ' + jobId + ' not found' });
                }

                resolve({
                    active: (jobDetails.active == 'true'),
                    duration: duration,
                    path: JSON.parse(jobDetails.path),
                    message: jobDetails.message
                });
            });
    });
}

function getDuration(timings) {
    // get first start
    let start = timings.map(JSON.parse)
        .filter(time => 'started' in time)
        .sort((a, b) => {
            return parseInt(a.started) > parseInt(b.started);
        });

    if (start.length) start = start[0].started;

    // get last finish
    let finish = timings.map(JSON.parse)
        .filter(time => 'finished' in time)
        .sort((a, b) => {
            return parseInt(a.started) < parseInt(b.started);
        });

    if (finish.length) finish = finish[0].finished;

    return (start && finish) ? (finish - start) : null;
}

function respondJSON(response, obj, statusCode) {
    response.writeHead(statusCode || 200, {'Content-Type': 'application/json'});
    return response.end(JSON.stringify(obj));
}
