'use strict';

const http = require('http');
const log = require('fastlog')('api', process.env.LogLevel || 'info');
const redis = require('redis');
const rsmq = require('rsmq');

const client = redis.createClient({ host: process.env.RedisHost || 'localhost' });
const queue = new rsmq({ client: client });

let server;
const port = 20008;

function start() {
    log.info('online, setting up api server...');
    return setupServer();
}

function stop() {
    log.info('stopping api server...');
    if (server) server.stop();
}

// setup a basic http server and listen for requests
function setupServer() {
    server = http.createServer((req, resp) => {
        if (req.url === '/new' && req.method === 'POST') {
            addNewJob(req, resp);
        } else {
            return respondJSON(resp, {
                method: req.method,
                url: req.url
            });
        }
    });

    log.info('serving on port', port);
    return server.listen(port);
}

// parse a POST request with a new job to be done
// we form this request into a message and add it to the queue
function addNewJob(req, resp) {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => {
        log.debug('got', body);

        // careful with user input
        try {
            body = JSON.parse(body);
        } catch (e) {
            log.error(e);

            return respondJSON(resp, {
                received: false,
                error: e
            });
        }

        // todo: some really basic validation

        // queue two messages
            // from the start looking toward the end
            // from the end looking toward the start

        const jobId = body.source + '=>' + body.destination;

        const forwardMessage = {
            jobId: jobId,
            title: body.source,
            destination: body.destination,
            reversed: false,
            path: []
        };

        const backwardMessage = {
            jobId: jobId,
            title: body.destination,
            destination: body.source,
            reversed: true,
            path: []
        };

        createJob(jobId)
            .then(() => {
                // return a jobId that the client polls for updates
                // everything uses that jobId as an identifier
                Promise.all([
                    queueMessage(forwardMessage),
                    queueMessage(backwardMessage)
                ]).then(() => {
                    return respondJSON(resp, {
                        received: true,
                        jobId: jobId
                    });
                })
            })
            .catch(err => {
                log.error(err);
                return respondJSON(resp, {
                    received: false,
                    error: err
                });
            });
    });
}

// a central place to track the job
function createJob(jobId) {
    return Promise.resolve();
}

function queueMessage(message) {
    return new Promise((resolve, reject) => {
        // create the queue if it doesn't exist yet
        queue.createQueue({
            qname: 'fetch'
        }, (err, result) => {
            if (err && (err.name != 'queueExists')) {
                return reject(err);
            }

            message = JSON.stringify(message);
            log.debug('queuing message', message);

            queue.sendMessage({
                qname: 'fetch',
                message: message
            }, (err, response) => {
                if (err) return reject(err);
                return resolve();
            });
        });

    });
}

function respondJSON(response, obj) {
    response.writeHead(200, {'Content-Type': 'application/json'});
    return response.end(JSON.stringify(obj));
}

module.exports = {
    start: start,
    stop: stop
};
