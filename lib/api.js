'use strict';

const http = require('http');
const url = require('url');
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

// setup a basic http server, listen for requests, and parse POST requests to /new
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
            return respondJSON(response, {
                message: 'hello',
                id: jobId
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
    const jobId = [request.source, request.destination].join('=>');

    // queue two messages
        // from the start looking toward the end
        // from the end looking toward the start
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
            jobId: jobId
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

function respondJSON(response, obj, statusCode) {
    response.writeHead(statusCode || 200, {'Content-Type': 'application/json'});
    return response.end(JSON.stringify(obj));
}
