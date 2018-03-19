'use strict';

const http = require('http');
const ws = require('ws');
const log = require('fastlog')('api', process.env.LogLevel || 'info');
const rsmq = require('rsmq');

const queue = new rsmq({
    host: process.env.RedisHost || 'localhost'
});

const port = 20008;

function init() {
    log.info('online, setting up api server...');
    return setupServer();
}

// setup a basic http and websocket server and listen for requests
function setupServer() {
    const server = http.createServer((req, resp) => {
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

        const message = {
            jobId: [body.start, body.end].join('=>'),
            start: body.start,
            end: body.end,
            path: []
        };

        queue.sendMessage({
            qname: 'fetch',
            message: JSON.stringify(message)
        }, (err, response) => {
            if (err) return log.error(err);

            return respondJSON(resp, {
                received: true,
                jobId: message.jobId
            });
        });
    });
}

function respondJSON(response, obj) {
    response.writeHead(200, {'Content-Type': 'application/json'});
    return response.end(JSON.stringify(obj));
}

module.exports = init;
