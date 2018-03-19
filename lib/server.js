'use strict';

const http = require('http');
const ws = require('ws');
const log = require('fastlog')('server', process.env.LogLevel || 'info');
const rsmq = require('rsmq');

const queue = new rsmq({
    host: process.env.RedisHost || 'localhost'
});

const port = 20008;

let server;

function init() {
    log.info('online, setting up server...');
    return setupServer();
}

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

        // create a message
        const message = {
            jobId: [body.start, body.end].join('=>'),
            start: body.start,
            end: body.end,
            path: []
        };

        // add the message to the queue
        queue.sendMessage({
            qname: 'fetch',
            message: JSON.stringify(message)
        }, (err, response) => {
            if (err) return log.error(err);

            // respond to the user
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
