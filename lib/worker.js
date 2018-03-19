'use strict';

const rsmq = require('rsmq');
const log = require('fastlog')('worker', process.env.LogLevel || 'info');

const queue = new rsmq({
    host: process.env.RedisHost || 'localhost'
});

const pollInterval = 100;

function init() {
    log.info('worker online, polling...');
    log.info('pollInterval:', pollInterval);
    getMessage();
}

// grabs a message from the queue and hands it off if there's work to be done
// if the queue is empty it waits `pollInterval` ms and checks again
function getMessage() {
    log.debug('polling');

    queue.receiveMessage({
        qname: 'fetch'
    }, (err, response) => {
        if (err && err.name === 'queueNotFound') {
            // queue is missing on initial start until the first queued message
            // keep polling, but a little slower
            return setTimeout(getMessage, pollInterval * 10);
        }

        if (err) return log.error(err);

        if (Object.keys(response).length > 0) {
            return doWork(response);
        } else {
            setTimeout(getMessage, pollInterval);
        }
    });
}

function doWork(message) {
    log.debug('message received:', JSON.stringify(message));

    // do the work
    // delete the message if everything's good
    // finally call getMessage again

    setTimeout(() => {
        log.debug('done, back to polling');
        getMessage();
    }, pollInterval * 20);
}

module.exports = {
    init: init
};
