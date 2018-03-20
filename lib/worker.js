'use strict';

const rsmq = require('rsmq');
const redis = require('redis');
const log = require('fastlog')('worker', process.env.LogLevel || 'info');
const wiki = require('./wiki');

const client = redis.createClient({ host: process.env.RedisHost || 'localhost' });
const queue = new rsmq({ client: client });

const pollInterval = 100;
let polling = true;

function start() {
    log.info('worker online, polling...');
    log.info('pollInterval:', pollInterval);
    poll();
}

function stop() {
    log.info('stopping worker...');
    polling = false;
}

function poll() {
    if (polling) setTimeout(getMessage, pollInterval);
}

// grabs a message from the queue and hands it off if there's work to be done
// if the queue is empty, waits and trys again after a delay
function getMessage() {
    log.debug('polling');

    queue.receiveMessage({
        qname: 'fetch'
    }, (err, response) => {
        // queue hasn't been created until the first queued message
        // keep polling, it will show up eventually
        // ideally our queue lib would handle this transparently
        if (err && err.name === 'queueNotFound') return poll();
        if (err) return log.error(err);

        if (Object.keys(response).length > 0) {
            return processMessage(response);
        } else {
            // empty queue, keep polling
            return poll();
        }
    });
}

function processMessage(response) {
    log.debug('message received:', JSON.stringify(response));

    const message = JSON.parse(response.message);
    let links;

    jobIsActive(message.jobId)
        .then(() => wiki.getPageByTitle(message.title))
        .then(results => {
            if (results.continue) queueContinue(results.continue);
            return links = results.links;
        })
        .then(() => wiki.hasDestination(links, message.destination))
        .then(result => updateJobStatus(result, message))
        .then(status => queueLinks(status, links, message))
        .catch(log.error)
        .then(() => deleteMessage(response.id))
        .then(() => poll());
}

function queueContinue(url) {
    // todo: queue the url just like api.js

    // notice no one is waiting for this to resolve, it's not that important
    // we're more concerned about time

    return Promise.resolve();
}

function jobIsActive(jobId) {
    // will only resolve if the job is active
    // anything else errors
    // which we use to skip the promise chain and go straight to deleting the message
    return new Promise((resolve, reject) => {
        client.hget('job:' + jobId, 'active', (err, response) => {
            if (err) return reject(err);
            if (response == 'true') {
                return resolve();
            } else {
                reject({
                    name: 'jobNotActive',
                    jobId: jobId
                });
            }
        });
    });
}

function updateJobStatus(foundDestination, message) {
    log.debug(foundDestination ? 'found!' : 'not found');

    return new Promise((resolve, reject) => {
        if (foundDestination) {
            log.info('found destination');

            // todo: also set final path, time, a bunch of things

            client.hset('job:' + message.jobId, 'active', 'false', (err, response) => {
                if (err) return reject(err);
                console.log('everyone should stop soon', response);
                resolve();
            });
        } else {
            resolve();
        }
    });
}

function queueLink(message) {
    return new Promise((resolve, reject) => {
        queue.sendMessage({
            qname: 'fetch',
            message: JSON.stringify(message)
        }, (err, response) => {
            if (err) return reject(err);
            return resolve();
        });
    });
}

function queueLinks(status, links, message) {
    // if the destination was found, these are queued unnecessarily
    // but the next worker will pick that up immediately

    // create a new message, mostly copied from the original message
    const messages = links.map(link => {
        return {
            jobId: message.jobId,
            destination: message.destination,
            title: link,
            reversed: message.reversed,
            path: message.path.concat([message.title])
        }
    });

    return Promise.all(messages.map(queueLink));
}

function deleteMessage(id) {
    log.debug('deleting', id);

    return new Promise((resolve, reject) => {
        queue.deleteMessage({
            qname: 'fetch',
            id: id
        }, (err, response) => {
            if (err) return reject(err);
            resolve();
        });
    });
}

module.exports = {
    start: start,
    stop: stop
};
