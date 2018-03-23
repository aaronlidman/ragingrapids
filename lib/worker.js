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
    const title = message.path[message.path.length-1];
    let links;

    jobIsActive(message.jobId)
        .then(() => wiki.getPage(title, message.jobType, client))
        .then(results => {
            if (results.continue) queueContinue(results.continue);
            return links = results.links;
        })
        .then(() => wiki.hasDestination(links, message.destination))
        .then(status => findConnectors(status, links, message))
        .then(results => updateJobStatus(results, message))
        .then(status => queueLinks(status, links, message))
        .catch(err => {
            if (err.name != 'jobNotActive') log.error(err);
        })
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

function updateJobStatus(results, message) {
    return new Promise((resolve, reject) => {
        if (results.foundDestination) {
            if (message.jobType === 'forward') {
                log.info('found destination', message.path.concat(results.destinationPath));
            } else {
                log.info('found destination', results.destinationPath.concat(message.path.reverse()));
            }

            // todo: what about straight up directional find all the way?

            // todo: set final path, set end time, a bunch of things

            client.hset('job:' + message.jobId, 'active', 'false', (err, response) => {
                if (err) return reject(err);
                console.log('everyone should stop soon', response);
                resolve({ active: false });
            });
        } else {
            resolve({ active: true });
        }
    });
}

function findConnectors(status, links, message) {
    // todo: resolve these better
    if (status) return Promise.resolve({
        foundDestination: status
    });
    if (!links.length) return Promise.resolve({
        foundDestination: false
    });

    return new Promise((resolve, reject) => {
        // has anything we're about to queue been linked to the destination before?

        client.hmget(message.destination, links, (err, results) => {
            if (err) return reject(err);

            results = results.filter(result => {
                return !!result;
            });

            if (results[0]) console.log('hey', JSON.stringify(message), results[0]);

            if (results.length) return resolve({
                foundDestination: true,
                destinationPath: JSON.parse(results[0])
            });

            updateGraph(links, message)
                .then(() => resolve(false, message));
        });
    });
}

function updateGraph(links, message) {
    const backward = (message.jobType == 'backward');

    return new Promise((resolve, reject) => {
        const setObj = {};

        links.forEach(link => {
            let path = message.path.concat([link]);
            setObj[link] = JSON.stringify(path);
        });

        client.hmset(message.source, setObj, (err, results) => {
            if (err) {
                console.log('mismatch problem:', JSON.stringify(setObj));
                return reject(err);
            }

            resolve();
        });
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
    if (!status.active) return Promise.resolve();

    // create a new message, mostly copied from the original message
    const messages = links.map(link => {
        return {
            jobId: message.jobId,
            jobType: message.jobType,
            source: message.source,
            destination: message.destination,
            path: message.path.concat([link])
        }
    });

    log.debug('queuing', messages.length, 'links');
    return Promise.all(messages.map(queueLink));
}

function deleteMessage(id) {
    log.debug('deleting', id);

    return new Promise((resolve, reject) => {
        queue.deleteMessage({
            qname: 'fetch',
            id: id
        }, () => {});
        // don't wait for response
        resolve();
    });
}

module.exports = {
    start: start,
    stop: stop
};
