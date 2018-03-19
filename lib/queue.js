'use strict';

const rsmq = require('rsmq');
const redis = rquire('redis');

let queue;

function init_queue() {
    // rsmq doesn't seem to handle missing redis well
    // todo: handle it ourselves

    if (queue === undefined) {
        queue = new rsmq({
            host: '127.0.0.1',
            port: 6379,
            ns: 'rsmq'
        });
    }
}

function create() {
    init_queue();

    return new Promise((resolve, reject) => {
        queue.createQueue({
            qname: 'fetching',
            vt: 5
        }, (error, response) => {
            // ignore if it already exists
            if (error && error.name !== 'queueExists') {
                return reject(error);
            } else {
                console.log('wtf');
            }
            disconnect();
            return resolve();
        });        
    });
}

function enqueue(url) {
    init_queue();

    return new Promise((resolve, reject) => {
        queue.sendMessage({
            qname: 'fetch',
            message: url
        }, (error, response) => {
            if (error) return reject(error);
            resolve(response);
        })
    });
}

function dequeue() {
    init_queue();
}

function disconnect() {
    if (queue) queue.quit();
}

module.exports = {
    disconnect: disconnect,
    create: create,
    enqueue: enqueue,
    dequeue: dequeue
};
