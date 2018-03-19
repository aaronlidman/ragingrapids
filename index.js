'use strict';

const args = require('minimist')(process.argv.slice(2), {
    'alias': {
        'start': 's',
        'end': 'e'
    }
});

const wiki = require('./lib/wiki.js');
const queue = require('./lib/queue.js');

// [args.start, args.end].forEach(title => {
//     wiki.getPageByTitle(args.start)
//         .then(wiki.parseLinks)
//         .then(links => {
//             // only one step removed
//             return (links.indexOf(args.end) > -1) ? 'yes' : 'no';
//         })
//         .then(results => console.log(results))
//         .catch(err => console.error(err));
// });

// all workers will need:
    // overall job id, not just message id, from redis
    // destination, from redis
    // lineage

// server does two things:
    // if there is no queue yet create one
    // start server

queue.create()
    .then(server.start)
    .catch(console.error)
