'use strict';

const https = require('https');
const parseUrl = require('url').parse;

const url = [
    'https://en.wikipedia.org/w/api.php?action=query',
    'prop=links',
    'titles={title}',
    'pllimit=500',
    'plnamespace=0',
    'format=json'
].join('&');

function normalizeTitle(title) {
    title = title.trim();
    return encodeURIComponent(title[0].toUpperCase() + title.slice(1));
}

function getPageByTitle(title) {
    return getPage(urlFromTitle(title));
}

function urlFromTitle(title) {
    title = normalizeTitle(title);
    const apiUrl = url.replace('{title}', title);
    return apiUrl;
}

function getPage(url) {
    let options = parseUrl(url);

    // playing nice: https://meta.wikimedia.org/wiki/User-Agent_policy
    options.headers = {
        "User-Agent": "wikiace/0.0 (aaronlidman@gmail.com)"
    };

    return new Promise((resolve, reject) => {
        https.get(options, res => {
            if (response.statusCode !== 200) {
                return reject('stausCode ' + response.statusCode);
            }

            let results = '';
            response.on('data', chunk => {
                results += chunk;
            });

            response.on('end', () => {
                try {
                    results = JSON.parse(results);
                } catch (e) {
                    return reject(e);
                }

                // peak into the results to catch 404s a little sooner
                if (results.query.pages['-1']) {
                    return reject('page not found');
                }

                // queue any additional pages of results for other workers
                enqueueContinue(results.continue || {}, url)
                    .then(resolve(results));
            });
        });
    });
};

function enqueueContinue(continueObj, url) {
    if ('plcontinue' in continueObj) {
        url += ('&plcontinue=' + continueObj.plcontinue);
        // todo:
            // enqueue it
            // resolve after the queue was succesful
                // reject for any reason?
                // probably not because we can always find other paths
        console.log('found a continue, will queue', url);
        return Promise.resolve();
    } else {
        return Promise.resolve();
    }
}

function parseLinks(pageResults) {
    const pages = pageResults.query.pages;
    const links = [];

    for (const page in pages) {
        pages[page].links.forEach(link => {
            links.push(link.title);
        });
    }

    return links;
}

module.exports = {
    getPageByTitle: getPageByTitle,
    parseLinks: parseLinks,
    urlFromTitle: urlFromTitle
};
