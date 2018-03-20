'use strict';

const https = require('https');
const parseUrl = require('url').parse;
const log = require('fastlog')('wiki', process.env.LogLevel || 'info');

const url = [
    'https://en.wikipedia.org/w/api.php?action=query',
    'prop=links',
    'titles={title}',
    'pllimit=500',
    'plnamespace=0',
    'format=json',
    'redirects'
].join('&');

function normalizeTitle(title) {
    title = title.trim();
    return encodeURIComponent(title[0].toUpperCase() + title.slice(1));
}

function getPageByTitle(title) {
    return getPage(urlFromTitle(title), title);
}

function urlFromTitle(title) {
    title = normalizeTitle(title);
    const apiUrl = url.replace('{title}', title);
    return apiUrl;
}

function getPage(url) {
    log.debug('getting page', url);
    let options = parseUrl(url);

    // todo: look for cached results first

    // todo: ratelimit
    // 10/s/domain?

    // playing nice: https://meta.wikimedia.org/wiki/User-Agent_policy
    options.headers = {
        "User-Agent": "wikiracer/0.0 (aaronlidman@gmail.com)"
    };

    return new Promise((resolve, reject) => {
        https.get(options, response => {
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

                // catch 404s early
                if (results.query.pages['-1']) {
                    return reject({
                        name: 'pageNotFound',
                        url: url
                    });
                }

                // todo: cache just resulting links

                resolve({
                    continue: parseContinue(results, url),
                    links: parseLinks(results)
                });
            });
        });
    });
};

function parseContinue(pageResults, url) {
    if (pageResults.continue && pageResults.continue.plcontinue) {
        Promise.resolve(url + ('&plcontinue=' + pageResults.continue.plcontinue));
    } else {
        Promise.resolve(null);
    };
}

function parseLinks(pageResults) {
    const pages = pageResults.query.pages;
    const links = [];

    for (const page in pages) {
        pages[page].links.forEach(link => {
            links.push(link.title);
        });
    }

    // normalize mediawiki unicode output, simpler for anyone downstream
    // Pok\u00e9mon => Pokémon
    return links.map(link => link.normalize());
}

function hasDestination(links, destination) {
    return (links.indexOf(destination) > -1);
}

module.exports = {
    getPageByTitle: getPageByTitle,
    parseLinks: parseLinks,
    urlFromTitle: urlFromTitle,
    hasDestination: hasDestination
};
