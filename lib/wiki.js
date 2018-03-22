'use strict';

const https = require('https');
const parseUrl = require('url').parse;
const log = require('fastlog')('wiki', process.env.LogLevel || 'info');

const urls = {
    forward: [
        'https://en.wikipedia.org/w/api.php?action=query',
        'prop=links',
        'titles={title}',
        'pllimit=500',
        'plnamespace=0|14|10|100',
        'format=json'
    ].join('&'),
    backward: [
        'https://en.wikipedia.org/w/api.php?action=query',
        'prop=linkshere',
        'titles={title}',
        'lhlimit=500',
        'lhnamespace=0|14|10|100',
        'lhprop=title',
        'lhshow=!redirect',
        'format=json'
    ].join('&')
}

function urlFromTitle(title, direction) {
    title = title.trim();
    title = title[0].toUpperCase() + title.slice(1);
    return encodeURI(urls[direction].replace('{title}', title));
}

function getPage(title, direction) {
    const url = urlFromTitle(title, direction);
    log.debug('getting page', url);

    let options = parseUrl(url);
    options.headers = { "User-Agent": "wikiracer/0.0 (aaronlidman@gmail.com)" };
    // playing nice: https://meta.wikimedia.org/wiki/User-Agent_policy

    // todo: ratelimit, can we avoid it?

    return new Promise((resolve, reject) => {
        https.get(options, response => {
            if (response.statusCode !== 200) {
                return reject('stausCode ' + response.statusCode);
            }

            let results = '';
            response.on('data', chunk => {
                results += chunk;
            });

            // todo: catch `error` event, wikipedia barfs sometimes

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

                log.debug('done requesting', url);

                resolve({
                    continue: parseContinue(results, url),
                    links: parseLinks(results, direction)
                });
            });
        });
    });
};

function parseContinue(pageResults, url) {
    if (pageResults.continue) {
        if (pageResults.continue.plcontinue) {
            return Promise.resolve(url + ('&plcontinue=' + pageResults.continue.plcontinue));
        } else if (pageResults.continue.lhcontinue) {
            return Promise.resolve(url + ('&lhcontinue=' + pageResults.continue.lhcontinue));
        }
    }
    Promise.resolve(null);
}

function parseLinks(pageResults, direction) {
    const pages = pageResults.query.pages;
    const links = [];
    const linkType = (direction == 'forward' ? 'links' : 'linkshere');

    for (const page in pages) {
        // redirect pages don't have any links
        if (pages[page][linkType]) {
            pages[page][linkType].forEach(link => {
                links.push(link.title);
            });
        }
    }

    // normalize mediawiki unicode output, simpler for anyone downstream
    // Pok\u00e9mon => Pokémon
    return links.map(link => link.normalize());
}

function hasDestination(links, destination) {
    return (links.indexOf(destination) > -1);
}

module.exports = {
    getPage: getPage,
    urlFromTitle: urlFromTitle,
    hasDestination: hasDestination
};
