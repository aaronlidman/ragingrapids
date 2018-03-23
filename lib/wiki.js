'use strict';

const https = require('https');
const urlUtil = require('url');
const log = require('fastlog')('wiki', process.env.LogLevel || 'info');

module.exports = {
    getLinks: getLinks,
    hasDestination: hasDestination
};

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
        'format=json'
    ].join('&')
}

function urlFromTitle(title, direction) {
    title = title.trim();
    title = title[0].toUpperCase() + title.slice(1);
    return encodeURI(urls[direction].replace('{title}', title));
}

function getLinks(title, direction, links, url) {
    if (!url) url = urlFromTitle(title, direction);

    log.debug('getting page', url);

    let options = urlUtil.parse(url);
    options.headers = { "User-Agent": "wikiracer/0.0 (aaronlidman@gmail.com)" };

    return new Promise((resolve, reject) => {
        https.get(options, response => {
            if (response.statusCode !== 200) {
                return reject('stausCode ' + response.statusCode);
            }

            let results = '';
            response.on('data', chunk => results += chunk);

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

                // append any new links to the existing links array, for continues
                // loop around as many times as needed and finally return all links at once
                const parsed = parseLinks(results, direction);
                links = (links || []).concat(parsed);

                const continueUrl = parseContinue(results, url);
                if (continueUrl) {
                    return resolve(getLinks(null, direction, links, continueUrl));
                } else {
                    return resolve(links);
                }
            });
        });
    });
}

function parseContinue(pageResults, url) {
    if (!pageResults.continue) return null;

    if (pageResults.continue.plcontinue) {
        // if the url already has a `continue` we need to remove it so there is only one
        url = url.split('&plcontinue')[0];
        return url + '&plcontinue=' + pageResults.continue.plcontinue;
    } else if (pageResults.continue.lhcontinue) {
        url = url.split('&lhcontinue')[0];
        return url + '&lhcontinue=' + pageResults.continue.lhcontinue;
    }
}

function parseLinks(pageResults, direction) {
    const pages = pageResults.query.pages;
    const links = [];
    const linkType = (direction == 'forward' ? 'links' : 'linkshere');

    for (const page in pages) {
        // `if` because redirect pages don't have links
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
