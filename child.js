/**
 * Created by rafaelkallis on 02.10.16.
 * Worker / Computation
 */
var http = require('http');
var bluebird = require('bluebird');
var paths = process.env.PATHS || [];

function filterPushEvents(events) {
    return events.filter((event) => {
        return event.type == 'PushEvent';
    });
}
function mapDistinctRepositoryCommits(push_events) {
    return push_events.map((push_event) => {
        return {
            repo: push_event.repo.name,
            commits: push_event.payload.commits
                .filter((commit) => commit.distinct)
                .map((commit) => {
                    return {
                        repo: push_event.repo.name,
                        sha: commit.sha,
                        authorEmailSha: commit.author.email.substr(0, 40)
                    }
                })
        };
    });
}
function fetchData(paths, callback) {
    const hostname = `data.githubarchive.org`;
    let zlib = require('zlib');
    bluebird.all(paths.map((path) => new Promise((resolve, reject) => {
        let server_req = http.request({
            hostname: hostname,
            path: path
        }, (response) => {
            let chunks = [];
            let pipe = response.pipe(zlib.createGunzip());
            pipe.on('data', (chunk) => {
                chunks.push(chunk);
            });
            pipe.on('end', () => {
                process.send({
                    pid: process.pid,
                    type: 'log',
                    level: 'log',
                    payload: `started processing request response`
                });
                let chunk = Buffer.concat(chunks);
                chunks = null;
                process.send({pid: process.pid, type: 'log', level: 'log', payload: `concatenated successfully`});
                let string = chunk.toString();
                chunk = null;
                process.send({pid: process.pid, type: 'log', level: 'log', payload: `stringified successfully`});
                let json = `[${string.slice(0, -1).split('\n').join()}]`;
                string = null;
                process.send({
                    pid: process.pid,
                    type: 'log',
                    level: 'log',
                    payload: `formatted to json successfully`
                });
                let events = JSON.parse(json);
                json = null;
                process.send({pid: process.pid, type: 'log', level: 'log', payload: `parsed successfully`});
                let pushEvents = filterPushEvents(events);
                events = null;
                process.send({pid: process.pid, type: 'log', level: 'log', payload: `filtered successfully`});
                let repositoryCommits = [].concat(mapDistinctRepositoryCommits(pushEvents));
                pushEvents = null;
                process.send({
                    pid: process.pid,
                    type: 'log',
                    level: 'log',
                    payload: `mapped repository commits successfully`
                });
                let partReducedRepositoryCommits = reduceRepositories(repositoryCommits);
                repositoryCommits = null;
                process.send({
                    pid: process.pid,
                    type: 'log',
                    level: 'log',
                    payload: `reduced request repostiory commits successfully`
                });
                process.send({
                    pid: process.pid,
                    type: 'log',
                    level: 'log',
                    payload: `ended processing request response`
                });
                resolve(partReducedRepositoryCommits);
            });
        });
        server_req.on('error', (err) => {
            process.send({pid: process.pid, type: 'log', level: 'error', payload: `request failed`});
            reject(err);
        });
        server_req.end();
        process.send({pid: process.pid, type: 'log', level: 'log', payload: `request sent`});
    }))).then((arrayOfReducedRepositoryCommits) => {
        let reducedRepositoryCommits = reduceRepositories([].concat(...arrayOfReducedRepositoryCommits));
        arrayOfReducedRepositoryCommits = null;
        process.send({
            pid: process.pid,
            type: 'log',
            level: 'log',
            payload: `reduced process repository commits successfully`
        });
        callback(reducedRepositoryCommits);
    });
}

fetchData(paths, (data) => {
    process.send({
        pid: process.pid,
        type: 'fetchDataSuccess',
        data: data
    });
});

process.send({pid: process.pid, type: 'log', level: 'log', payload: `started fetching`});

Object.values = Object.values || ((obj) => Object.keys(obj).map(key => obj[key]));

/**
 * Groups elements by keys
 * @param array
 * @param getKey
 * @returns {{}}
 */
function groupBy(array, getKey) {
    let groups = {};
    array.forEach((elem) => {
        let key = getKey(elem);
        groups[key] && (groups[key] = groups[key].concat(elem)) || (groups[key] = [elem]);
    });
    return groups;
}

/**
 * Reduces repositories
 * @param repository_commits
 * @returns {Array}
 */
function reduceRepositories(repository_commits) {
    return Object.values(groupBy(repository_commits, (commit) => {
        return commit.repo;
    }))
        .map((group) => {
            let reduced = {
                repo: group[0].repo,
                commits: []
            };
            group.forEach(group => {
                reduced.commits = reduced.commits.concat(group.commits);
            });
            return reduced;
        });
}