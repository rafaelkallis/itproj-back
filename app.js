/**
 * Created by rafaelkallis on 29.09.16.
 */
var express = require('express');
var http = require('http');
var zlib = require('zlib');
var async = require('async');
var Pool = require('pg').Pool;
var app = express();
var schedule = require('node-schedule');

const max_n_repositories = 50;
const max_n_db_connections = 50;

// app.get('/', (req, res) => { //TODO
//     res.send('Hello World!');
// });
//
// var job = schedule.scheduleJob('0 0 * * * *', () => { //TODO
//     // run pull job
// });

// var pool = new Pool(); //TODO add config

// pool.connect(function (err, client, done) { //TODO
//     if (err) {
//         return console.error('error fetching client from pool', err);
//     }
//     client.query('CREATE TABLE IF NOT EXISTS "repository" (name VARCHAR(40) PRIMARY KEY,n_commits INT NOT NULL);', function (err, result) {
//         if (err) {
//             return console.error('error running query', err);
//         }
//     });
//     client.query('CREATE TABLE IF NOT EXISTS "repository" (name VARCHAR(40) PRIMARY KEY,n_commits INT NOT NULL);', function (err, result) {
//         if (err) {
//             return console.error('error running query', err);
//         }
//     });
//     done();
// });
var option = {
    hostname: 'data.githubarchive.org',
    path: '/2016-09-01-20.json.gz'
};

var req = http.request(option, (res) => {
    var chunks = [];
    var pipe = res.pipe(zlib.createGunzip());
    pipe.on('data', (chunk) => {
        chunks.push(chunk);
    });
    pipe.on('end', () => {
        onRequestEnd(chunks);
    });
});

req.on('error', (e) => {
    console.log(`Got error: ${e.message}`);
});

req.end();

function onRequestEnd(chunks) {
    new Promise((ful, rej) => {
        ful(Buffer.concat(chunks));
    }).then((big_chunk) => {
        return big_chunk.toString();
    }).then((stringified) => {
        return '[' + stringified.replace(/\n/g, ",").substr(0, stringified.length - 1) + ']';
    }).then((array) => {
        return JSON.parse(array);
    }).then((events) => {
        var filtered = [];
        async.filter(events, (event, callback) => {
            callback(null, event.type == 'PushEvent');
        }, (err, res) => {
            filtered = res;
        });
        return filtered;
    }).then((push_events) => {
        var repository_commits = [];
        async.map(push_events, (push_event, callback) => {
            callback(null, {
                repo: push_event.repo.name,
                n_commits: push_event.payload.size,
                commits: push_event.payload.commits
            })
        }, (err, res) => {
            repository_commits = res;
        });
        return repository_commits;
    }).then((repository_commits) => {
        return groupBy(repository_commits, (elem) => {
            return elem.repo;
        });
    }).then((commit_groups) => {
        var repositories = [];
        async.map(commit_groups, (group, callback) => {
            async.reduce(group, {               // <- 11 SIGENV, exit code 139
                repo: '',
                n_commits: 0,
                commits: []
            }, (prev, cur, callback) => {
                callback(null, {
                    repo: cur.repo,
                    n_commits: prev.n_commits + cur.n_commits,
                    commits: prev.commits.concat(cur.commits)
                });
            }, (err, sum) => {
                callback(null, sum);
            });
        }, (err, res) => {
            repositories = res;
            console.log('passed');
        });
        return repositories;
    }).then((repositories) => {
        var sorted_repositories = [];
        async.sortBy(repositories, function (repository, callback) {
            callback(null, repository.n_commits * -1);
        }, function (err, res) {
            sorted_repositories = res;
        });
        return sorted_repositories;
    }).then((sorted_repositories) => {
        return sorted_repositories.slice(0, max_n_repositories);
    }).then((top_50_repositories) => {
        //persist repositories
        // async.mapLimit(top_50_repositories, max_n_db_connections, (repository, callback) => {
        //     pool.connect(function (err, client, done) {
        //         if (err) {
        //             return console.error('error fetching client from pool', err);
        //         }
        //         client.query('INSERT INTO "repository" VALUES ($1,$2)', [repository.repo, repository.n_commits], function (err, result) {
        //             done();
        //             callback();
        //             if (err) {
        //                 return console.error('error running query', err);
        //             }
        //         });
        //     });
        // });
        return top_50_repositories;
    });
}

function groupBy(array, getKey) {
    var groups = {};
    async.map(array, (elem, callback) => {
        var key = getKey(elem);
        groups[key] && (groups[key] = groups[key].concat(elem)) || (groups[key] = [elem]);
        callback();
    });
    return groups;
}

app.listen(8080);