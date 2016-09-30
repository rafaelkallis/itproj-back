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

Object.values = Object.values || ((obj) => Object.keys(obj).map(key => obj[key]));

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
    let stringified = stringifyChunks(chunks);
    let validJSON = genValidJSON(stringified);
    let events = JSON.parse(validJSON);
    let push_events = filterPushEvents(events);
    let repository_commits = getRepositoryCommits(push_events);
    let repositories = reduceCommits(repository_commits);
    let rep_sort_by_n_commits_desc = repositories.sort((repo1, repo2) => {
        return repo2.commits.length - repo1.commits.length;
    });
    let top_reps = rep_sort_by_n_commits_desc.slice(0, max_n_repositories);
    top_rep_by_n_commits_desc.forEach((repository) => {
        //TODO persist
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
    });
}

function stringifyChunks(chunks) {
    return Buffer.concat(chunks).toString();
}

function genValidJSON(invalidJSON) {
    return '[' + invalidJSON.replace(/\n/g, ",").substr(0, invalidJSON.length - 1) + ']';
}

function filterPushEvents(events) {
    return events.filter((event) => {
        return event.type == 'PushEvent';
    });
}

function getRepositoryCommits(push_events) {
    return push_events.map((push_event) => {
        return {
            repo: push_event.repo.name,
            commits: push_event.payload.commits
        };
    });
}

function groupBy(array, getKey) {
    let groups = {};
    array.forEach((elem) => {
        let key = getKey(elem);
        groups[key] && (groups[key] = groups[key].concat(elem)) || (groups[key] = [elem]);
    });
    return groups;
}

function reduceCommits(repository_commits) {
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

app.listen(8080);