/**
 * Created by rafaelkallis on 29.09.16.
 */
var express = require('express');
var http = require('http');
var zlib = require('zlib');
var Pool = require('pg').Pool;
var app = express();
var schedule = require('node-schedule');

const max_n_repositories = 50;

Object.values = Object.values || ((obj) => Object.keys(obj).map(key => obj[key]));


app.get('/', (client_req, client_res) => {
  
});

var server_req = http.request({
    hostname: 'data.githubarchive.org',
    path: '/2016-09-01-20.json.gz'
}, (res) => {
    var chunks = [];
    var pipe = res.pipe(zlib.createGunzip());
    pipe.on('data', (chunk) => {
        chunks.push(chunk);
    });
    pipe.on('end', () => {
        onRequestEnd(chunks);
    });
});

server_req.on('error', (e) => {
    console.log(`Got error: ${e.message}`);
});

server_req.end();

//
// var job = schedule.scheduleJob('0 0 * * * *', () => { //TODO
//     // run pull job
// });

var pool = new Pool({
    user: process.env.POSTGRES_USERNAME,
    database: process.env.POSTGRES_DATABASE,
    password: process.env.POSTGRES_PASSWORD,
    host: process.env.POSTGRES_HOST,
    post: process.env.POSTGRES_PORT,
    max: 10,
    idleTimeoutMillis: 30000
});

// pool.connect(function (err, client, done) {
//     !err && client.query(`
//     CREATE TABLE IF NOT EXISTS "repository" (
//         name VARCHAR(64) PRIMARY KEY,
//         n_commits INT NOT NULL
//     );
//     CREATE TABLE IF NOT EXISTS "user" (
//         hashed_email CHAR(40) PRIMARY KEY,
//         any_commit_sha CHAR(40) NOT NULL
//     );
//     CREATE TABLE IF NOT EXISTS "commits" (
//         repository_name VARCHAR(64) PRIMARY KEY,
//         user_hashed_email CHAR(40) NOT NULL, n_commits INT NOT NULL,
//         CONSTRAINT commits_repository_name_fk FOREIGN KEY (repository_name) REFERENCES repository (name),
//         CONSTRAINT commits_user_hashed_email_fk FOREIGN KEY (user_hashed_email) REFERENCES "user" (hashed_email)
//     );`, (err, result) => {
//         err && console.error('error creating tables', err);
//         done();
//     }) || console.error('error getting client from pool', err);
// });

function onRequestEnd(chunks) {
    console.log("Computation Started");
    let stringified = stringifyChunks(chunks);
    let validJSON = genValidJSON(stringified);
    let events = JSON.parse(validJSON);
    let push_events = filterPushEvents(events);
    let repository_commits = getRepositoryCommits(push_events);

    let repositories = reduceRepositories(repository_commits);
    let rep_sort_by_n_commits_desc = repositories.sort((repo1, repo2) => {
        return repo2.commits.length - repo1.commits.length;
    });
    let top_reps = rep_sort_by_n_commits_desc.slice(0, max_n_repositories);

    let flatCommits = flatMapCommits(repository_commits);
    let author_commits = reduceCommits(flatCommits);

    let repository_query_args = genInsertStatements(top_reps.map((rep) => [rep.repo, rep.commits.length]), 'repository');
    let user_query_args = genInsertStatements(author_commits.map((author_commit) => [author_commit.user_email_sha, author_commit.any_commit_sha]), 'user');
    let commits_query_args = genInsertStatements(author_commits.map((author_commit) => [author_commit.repo, author_commit.user_email_sha, author_commit.n_commits]), 'commits');

    let begin_query_args = {
        queries: ['BEGIN'],
        args: []
    };
    let rollback_query_args = (err) => {
        return {
            queries: [err ? 'ROLLBACK' : 'COMMIT'],
            args: []
        }
    };
    pool.connect(function (transaction_err, client, done) {
        stackQueries(client, begin_query_args, transaction_err, (err) =>
            stackQueries(client, repository_query_args, err, (err) =>
                stackQueries(client, user_query_args, err, (err) =>
                    stackQueries(client, commits_query_args, err, (err) =>
                        stackQueries(client, rollback_query_args(err), err, (err) => {
                            if(err){
                                done(err);
                                console.error('Error on transaction');
                            } else {
                                done();
                                console.log('Transaction successful');
                            }
                        })
                    )
                )
            )
        );
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

function getShaPartFromEmail(email) {
    return email.substr(0, 40);
}

function flatMapCommits(repository_commits) {
    let flatMappedCommits = [];
    repository_commits.forEach((repository_commit) => {
        repository_commit.commits.forEach((commit) => {
            commit.distinct && flatMappedCommits.push({
                repo: repository_commit.repo,
                sha: commit.sha,
                author_email_sha: getShaPartFromEmail(commit.author.email)
            });
        });
    });
    return flatMappedCommits;
}

function reduceCommits(repository_commits) {
    return Object.values(groupBy(repository_commits, (commit) => {
        return commit.repo.concat(commit.author_email_sha);
    }))
        .map((all_commits_one_user_one_repo) => {
            return {
                repo: all_commits_one_user_one_repo[0].repo,
                user_email_sha: all_commits_one_user_one_repo[0].author_email_sha,
                any_commit_sha: all_commits_one_user_one_repo[0].sha,
                n_commits: all_commits_one_user_one_repo.length
            };
        });
}

const chunk_size = 5000;

function genInsertStatements(tuples, table_name) {
    let queries = [];
    let args = [];
    let chunk_index = 0;
    let chunk_elem_index = 0;
    let placeholder_index = 0;
    while (tuples.length) {
        if (!chunk_elem_index) {
            queries[chunk_index] = [];
            args[chunk_index] = [];
        }
        let tuple = tuples.pop();
        let gen_placeholder_index = genPlaceholder(tuple.length, placeholder_index);
        let placeholder = gen_placeholder_index.placeholder;
        placeholder_index = gen_placeholder_index.index;
        queries[chunk_index].push(placeholder);
        while (tuple.length) {
            args[chunk_index].push(tuple.shift());
        }
        if (++chunk_elem_index == chunk_size) {
            chunk_elem_index = 0;
            ++chunk_index;
            placeholder_index = 0;
        }
    }
    queries = queries.map((query) => `INSERT INTO "${table_name}" VALUES ` + query.join() + ` ON CONFLICT DO NOTHING`);

    return {
        queries: queries,
        args: args
    };
}

function genPlaceholder(size, index) {
    let placeholder = [];
    while (size--) {
        placeholder.push(`$${++index}`);
    }
    return {
        placeholder: '(' + placeholder.join() + ')',
        index: index
    };
}

function stackQueries(client, queries_args, transaction_err, callback) {
    if (!transaction_err && queries_args.queries.length) {
        let query = queries_args.queries.pop();
        let args = queries_args.args.pop();
        console.log(query);
        client.query(query, args, (err) => stackQueries(client, queries_args, transaction_err |= err, callback));
    } else {
        callback(transaction_err);
    }
}

app.listen(8080);