/**
 * Created by rafaelkallis on 29.09.16.
 */
require('log-timestamp');
let http = require('http');
let zlib = require('zlib');
var schedule = require('node-schedule');
var async = require('async');
var Pool = require('pg').Pool;
var JSONStream = require('JSONStream');
const max_days_of_data = process.env.MAX_DAYS || 7;
const max_top_repositories = process.env.MAX_TOP_REPOSITORIES || 500;
const port = process.env.PORT || 8080;
const hostname = `data.githubarchive.org`;
const db_chunk_size = 10000;
const db_pool_size = 10;
const pgConfig = {
    user: process.env.POSTGRES_USERNAME || 'postgres',
    database: process.env.POSTGRES_DATABASE || 'postgres',
    password: process.env.POSTGRES_PASSWORD || 'postgres',
    host: process.env.POSTGRES_HOST || 'localhost',
    post: process.env.POSTGRES_PORT || 5432,
    max: db_pool_size,
    idleTimeoutMillis: 30000
};

Object.values = Object.values || ((obj) => Object.keys(obj).map(key => obj[key]));

let pool = new Pool(pgConfig);

pool.connect((err, client, done) => {
    err && console.error('error connecting client', err);
    !err && client.query(`
    CREATE TABLE IF NOT EXISTS "repositories"(
        name VARCHAR(128) PRIMARY KEY NOT NULL,
        n_commits INTEGER NOT NULL
    ); 
    CREATE TABLE IF NOT EXISTS "users"(
        hashed_email CHAR(40) PRIMARY KEY NOT NULL,
        any_commit_sha CHAR(40) NOT NULL
    );
    CREATE TABLE IF NOT EXISTS "commits"(
        repository_name VARCHAR(128) NOT NULL,
        user_hashed_email CHAR(40) NOT NULL,
        n_commits INTEGER NOT NULL,
        CONSTRAINT commits_pkey PRIMARY KEY (repository_name, user_hashed_email)
    );
    CREATE TABLE IF NOT EXISTS "hourly_commits"(
        repository_name VARCHAR(128) NOT NULL,
        user_hashed_email CHAR(40) NOT NULL,
        n_commits INTEGER,
        timestamp TIMESTAMP DEFAULT now(),
        CONSTRAINT hourly_commits_repository_name_user_hashed_email_pk PRIMARY KEY (repository_name, user_hashed_email)
    );`, (err) => {
        done();
        err && console.error('error creating tables', err);
    });

});


schedule.scheduleJob('0 30 * * * *', () => {
    console.log(`starting hourly update job`);
    updateJob();
});

function updateJob() {
    let server_req = http.request({
        hostname: hostname,
        path: generatePath()
    }, (response) => {
        let hourCommits = [];
        let stream = response.pipe(zlib.createGunzip()).pipe(JSONStream.parse());
        stream.on('data', (event) => {
            event.type == "PushEvent" && hourCommits.push(...event.payload.commits
                .filter((commit) => commit.distinct)
                .map((commit) => ({
                        repository_name: event.repo.name,
                        user_hashed_email: commit.author.email.substr(0, 40),
                        n_commits: 1
                    })
                )
            );
        });
        stream.on('end', () => {
            onRequestEnd(hourCommits);
        });
    });
    server_req.on('error', (err) => {
        console.error(err);
    });
    server_req.end();
    console.log(`request sent`);
}

let queryDb = (table, callback) => {
    pool.connect((err, client, done) => {
        if (err) {
            callback(err);
            return;
        }
        client.query(`SELECT * FROM "${table}"`, (queryError, result) => {
            done();
            if (queryError) {
                callback(queryError, null);
                return;
            }
            callback(null, result);
        });
    });
};
let respond = (response, queryError, queryResult) => {
    if (queryError) {
        response.writeHead(500);
        response.end();
    } else {
        response.writeHead(200, {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': "Origin, X-Requested-With, Content-Type, Accept"
        });
        response.end(JSON.stringify(queryResult.rows));
    }
};
let server = http.createServer((request, response) => {
    switch (request.url) {
        case '/repositories':
            queryDb('repositories', (err, result) => respond(response, err, result));
            break;
        case '/users':
            queryDb('users', (err, result) => respond(response, err, result));
            break;
        case '/rels':
            queryDb('commits', (err, result) => respond(response, err, result));
            break;
        default:
            response.writeHead(404);
            response.end();
    }
});
console.log(`listening on port ${port}`);
server.listen(port);


/**
 * Gets executed after successful execution
 * @param hourlyCommits
 */
function onRequestEnd(hourlyCommits) {
    pool.connect((err, client, done) => {
        let rollback = (err) => {
            client.query('ROLLBACK', (tx_err) => done);
            console.error(err);
        };
        err ? console.error(err) || done() : console.log(`beginning transaction`) || client.query('BEGIN', (tx_err) =>
            (err = tx_err) ? rollback(err) : console.log(`deleting outdated hourly commits`) & client.query(`DELETE FROM "hourly_commits" WHERE timestamp < NOW() - INTERVAL '${max_days_of_data} days'`, (tx_err) =>
                (err = tx_err) ? rollback(err) : console.log(`inserting new hourly commits`) & async.series(generateHourlyCommitQueries(client, reduceCommits(hourlyCommits)), (tx_err) =>
                    (err = tx_err) ? rollback(err) : console.log(`deleting outdated records`) & client.query(`DELETE FROM "repositories"; DELETE FROM "users"; DELETE FROM "commits";`, (tx_err) =>
                        (err = tx_err) ? rollback(err) : console.log(`inserting new records into "repository"`) & client.query(`  
                            INSERT INTO "repositories" (
                                SELECT
                                    repository_name,
                                    SUM(n_commits) AS n_commits
                                FROM "hourly_commits"
                                GROUP BY repository_name
                                ORDER BY n_commits DESC
                                LIMIT ${max_top_repositories}
                            );`, (tx_err) =>
                            (err = tx_err) ? rollback(err) : console.log(`inserting new records into "commits"`) & client.query(`  
                                INSERT INTO "commits" (
                                    SELECT 
                                        repository_name,
                                        user_hashed_email,
                                        SUM(hourly_commits.n_commits) 
                                    FROM "hourly_commits" 
                                    JOIN (
                                        SELECT 
                                            name 
                                        FROM "repositories") AS top_repository 
                                        ON hourly_commits.repository_name = top_repository.name
                                    GROUP BY repository_name,user_hashed_email
                                )`, (tx_err) =>
                                (err = tx_err) ? rollback(err) : console.log(`inserting new records into "user"`) & client.query(` 
                                    INSERT INTO "users" (
                                        SELECT 
                                            user_hashed_email,
                                            SUM(n_commits)
                                        FROM "commits" 
                                        GROUP BY user_hashed_email
                                    )`, (tx_err) =>
                                    (err = tx_err) ? rollback(err) : console.log('committing transaction') & client.query('COMMIT', (tx_err) =>
                                        (err = tx_err) ? console.error(`transaction commit failed`) & done(err) : console.log(`transaction commit successful`) & done()
                                    )
                                )
                            )
                        )
                    )
                )
            )
        );
    });
}

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
 * @param repositoryCommits
 * @returns [{repository_name: <>, user_email_sha: <>, n_commits: <>}, ...]
 */
function reduceCommits(repositoryCommits) {
    return Object.values(groupBy(repositoryCommits, (commit) => commit.repository_name + commit.user_email_sha))
        .map((group) => {
            let reduced = {
                repository_name: group[0].repository_name,
                user_hashed_email: group[0].user_hashed_email,
                n_commits: 0
            };
            group.forEach((commit) => reduced.n_commits += commit.n_commits);
            return reduced;
        });
}

/**
 * Generates the SQL insert statement execution functions
 * @param client
 * @param commits
 * @returns {Array|*}
 */
function generateHourlyCommitQueries(client, commits) {
    return generateInsertStatements(commits.map((commit) => [commit.repository_name, commit.user_hashed_email, commit.n_commits]), 'hourly_commits')
        .map((query_args) => (callback) => client.query(query_args.query, query_args.args, (err) => err ? callback(err) : callback()))
}

/**
 * Generates the SQL insert statements
 * @param tuples
 * @param table_name
 * @returns {Array}
 */
function generateInsertStatements(tuples, table_name) {
    let query_args_array = [];

    let query = [];
    let args = [];
    let chunk_index = 0;
    let chunk_elem_index = 0;
    let placeholder_index = 0;
    while (tuples.length) {
        let tuple = tuples.pop();
        let gen_placeholder_index = generatePreparedStatementPlaceholder(tuple.length, placeholder_index);
        let placeholder = gen_placeholder_index.placeholder;
        placeholder_index = gen_placeholder_index.index;
        query.push(placeholder);
        while (tuple.length) {
            args.push(tuple.shift());
        }
        if (++chunk_elem_index == db_chunk_size || !tuples.length) {
            query_args_array.push({
                query: `INSERT INTO "${table_name}" VALUES ${query.join()} ON CONFLICT DO NOTHING`,
                args: args
            });
            query = [];
            args = [];
            chunk_elem_index = 0;
            ++chunk_index;
            placeholder_index = 0;
        }
    }
    return query_args_array;
}

/**
 * Generates placeholders for prepared statements
 * @param size
 * @param index
 * @returns {{placeholder: string, index: *}}
 */
function generatePreparedStatementPlaceholder(size, index) {
    let placeholder = [];
    while (size--) {
        placeholder.push(`$${++index}`);
    }
    return {
        placeholder: `(${placeholder.join()})`,
        index: index
    };
}

/**
 * Generates the path that gets queried
 * @returns {string}
 */
function generatePath() {
    let addLeadingZero = (num) => num < 10 ? '0' + num.toString() : num.toString();
    let date = new Date();
    date.setDate(date.getDate() - 1);
    return `/${date.getFullYear()}-${addLeadingZero(date.getMonth() + 1)}-${addLeadingZero(date.getDate())}-${date.getHours()}.json.gz`;
}