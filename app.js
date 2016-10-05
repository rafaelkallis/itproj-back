/**
 * Created by rafaelkallis on 29.09.16.
 */

require('log-timestamp');
var http = require('http');
var zlib = require('zlib');
var schedule = require('node-schedule');
var async = require('async');
var Pool = require('pg').Pool;
var JSONStream = require('JSONStream');
var QueryStream = require('pg-query-stream');

/**
 * Number of days the fetched data stays in the database
 * @type {any}
 */
const max_days_of_data = process.env.MAX_DAYS || 7;

/**
 * Number of repositories to consider in computation starting with the one with most commits
 * @type {any}
 */
const max_top_repositories = process.env.MAX_TOP_REPOSITORIES || 200;

/**
 * Port to expose
 * @type {any}
 */
const port = process.env.PORT || 8080;

/**
 * GitHub archive's hostname
 * @type {string}
 */
const hostname = `data.githubarchive.org`;

/**
 * Number of elements included in each insert query into the database
 * @type {number}
 */
const max_elements_per_insert_query = 5000;

/**
 * Endpoints for data
 * @type {string}
 */
const repositories_endpoint = "/repositories";
const users_endpoint = "/users";
const commits_endpoint = "/rels";

/**
 * Database table names
 * @type {string}
 */
const repositories_table = "repositories";
const users_table = "users";
const commits_table = "commits";
const hourly_commits_table = "hourly_commits";

/**
 * Database connection pool
 */
const pool = new Pool({
    user: process.env.POSTGRES_USERNAME || 'postgres',
    database: process.env.POSTGRES_DATABASE || 'postgres',
    password: process.env.POSTGRES_PASSWORD || 'postgres',
    host: process.env.POSTGRES_HOST || 'localhost',
    post: process.env.POSTGRES_PORT || 5432,
    max: process.env.POSTGRES_MAX_CONNECTIONS || 5,
    idleTimeoutMillis: 30000
});

Object.values = Object.values || ((obj) => Object.keys(obj).map(key => obj[key]));

/**
 * Makes sure the tables exist in the database
 */
pool.connect((err, client, done) =>
    err ? console.error('error connecting to database', err) && process.exit(1) : client.query(`
        CREATE TABLE IF NOT EXISTS "${repositories_table}"(
            name VARCHAR(128) PRIMARY KEY NOT NULL,
            n_commits INTEGER NOT NULL
        ); 
        CREATE TABLE IF NOT EXISTS "${users_table}"(
            hashed_email CHAR(40) PRIMARY KEY NOT NULL,
            any_commit_url VARCHAR(200) NOT NULL,
            n_commits INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS "${commits_table}"(
            repository_name VARCHAR(128) NOT NULL,
            user_hashed_email CHAR(40) NOT NULL,
            any_commit_url VARCHAR(200) NOT NULL,
            n_commits INTEGER NOT NULL,
            CONSTRAINT commits_pk PRIMARY KEY (repository_name, user_hashed_email)
        );
        CREATE TABLE IF NOT EXISTS "${hourly_commits_table}"(
            repository_name VARCHAR(128) NOT NULL,
            user_hashed_email CHAR(40) NOT NULL,
            any_commit_url VARCHAR(200) NOT NULL,
            n_commits INTEGER,
            timestamp TIMESTAMP DEFAULT now(),
            CONSTRAINT hourly_commits_repository_name_user_hashed_email_pk PRIMARY KEY (repository_name, user_hashed_email)
        );`, (err) => done() && err ? console.error('error creating tables', err) : console.log('tables created')
    )
);

/**
 * Runs the updateJob every hour, on 30 minutes. e.g 11:30, 12:30, etc.
 */
schedule.scheduleJob('0 30 * * * *', () => {
    console.log(`starting hourly update job`);
});
updateJob();

/**
 * Launches the server
 */
http.createServer((request, response) => {
    switch (request.url) {
        case repositories_endpoint:
            get(repositories_table, response);
            break;
        case users_endpoint:
            get(users_table, response);
            break;
        case commits_endpoint:
            get(commits_table, response);
            break;
        default:
            response.writeHead(404);
            response.end();
    }
}).listen(port);

console.log(`listening on port ${port}`);

/**
 * Fetches data from the GitHub archive
 */
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
                        url: `https://api.github.com/repos/${event.repo.name}/commits/${commit.sha}`,
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

/**
 * Fetches the requested data from database and pipes it straight into the response
 * @param table
 * @param response
 */
function get(table, response) {
    pool.connect((err, client, done) => {
        response.setHeader('Access-Control-Allow-Origin', '*');
        response.setHeader('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
        if (err) {
            response.statusCode = 500;
            return response.end(err);
        }
        response.setHeader('Content-Type', 'application/json');
        client.query(new QueryStream(`SELECT * FROM ${table}`)).pipe(JSONStream.stringify()).pipe(response).on('finish', done);
    });
}

/**
 * Gets executed after successful fetching of GitHub archive data
 * Connects to database and adds newest hourly commits.
 * Recomputes the top x repositories by number of commits
 * Recomputes the authors of commits
 * Recomputes the commits made by the authors to the repositories
 * @param hourlyCommits
 */
function onRequestEnd(hourlyCommits) {
    pool.connect((err, client, done) => {
        let rollback = (err) => {
            client.query('ROLLBACK', (tx_err) => done);
            console.error(err);
        };
        err ? console.error(err) || done() : console.log(`beginning transaction`) || client.query('BEGIN', (tx_err) =>
            (err = tx_err) ? rollback(err) : console.log(`deleting outdated hourly commits`) & client.query(`DELETE FROM "${hourly_commits_table}" WHERE timestamp < NOW() - INTERVAL '${max_days_of_data} days'`, (tx_err) =>
                (err = tx_err) ? rollback(err) : console.log(`inserting new hourly commits`) & async.series(generateClientInsertFunction(client, reduceCommits(hourlyCommits)), (tx_err) =>
                    (err = tx_err) ? rollback(err) : console.log(`deleting outdated records`) & client.query(`DELETE FROM "${repositories_table}"; DELETE FROM "${users_table}"; DELETE FROM "${commits_table}";`, (tx_err) =>
                        (err = tx_err) ? rollback(err) : console.log(`inserting new records into "${repositories_table}"`) & client.query(`  
                            INSERT INTO "${repositories_table}" (
                                SELECT
                                    repository_name, 
                                    SUM(n_commits) AS n_commits
                                FROM "${hourly_commits_table}"
                                GROUP BY repository_name
                                ORDER BY n_commits DESC
                                LIMIT ${max_top_repositories}
                            );`, (tx_err) =>
                            (err = tx_err) ? rollback(err) : console.log(`inserting new records into "${commits_table}"`) & client.query(`  
                                INSERT INTO "${commits_table}" (
                                    SELECT 
                                        repository_name,
                                        user_hashed_email,
                                        MIN(any_commit_url) as any_commit_url,
                                        SUM(hourly_commits.n_commits) AS n_commits
                                    FROM "${hourly_commits_table}" 
                                    JOIN (
                                        SELECT 
                                            name 
                                        FROM "${repositories_table}") AS top_repository 
                                        ON ${hourly_commits_table}.repository_name = top_repository.name
                                    GROUP BY repository_name,user_hashed_email
                                    ORDER BY n_commits DESC
                                )`, (tx_err) =>
                                (err = tx_err) ? rollback(err) : console.log(`inserting new records into "${users_table}"`) & client.query(` 
                                    INSERT INTO "${users_table}" (
                                        SELECT 
                                            user_hashed_email,
                                            MIN(any_commit_url) AS any_commit_url,
                                            SUM(n_commits) AS n_commits
                                        FROM "${commits_table}" 
                                        GROUP BY user_hashed_email
                                        ORDER BY n_commits DESC
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
    return Object.values(groupBy(repositoryCommits, (commit) => commit.repository_name + commit.user_hashed_email))
        .map((group) => {
            let reduced = {
                repository_name: group[0].repository_name,
                user_hashed_email: group[0].user_hashed_email,
                any_commit_url: group[0].url,
                n_commits: 0
            };
            group.forEach((commit) => reduced.n_commits += commit.n_commits);
            return reduced;
        });
}

/**
 * Generates an array of functions which execute the insert statement
 * @param client
 * @param commits
 * @returns {Array|*}
 */
function generateClientInsertFunction(client, commits) {
    return generateInsertStatements(commits.map((commit) => [commit.repository_name, commit.user_hashed_email, commit.any_commit_url, commit.n_commits]), 'hourly_commits')
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
        let gen_placeholder_index = generatePlaceholders(tuple.length, placeholder_index);
        let placeholder = gen_placeholder_index.placeholder;
        placeholder_index = gen_placeholder_index.index;
        query.push(placeholder);
        while (tuple.length) {
            args.push(tuple.shift());
        }
        if (++chunk_elem_index == max_elements_per_insert_query || !tuples.length) {
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
 * Generates placeholders for insert statements
 * @param size
 * @param index
 * @returns {{placeholder: string, index: *}}
 */
function generatePlaceholders(size, index) {
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