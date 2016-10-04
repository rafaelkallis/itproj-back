/**
 * Created by rafaelkallis on 03.10.16.
 */
let http = require('http');
let zlib = require('zlib');
let async = require('async');
let Client = require('pg').Client;
let Transaction = require('pg-transaction');
let JSONStream = require('JSONStream');
const db_chunk_size = 1000;
const hostname = `data.githubarchive.org`;
const persist_days = process.env.PERSIST_DAYS;
const pgConfig = {
    user: process.env.POSTGRES_USERNAME || 'postgres',
    database: process.env.POSTGRES_DATABASE || 'postgres',
    password: process.env.POSTGRES_PASSWORD || 'postgres',
    host: process.env.POSTGRES_HOST || 'localhost',
    post: process.env.POSTGRES_PORT || 5432,
    idleTimeoutMillis: process.env.POSTGRES_IDLE_TIMEOUT || 30000
};
Object.values = Object.values || ((obj) => Object.keys(obj).map(key => obj[key]));

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
    sendFailMessage(err);
});

server_req.end();
sendLogMessage(`request sent`);

/**
 * Gets executed after successful execution
 * @param hourlyCommits
 */
function onRequestEnd(hourlyCommits) {
    sendLogMessage(`mapped requests successfully`);
    hourlyCommits = reduceCommits(hourlyCommits);
    sendLogMessage(`reduced commits successfully`);
    let client = new Client(pgConfig);
    client.connect((err) => {
        if (err) {
            sendFailMessage(err);
            return;
        }
        let tx = new Transaction(client);
        let hourlyCommitQueries = generateHourlyCommitQueries(tx, hourlyCommits);
        sendLogMessage(`generated commit insert statements successfully`);
        hourlyCommits = null;
        tx.begin((tx_err) => err = tx_err);
        !err && sendLogMessage(`transaction begun`);
        !err && async.series(hourlyCommitQueries, (tx_err) => {
            !(err = tx_err) && tx.query(`DELETE FROM "repository"; DELETE FROM "user"; DELETE FROM "commits";`, (tx_err) => err = tx_err);
            !err && persist_days && tx.query(`DELETE FROM "hourly_commits" WHERE timestamp < NOW() - INTERVAL '$1 days';`, [persist_days], (tx_err) => err = tx_err);
            !err && sendLogMessage(`deleted outdated data`);
            !err && tx.query(`INSERT INTO "commits" (SELECT repository_name,user_hashed_email,SUM(n_commits) FROM "hourly_commits" GROUP BY repository_name,user_hashed_email);`, (tx_err) => err = tx_err);
            !err && sendLogMessage(`updated commits`);
            !err && tx.query(`INSERT INTO "repository" (SELECT repository_name,SUM(n_commits) FROM "commits" GROUP BY repository_name);`, (tx_err) => err = tx_err);
            !err && sendLogMessage(`updated repository`);
            !err && tx.query(`INSERT INTO "user" (SELECT user_hashed_email,SUM(n_commits) FROM "commits" GROUP BY user_hashed_email);`, (tx_err) => err = tx_err);
            !err && sendLogMessage(`updated user`);
            if (err) {
                tx.rollback(() => {
                    client.end();
                    sendFailMessage(err);
                });
            } else {
                tx.commit((err) => {
                    client.end();
                    err && sendFailMessage(err);
                    !err && sendSuccessMessage();
                });
            }
        });
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
 * @param tx
 * @param commits
 * @returns {Array|*}
 */
function generateHourlyCommitQueries(tx, commits) {
    return generateInsertStatements(commits.map((commit) => [commit.repository_name, commit.user_hashed_email, commit.n_commits]), 'hourly_commits')
        .map((query_args) => (callback) => {
            tx.query(query_args.query, query_args.args, (err) => callback(err));
        });
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

/**
 * Sends a log message to the master
 * @param message
 */
function sendLogMessage(message) {
    process.send && process.send({
        pid: process.pid,
        type: 'log',
        level: 'log',
        payload: message
    }) || console.log(message);
}

/**
 * Sends a success message to the master
 */
function sendSuccessMessage() {
    process.send && process.send({
        pid: process.pid,
        type: 'success'
    }) || console.log(`successful`);
}

/**
 * Sends a fail message to the master
 * @param err
 */
function sendFailMessage(err) {
    process.send && process.send({
        pid: process.pid,
        type: 'fail',
        payload: err
    }) || console.error(err);
}