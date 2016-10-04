/**
 * Created by rafaelkallis on 29.09.16.
 */
var bluebird = require('bluebird');
var cluster = require('cluster');
var child_process = require('child_process');

/**
 * Master
 */
if (cluster.isMaster) {
    require('log-timestamp');
    let Transaction = require('pg-transaction');
    let schedule = require('node-schedule');
    const max_days_of_data = 1;
    const max_n_repositories = 50;
    const db_chunk_size = 10000;
    const db_pool_size = 20;
    const db_timeout = 30000;
    const pgConfig = {
        user: process.env.POSTGRES_USERNAME || 'postgres',
        database: process.env.POSTGRES_DATABASE || 'postgres',
        password: process.env.POSTGRES_PASSWORD || 'postgres',
        host: process.env.POSTGRES_HOST || 'localhost',
        post: process.env.POSTGRES_PORT || 5432,
        idleTimeoutMillis: db_timeout
    };
    let updateActive = false;

    let log = (pid, level, payload) => {
        let text = `${pid}: ${payload}`;
        switch (level) {
            case 'log':
                console.log(text);
                break;
            case 'error':
                console.error(text);
        }
    };

    let generateInsertStatements = (tuples, table_name) => {
        let generatePreparedStatementPlaceholder = (size, index) => {
            let placeholder = [];
            while (size--) {
                placeholder.push(`$${++index}`);
            }
            return {
                placeholder: `(${placeholder.join()})`,
                index: index
            };
        };
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
            let gen_placeholder_index = generatePreparedStatementPlaceholder(tuple.length, placeholder_index);
            let placeholder = gen_placeholder_index.placeholder;
            placeholder_index = gen_placeholder_index.index;
            queries[chunk_index].push(placeholder);
            while (tuple.length) {
                args[chunk_index].push(tuple.shift());
            }
            if (++chunk_elem_index == db_chunk_size) {
                chunk_elem_index = 0;
                ++chunk_index;
                placeholder_index = 0;
            }
        }
        queries = queries.map((query) => `INSERT INTO "${table_name}" VALUES ${query.join()} ON CONFLICT DO NOTHING`);

        return {
            queries: queries,
            args: args
        };
    };

    /**
     * Updates the data in the database.
     * Spawns workers which fetch events from githubarchive.
     * After a successful fetch it merges all data.
     * After merging the data, more computation gets executed, reducing, sorting and mapping the data.
     * The processed data then gets inserted into the db.
     */
    let updateData = (days) => {
        if (updateActive) return;
        updateActive = true;

        /**
         * LinearBarrier ensures that the execution continues when all workers have finished computation
         * @param nWorkers
         * @param cb
         * @constructor
         */
        let LinearBarrier = (nWorkers, cb) => {
            let mergedData = [];
            let workersRemaining = nWorkers;
            let callback = cb;
            this.countDown = (data) => {
                mergedData.push(data);
                !--workersRemaining && callback(...mergedData);
            };
        };

        /**
         *
         * @param repositoryCommits
         */
        let processRepositoryCommits = (repositoryCommits) => {
            console.log(`All requests reduced successfully`);
            let repositories = reduceRepositories([].concat(...repositoryCommits));
            console.log(`Reduced repositories successfully`);
            repositoryCommits = null;
            let repositoriesSortByCommitsDesc = repositories.sort((repo1, repo2) => {
                return repo2.commits.length - repo1.commits.length;
            });
            repositories = null;
            console.log(`Sorted repositories successfully`);
            let topRepositories = repositoriesSortByCommitsDesc.slice(0, max_n_repositories);
            repositoriesSortByCommitsDesc = null;

            let flatCommits = [].concat(...topRepositories.map((topRepository) => topRepository.commits));
            console.log(`Flatmapped repositories successfully`);
            let authorCommits = reduceCommits(flatCommits);
            flatCommits = null;
            console.log(`Reduced commits successfully`);

            let repositoryQueriesArgs = generateInsertStatements(topRepositories.map((rep) => [rep.repo, rep.commits.length]), 'repository');
            console.log(`Repository statements generated successfully`);
            let userQueriesArgs = generateInsertStatements(authorCommits.map((author_commit) => [author_commit.userEmailSha, author_commit.anyCommitSha]), 'user');
            console.log(`User statements generated successfully`);
            let commitsQueriesArgs = generateInsertStatements(authorCommits.map((author_commit) => [author_commit.repo, author_commit.userEmailSha, author_commit.nCommits]), 'commits');
            console.log(`Commit statements generated successfully`);

            let Client = require('pg').Client;
            let client = new Client(pgConfig);
            client.connect((err) => {
                let tx = new Transaction(client);
                tx.on('error', (err) => console.error(err));
                tx.begin();
                !err && tx.query('DELETE FROM "commits"; DELETE FROM "user"; DELETE FROM "repository";', (query_err) => err |= query_err);
                !err && repositoryQueriesArgs.queries.forEach((query, index) => tx.query(query, repositoryQueriesArgs.args[index], (query_err) => err |= query_err));
                !err && userQueriesArgs.queries.forEach((query, index) => tx.query(query, userQueriesArgs.args[index]));
                !err && commitsQueriesArgs.queries.forEach((query, index) => tx.query(query, commitsQueriesArgs.args[index]));
                if (err) {
                    tx.abort(() => {
                        console.error("Transaction Aborted");
                        client.end();
                    });
                } else {
                    tx.commit(() => {
                        console.log('Transaction Successful');
                        client.end();
                    });

                }
            });
        };

        /**
         * Adds leading zeroes for valid path
         * @param num
         */
        let addLeadingZero = (num) => num < 10 ? '0' + num.toString() : num.toString();

        /**
         * Generates Paths for fetching
         * @returns {Array}
         */
        let generatePaths = (days) => {
            let paths = [];
            let now = new Date(), yesterday = new Date(), date = new Date();
            yesterday.setDate(now.getDate() - 1);
            yesterday.setSeconds(0);
            yesterday.setMilliseconds(0);
            date.setDate(yesterday.getDate() - days);
            date.setSeconds(0);
            date.setMilliseconds(0);

            while (date.getTime() != yesterday.getTime()) {
                paths.push(`/${date.getFullYear()}-${addLeadingZero(date.getMonth() + 1)}-${addLeadingZero(date.getDate())}-${date.getHours()}.json.gz`);
                date.setHours(date.getHours() + 1);
            }
            return paths;
        };

        /**
         * Used to generate an even distribution of parameters
         * @param array
         * @param nStacks
         * @returns {Array}
         */
        let shuffle = (array, nStacks) => {
            let stacks = [];
            let index = nStacks;
            while (--index) {
                stacks[index] = [];
            }
            while (array.length) {
                !(index + 1) && (index = nStacks - 1);
                stacks[index] = array.pop();
            }
            return stacks;
        };

        let workers = {};
        let nCpus = require('os').cpus().length;
        let linearBarrier = new LinearBarrier(nCpus, (mergedData) => processRepositoryCommits(mergedData));
        let shuffledPaths = shuffle(generatePaths(days), nCpus);

        while (nCpus--) {
            let worker = child_process.fork('child.js',{
                env: {
                    PATHS: shuffledPaths[nCpus]
                }
            });
            worker.on('message', (message) => {
                switch (message.type) {
                    case 'fetchDataSuccess':
                        workers[message.pid].kill();
                        linearBarrier.countDown(message.data);
                        break;
                    case 'log':
                        log(message.pid, message.level, message.payload);
                        break;
                }
            });
            workers[worker.pid] = worker;
        }
        cluster.on('death', (worker) => {
            console.log(`pid:${worker.pid} killed`);
        });
    };

    let spawnListeners = (port) => {
        log('master', 'log', 'spawning listeners');
        let nCpus = require('os').cpus().length;
        let cpuIndex = nCpus;
        while (cpuIndex--) {
            let listener = cluster.fork({
                PORT: port,
                POSTGRES_USERNAME: process.env.POSTGRES_USERNAME || 'postgres',
                POSTGRES_DATABASE: process.env.POSTGRES_DATABASE || 'postgres',
                POSTGRES_PASSWORD: process.env.POSTGRES_PASSWORD || 'postgres',
                POSTGRES_HOST: process.env.POSTGRES_HOST || 'localhost',
                POSTGRES_PORT: process.env.POSTGRES_PORT || 5432,
                POSTGRES_POOL_SIZE: Math.floor(db_pool_size / nCpus),
                POSTGRES_TIMEOUT: db_timeout
            });
            listener.on('message', (message) => {
                switch (message.type) {
                    case'log':
                        log(message.pid, message.level, message.payload);
                        break;
                    case'update':
                        updateData(max_days_of_data);
                        break;
                }
            });
        }
    };

    let Client = require('pg').Client;
    let client = new Client(pgConfig);

    client.connect((err) => {
        err && console.error('error connecting client', err);
        !err && client.query(`
    CREATE TABLE IF NOT EXISTS "repository" (
        name CHARACTER VARYING(100) PRIMARY KEY NOT NULL,
        n_commits INTEGER NOT NULL
    ); 
    CREATE TABLE IF NOT EXISTS "user" (
        hashed_email CHARACTER(40) PRIMARY KEY NOT NULL,
        any_commit_sha CHARACTER(40) NOT NULL
    );
    CREATE TABLE IF NOT EXISTS "commits" (
        repository_name CHARACTER VARYING(100) NOT NULL,
        user_hashed_email CHARACTER(40) NOT NULL,
        n_commits INTEGER NOT NULL,
        PRIMARY KEY (repository_name, user_hashed_email),
        FOREIGN KEY (repository_name) REFERENCES "repository" (name),
        FOREIGN KEY (user_hashed_email) REFERENCES "user" (hashed_email)
    );`, (err) => {
            client.end();
            err && console.error('error creating tables', err);
        });

    });

    spawnListeners(8080);

    // schedule.scheduleJob('0 0 * * * *', () => { //TODO
    //     updateData();
    // });
}

/**
 * Worker / Listener
 */
if (cluster.isWorker) {
    let http = require('http');
    const pgConfig = {
        user: process.env.POSTGRES_USERNAME || 'postgres',
        database: process.env.POSTGRES_DATABASE || 'postgres',
        password: process.env.POSTGRES_PASSWORD || 'postgres',
        host: process.env.POSTGRES_HOST || 'localhost',
        post: process.env.POSTGRES_PORT || 5432,
        max: 10,
        idleTimeoutMillis: 30000
    };
    let port = process.env.PORT;
    let Pool = require('pg').Pool;
    let pool = new Pool(pgConfig);
    let queryDb = (table, callback) => {
        pool.connect((err, client, done) => {
            if (err) {
                callback(err);
                return;
            }
            client.query(`SELECT * FROM "${table}"`, (queryError, result) => {
                done();
                if (err) {
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
            response.writeHead(200, {'Content-Type': 'application/json', 'Access-Control-Allow-Origin':'*', 'Access-Control-Allow-Headers':"Origin, X-Requested-With, Content-Type, Accept"});
            response.end(JSON.stringify(queryResult.rows));
        }
    };
    let server = http.createServer((request, response) => {
        switch (request.url) {
            case '/repositories':
                queryDb('repository', (err, result) => respond(response, err, result));
                break;
            case '/users':
                queryDb('user', (err, result) => respond(response, err, result));
                break;
            case '/rels':
                queryDb('commits', (err, result) => respond(response, err, result));
                break;
            case '/force-update':
                // process.send({pid: process.pid, type: 'update'}); //TODO
                break;
            default:
                response.writeHead(404);
                response.end();
        }
    });
    server.listen(port);
    process.send({pid: process.pid, type: 'log', level: 'log', payload: `listening for connections on :${port}`});
}

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

/**
 * Reduces commits
 * @param repositoryCommits
 * @returns {Array}
 */
function reduceCommits(repositoryCommits) {
    return Object.values(groupBy(repositoryCommits, (commit) => {
        return commit.repo.concat(commit.authorEmailSha);
    }))
        .map((allCommitsOfAUserToARepository) => {
            return {
                repo: allCommitsOfAUserToARepository[0].repo,
                userEmailSha: allCommitsOfAUserToARepository[0].authorEmailSha,
                anyCommitSha: allCommitsOfAUserToARepository[0].sha,
                nCommits: allCommitsOfAUserToARepository.length
            };
        });
}