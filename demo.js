/**
 * Created by rafaelkallis on 03.10.16.
 */
let http = require('http');
let zlib = require('zlib');
let JSONStream = require('JSONStream');

let server_req = http.request({
    hostname: `data.githubarchive.org`,
    path: `/2016-09-09-1.json.gz`
}, (response) => {
    let commits = [];
    let pipe = response.pipe(zlib.createGunzip()).pipe(JSONStream.parse());
    pipe.on('data', (event) => {
        event.type == "PushEvent" && commits.push(...event.payload.commits
            .filter((commit) => commit.distinct)
            .map((commit) => ({
                    repository_name: event.repo.name,
                    user_hashed_email: commit.author.email.substr(0, 40),
                    n_commits: 1
                })
            )
        );
    });
    pipe.on('end', () => {

    });
});
server_req.on('error', (err) => {});
server_req.end();