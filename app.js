/**
 * Created by rafaelkallis on 29.09.16.
 */
var express = require('express');
var app = express();

app.get('/', (req, res) => {
    res.send('Hello World!');
});

app.listen(8080);