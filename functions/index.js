const functions = require('firebase-functions');
const bigquery = require('@google-cloud/bigquery')();
const moment = require('moment-timezone');

exports.save = functions.https.onRequest((req, res) => {
  if (req.method === 'POST') {
    const formattedDate = moment().tz('Asia/Bangkok').format('YYYY-MM-DD HH:mm:ss');
    const responseJson = {date: formattedDate, body: req.body};
    // console.log('Sending Formatted date:', formattedDate);
    console.log(req.body);
    res.status(200).send(JSON.stringify(responseJson));
  }
  else if (req.method === 'PUT') {
    res.status(403).send('Forbidden!');
  }
  else {
    res.status(403).send('Forbidden!');
  }
});

exports.helloWorld = function helloWorld (req, res) {
  if (req.body.message === undefined) {
    // This is an error case, as "message" is required
    res.status(400).send('No message defined!');
  } else {
    // Everything is ok
    console.log(req.body.message);
    res.status(200).end();
  }
};
