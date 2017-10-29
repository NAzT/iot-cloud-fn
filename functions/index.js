const functions = require('firebase-functions');
const bigquery = require('@google-cloud/bigquery')();
const moment = require('moment-timezone');

const config = {
  DATASET: 'SENSOR_DS_1',
  TABLE: 'MQTT_V1',
};

function insertRows (rows) {
  bigquery
    .dataset(config.DATASET)
    .table(config.TABLE)
    .insert(rows)
    .then((data) => {
      console.log(`${rows.length} rows inserted.`);
      console.log(data[0]);
    })
    .catch((err) => {
      console.error(`ERROR: ${err}`);
    });
}

exports.save = functions.https.onRequest((req, res) => {
  if (req.method === 'POST') {
    const formattedDate = moment().tz('Asia/Bangkok').format('YYYY-MM-DD HH:mm:ss');
    const responseJson = {date: formattedDate, body: req.body};
    const obj = Object.assign({}, req.body);
    console.log('obj2', typeof obj, obj);
    obj.d.field1 = obj.d.temperature;
    obj.d.field2 = obj.d.humidity;
    delete obj.d.temperature;
    delete obj.d.humidity;
    insertRows([obj]);
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
