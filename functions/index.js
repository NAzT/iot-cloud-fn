const functions = require('firebase-functions');
const bigquery = require('@google-cloud/bigquery')();
const moment = require('moment-timezone');

function insertRows (rows, config) {
    bigquery
      .dataset(config.DATASET)
      .table(config.TABLE)
      .insert(rows)
      .then((data) => {
          console.log(`${rows.length} rows inserted.`);
          console.log(data[0]);
      })
      .catch((err) => {
          console.error(`INSERT ERROR: ${err}`);
      });
}

exports.save = functions.https.onRequest((req, res) => {
    const config = {
        DATASET: 'SENSOR_DS_1',
        TABLE: 'MQTT_V1',
    };
    if (req.method === 'POST') {
        const formattedDate = moment().format('YYYY-MM-DD HH:mm:ss');
        const responseJson = {date: formattedDate, body: req.body};
        const obj = Object.assign({}, req.body);
        delete obj.d.temperature;
        delete obj.d.humidity;
        insertRows([obj], config);
        res.status(200).send(JSON.stringify(responseJson));
    }
    else if (req.method === 'PUT') {
        res.status(403).send('Forbidden!');
    }
    else {
        res.status(403).send('Forbidden!');
    }
});

exports.traffy = functions.https.onRequest((req, res) => {
    const config = {
        DATASET: 'TRAFFY_BIN',
        TABLE: 'v1',
    };
    if (req.method === 'POST') {
        const row = Object.assign({}, req.body);
        console.log('v1 row => ', row);
        insertRows([row], config);
        res.status(200).send('OK');
    }
    else {
        res.status(403).send('Forbidden!');
    }
});

exports.traffy_v2 = functions.https.onRequest((req, res) => {
    const config = {
        DATASET: 'TRAFFY_BIN',
        TABLE: 'v2',
    };
    if (req.method === 'POST') {
        const row = Object.assign({}, req.body);
        row.unix = new Date();
        row._time = moment.tz('Asia/Bangkok').unix();
        row.log_timestamp = moment.tz('Asia/Bangkok').format();
        console.log('v2 row => ', row);
        insertRows([row], config);
        res.status(200).send('OK');
    }
    else {
        res.status(403).send('Forbidden!');
    }
});


exports.sensor_node = functions.https.onRequest((req, res) => {
    const config = {
        DATASET: 'SENSOR_DS_1',
        TABLE: 'TEST_NO_1',
    };
    if (req.method === 'POST') {
        const formattedDate = moment().format('YYYY-MM-DD HH:mm:ss');
        const responseJson = {date: formattedDate, body: req.body};
        const obj = Object.assign({}, req.body);
        insertRows([obj], config);
        res.status(200).send(JSON.stringify(responseJson));
    }
    else if (req.method === 'PUT') {
        res.status(403).send('Forbidden!');
    }
    else {
        res.status(403).send('Forbidden!');
    }
})