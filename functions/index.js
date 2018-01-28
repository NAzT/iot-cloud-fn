const functions = require('firebase-functions')
const bigquery = require('@google-cloud/bigquery')()
const moment = require('moment-timezone')
const parsers = require('cmmc-parsers')

function insertRows (rows, config, cb) {
  bigquery
    .dataset(config.DATASET)
    .table(config.TABLE)
    .insert(rows)
    .then((data) => {
      // console.log(`${rows.length} rows inserted.`);
      // console.log(data[0]);
      // console.log('.>', data)
      cb(null, data)
    })
    .catch((err) => {
      console.error(`INSERT ERROR: ${err}`)
      cb(err)
    })
}

exports.save = functions.https.onRequest((req, res) => {
  const config = {
    DATASET: 'SENSOR_DS_1',
    TABLE: 'MQTT_V1',
  }
  if (req.method === 'POST') {
    const formattedDate = moment().format('YYYY-MM-DD HH:mm:ss')
    const responseJson = {date: formattedDate, body: req.body}
    const obj = Object.assign({}, req.body)
    delete obj.d.temperature
    delete obj.d.humidity
    insertRows([obj], config)
    res.status(200).send(JSON.stringify(responseJson))
  }
  else if (req.method === 'PUT') {
    res.status(403).send('Forbidden!')
  }
  else {
    res.status(403).send('Forbidden!')
  }
})

exports.traffy = functions.https.onRequest((req, res) => {
  const config = {
    DATASET: 'TRAFFY_BIN',
    TABLE: 'v1',
  }
  if (req.method === 'POST') {
    const row = Object.assign({}, req.body)
    console.log('v1 row => ', row)
    insertRows([row], config)
    res.status(200).send('OK')
  }
  else {
    res.status(403).send('Forbidden!')
  }
})

exports.traffy_v2 = functions.https.onRequest((req, res) => {
  const config = {
    DATASET: 'TRAFFY_BIN',
    TABLE: 'v2',
  }
  if (req.method === 'POST') {
    const row = Object.assign({}, req.body)
    row.unix = new Date()
    row._time = moment.tz('Asia/Bangkok').unix()
    row.log_timestamp = moment.tz('Asia/Bangkok').format()
    console.log('v2 row => ', row)
    insertRows([row], config)
    res.status(200).send('OK')
  }
  else {
    res.status(403).send('Forbidden!')
  }
})

exports.sensor_node = functions.https.onRequest((req, res) => {
  const config = {
    DATASET: 'SENSOR_DS_1',
    TABLE: 'TEST_NO_1',
  }
  if (req.method === 'POST') {
    const formattedDate = moment().format('YYYY-MM-DD HH:mm:ss')
    const responseJson = {date: formattedDate, body: req.body}
    const row = Object.assign({}, req.body)
    row.unix = new Date()
    row._time = moment.tz('Asia/Bangkok').unix()
    console.log(row)
    insertRows([row], config)
    res.status(200).send(JSON.stringify(responseJson))
  }
  else if (req.method === 'PUT') {
    res.status(403).send('Forbidden!')
  }
  else {
    res.status(403).send('Forbidden!')
  }
})

exports.nb_iot = functions.https.onRequest((req, res) => {
  const config = {
    DATASET: 'NB_IoT',
    TABLE: 'V2',
  }
  if (req.method === 'POST') {
    const message = req.body.message
    const inByte = Buffer.from(message, 'hex')
    var p = parsers.header.parse(inByte)
    const row = Object.assign({}, p)
    row.gps_latitude = row.gps_latitude / 10000000.0
    row.gps_longitude = row.gps_longitude / 10000000.0
    row.temperature_c = row.temperature_c / 100.0
    row.humidity_percent_rh = row.humidity_percent_rh / 100.0
    row.gas_resistance_ohm = row.gas_resistance_ohm / 1000.0
    row.unix = moment.tz('Asia/Bangkok').unix()
    row.since = moment.tz('Asia/Bangkok').toString()
    row._time = moment.tz('Asia/Bangkok').unix()
    console.log(row)
    insertRows([row], config)
    res.status(200).send(row)
  }
  else if (req.method === 'PUT') {
    res.status(403).send('Forbidden!')
  }
  else {
    res.status(403).send('Forbidden!')
  }
})

exports.sensor_node_series = functions.https.onRequest((req, res) => {
  if (req.method === 'POST') {
    if (Array.isArray(req.body)) {
      console.log(`input body is an array. len=${req.body.length}`)
      req.body.forEach(function (item, idx) {
        try {
          let parser = parsers.CMMC
          const result = parser.parse(Buffer.from(item.payload, 'hex'))
          console.log(item.payload)
          console.log(result)
          console.log(result.sensor_node)
          console.log(`[${idx}] ${result.sensor_node.device_name} - ${item.timestamp} field1 - ${result.sensor_node.field1 / 100} field2 - ${result.sensor_node.field2 / 100}`)
          console.log(`checksum = ${result.controller_sum}`)
        }
        catch (ex) {
          console.error('packet parsing error..', ex, item.payload)
        }
      })
    }
    res.status(200).send('parsing ok')
    // console.info(typeof req.body, req.body.length)
    // console.log(req.body)
  }
  else if (req.method === 'PUT') {
    res.status(403).send('Forbidden!')
  }
  else {
    res.status(403).send('Forbidden!')
  }
})

exports.kkao = functions.https.onRequest((req, res) => {
  if (req.method === 'POST') {
    console.log('body.data', req.body.data)
    const config = {
      DATASET: 'Trash3',
      TABLE: 'rev2',
    }
    if (Array.isArray(req.body)) {
      console.log(`input body is an array. len=${req.body.length}`)
      // req.body.forEach(function (item, idx) {
      //   try {
      //     let parser = parsers.CMMC
      //     const result = parser.parse(Buffer.from(item.payload, 'hex'))
      //     console.log(item.payload)
      //     console.log(result)
      //     console.log(result.sensor_node)
      //     console.log(`[${idx}] ${result.sensor_node.device_name} - ${item.timestamp} field1 - ${result.sensor_node.field1 / 100} field2 - ${result.sensor_node.field2 / 100}`)
      //     console.log(`checksum = ${result.controller_sum}`)
      //   }
      //   catch (ex) {
      //     console.error('packet parsing error..', ex, item.payload)
      //   }
      // })
    }
    else {
      console.log(req.body)
      let parser = parsers.SmartTrashP3
      const result = parser.parse(Buffer.from(req.body.payload, 'hex'))
      // console.log(result)
      const row = Object.assign({}, result)
      row.unix = moment.tz('Asia/Bangkok').unix()
      insertRows([row], config)
    }
    res.status(200).send('parsing ok')
    // console.info(typeof req.body, req.body.length)
    // console.log(req.body)
  }
  else if (req.method === 'PUT') {
    res.status(403).send('Forbidden!')
  }
  else {
    res.status(403).send('Forbidden!')
  }
})

exports.v3 = functions.https.onRequest((req, res) => {
  // console.log('req.body', req.body)
  // console.log('req.body.payload', req.body.payload)
  if (req.method === 'POST') {
    const config = {
      DATASET: 'TRAFFY_TEST',
      TABLE: 'V3_1_1_2',
    }
    const row = Object.assign({}, req.body.data)
    row.server_unix = moment.tz('Asia/Bangkok').unix()
    // console.log(`incoming data from: ${row.linkit}`)
    insertRows([row], config, (err, data) => {
      if (!err) {
        console.log(`distance = ${row.distance_cm}cm, batt = ${row.battery_raw / 100.0}V, sound = ${row.sound_avg_db / 100.0}dB`)
        console.log(`${row.linkit} [${row.gps_latitude}, ${row.gps_longitude}] (${row.gps_diff}) write ok!`)
        res.status(200).send(JSON.stringify({
          method: req.method,
          body: req.body,
        }))
      } else {
        if (Array.isArray(err.errors)) {
          err.errors.forEach((v, idx) => {
            v.errors.forEach((v, idx) => {
              console.error(`---- error: ${v.message}`)
            })
          })
        } else {
          console.error(`error - `, err)
        }
        console.error('write error!', err)
        res.status(500).send('Forbidden!')
      }
    })
  }
})

exports.v3_2 = functions.https.onRequest((req, res) => {
  if (req.method === 'POST') {
    const config = {
      DATASET: 'TRAFFY_TEST',
      TABLE: 'V3_2_1_2_11',
    }
    let rows = req.body.data
    console.log(JSON.stringify(rows))
    if (!Array.isArray(rows)) {
      rows = [rows]
    }
    rows.forEach((v, idx) => { v.gps_altitude_cm = parseInt(v.gps_altitude_cm, 10) })
    insertRows(rows, config, (err, data) => {
      if (!err) {
        console.log('insert ok.')
        res.status(200).send('ok')
      }
      else {
        if (Array.isArray(err.errors)) {
          err.errors.forEach((v, idx) => {
            v.errors.forEach((v, idx) => {
              console.error(`---- error: ${v.message}`)
            })
          })
        } else {
          console.error(`error - `, err)
        }
        res.status(500).send('insert error')
      }
    })
  }
  else {
    res.status(500).send('Forbidden!')
  }

})
