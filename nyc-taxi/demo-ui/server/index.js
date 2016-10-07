'use strict'

const path = require('path')
const express = require('express')
const morgan = require('morgan')
const moment = require('moment')

const app = express()

app.use('/nyc-taxi/neighborhoods/', express.static(path.join(__dirname, 'data')))

app.use(morgan('dev'))

app.get('/nyc-taxi/tripInfo', (req, res) => {
  const start = moment(req.query.startTime, moment.ISO_8601)
    .minutes(Math.round(moment().minutes() / 15) * 15)
    .seconds(0)
  console.log('%s -> %s', start, JSON.stringify(req.query))
  res.send([
    [moment(start).add(0, 'minutes'), Math.ceil(Math.random() * 600)],
    [moment(start).add(15, 'minutes'), Math.ceil(Math.random() * 700)],
    [moment(start).add(30, 'minutes'), Math.ceil(Math.random() * 800)],
    [moment(start).add(45, 'minutes'), Math.ceil(Math.random() * 900)],
  ])
})

console.log('Listening on port 8079')
app.listen(8079)
