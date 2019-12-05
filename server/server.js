// ==================================
// Imports
// ==================================
const fs = require('fs')
const path = require('path')
const express = require('express')
const https = require('https');

// ==================================
// Init
// ==================================
const app = express()
app.use(express.json())

let store = function () {
    const directoryPath = path.join(__dirname, "logs");
    const files = fs.readdirSync(directoryPath)
    let _store = {}
    files.forEach(file => {
        const data = fs.readFileSync(path.join(directoryPath, file)).toString()
        const file_kv = data.slice(0, data.length - 1) + '}'
        Object.assign(_store, JSON.parse(file_kv))
    })
    return _store
}()

let fileName = Date.now().toString()
let itemCount = 0
const maxItems = 100
const heartBeatTime = 10_000
let heartBeat = false

// ==================================
// Event loop
// ==================================
app.get('/', (req, res) => {
    res.send(store[req.body.key])
});

app.get('/heartbeat', (req, res) => {
    heartBeat = true
});

app.post('/', (req, res) => {
    const kv = req.body
    fs.appendFileSync(fileName, `${kv.key}:${kv.value},`)
    itemCount += 1
    store[kv.key] = kv.value
    if (itemCount === maxItems) {
        itemCount = 0
        fileName = Date.now().toString()
    }
    res.send("Key-value pair logged")
})

setInterval(() => {
   if (heartBeat) {
        heartBeat = false
   }
   // Init leader election
   https.put()
}, heartBeatTime)

const port = 3000
app.listen(port, () => console.log(`Example app listening on port ${port}!`))
