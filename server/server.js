// ==================================
// Imports
// ==================================
const fs = require('fs')
const path = require('path')
const express = require('express')

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

// ==================================
// Event loop
// ==================================
app.get('/', (req, res) => {
    res.send(store[req.body.key])
});

app.post('/', (req, res) => {
    const kv = req.body
    store[kv.key] = kv.value
    res.send("Key-value pair logged")
})

const port = 3000
app.listen(port, () => console.log(`Example app listening on port ${port}!`))
