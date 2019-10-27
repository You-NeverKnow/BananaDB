#include <iostream>
#include "httplib.h"
#include "Logger.h"
#include "utilities.h"

int main (int argc, char *argv[]) {
    using namespace httplib;

    Store store; int num;
    std::tie(store, num) = buildStore();
    Logger logger {num};

    Server server;

    // Add key-value pairs
    server.Post("/", [&](const Request& req, Response& res) {
        auto result = parseKeyValue(req.body);
        if (not result) {
            res.status = 400;
            res.set_content("Key/value pair not found!","text/plain");
            return;
        }

        auto [key, value] = std::tie(result->first, result->second);
        logger.log(key, value);
        store.emplace(key, value);
        res.set_content("Key-value pair logged", "text/plain");
    });

    // Get value for a key from the in memory data-store
    server.Get("/", [&](const Request& req, Response& res) {
        auto result = req.params.find("key");
        if (result == req.params.end()) {
            res.status = 400;
            res.set_content("Missing key field in query","text/plain");
            return;
        }

        auto value = store.find(result->second);
        if (value == store.end()) {
            res.status = 404;
            res.set_content("Database has no key: " + result->second,"text/plain");
            return;
        }

        res.set_content(value->second, "text/plain");
    });

    std::cout << "Serving at localhost:3000/" << '\n';
    server.listen("localhost", 3000);

};