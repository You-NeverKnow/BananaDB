#ifndef SERVER_UTILITIES_H
#define SERVER_UTILITIES_H

#include <fstream>
#include "json.hpp"

using KeyValue = std::pair<std::string, std::string>;
using Maybe = std::optional<KeyValue>;
using Store = std::unordered_map<std::string, std::string>;

/*
 * Parse Json of schema:
 *  {
 *      "key": some_key,
 *      "value": some_key,
 *  }
 */
Maybe parseKeyValue (const std::string& body) {
    using jsonParser = nlohmann::json;
    auto json = jsonParser::parse(body);

    // Validate json
    auto maybeKey = json.find("key");
    auto maybeValue = json.find("value");

    if (maybeKey == json.end() or maybeValue == json.end()) {
        return {};
    }

    // Constraint: keys and values are strings
    return KeyValue {maybeKey.value(), maybeValue.value()};
}


void buildStoreFromFile (Store& store, std::ifstream& f) {
    std::string tuple;
    while (std::getline(f, tuple)) {
        auto sep = tuple.find(',');
        auto key = tuple.substr(0, sep);
        auto value = tuple.substr(sep+1);
        store[key] = value;
    }
}

/*
 * Populate in memory key-value store with
 * key-value tuples from persistent logs
 */
std::pair<Store, int> buildStore() {
    Store store{};
    auto fileNum = 0;
    auto filename = "redo" + std::to_string(fileNum) + ".log";
    std::ifstream f {filename};

    while (f.is_open()) {
        buildStoreFromFile(store, f);
        f.close();
        filename = "redo" + std::to_string(++fileNum) + ".log";
        f.open(filename);
    }

    return {store, fileNum};
}

#endif //SERVER_UTILITIES_H
