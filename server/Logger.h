#ifndef SERVER_LOGGER_H
#define SERVER_LOGGER_H

#include <string>
#include <fstream>

/*
 * Writes key-values to disk for reconstruction of
 * key-value store in case of failure.
 *
 * NOT thread-safe
 */
class Logger {
    std::ofstream file;
    int fileNum = 1;
    int fileItems = 0;
    const int maxItems = 100;
public:
    explicit Logger (int num)
        : fileNum(num), file ("redo" + std::to_string(num) + ".log", std::ios::app) {}

    Logger (Logger& other) = delete;
    Logger& operator=(Logger& other) = delete;
    ~Logger();
private:
    void changeFile();
public:
    void log(const std::string& key, const std::string& value);
};

/*
 * Changes underlying log file.
 * The purpose is to prevent infinite growing log files.
 */
void Logger::changeFile() {
    file.close();
    file.open({"redo" + std::to_string(++fileNum) + ".log"});
}

Logger::~Logger() {
    file.close();
}

/*
 * Store the key-value tuple in a log file.
 */
void Logger::log(const std::string &key, const std::string &value) {
    file << key << "," << value << '\n';
    file.flush();

    if (++fileItems == maxItems) {
        changeFile();
        fileItems = 0;
    }
}

#endif //SERVER_LOGGER_H
