#pragma once
#include <string>
#include <vector>

/**
 * @brief Interface for CSV parsers.
 */
class ICSVParser {
public:
    virtual ~ICSVParser() = default;

    /**
     * @brief Splits a single CSV line into tokens, respecting
     *        any needed quoting rules.
     * @param line The raw CSV line
     * @return Vector of tokens (columns)
     */
    virtual std::vector<std::string> parseLine(const std::string &line) const = 0;
};
