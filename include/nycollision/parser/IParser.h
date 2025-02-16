#pragma once
#include "../core/Record.h"
#include <string>
#include <memory>

namespace nycollision {

/**
 * @brief Interface for parsing collision records from various formats
 */
class IParser {
public:
    virtual ~IParser() = default;

    /**
     * @brief Parse a single record from a line of text
     * @param line The text line to parse
     * @return Parsed record or nullptr if parsing failed
     */
    virtual std::shared_ptr<Record> parseRecord(const std::string& line) const = 0;

protected:
    // Protected constructor to prevent direct instantiation
    IParser() = default;
};

/**
 * @brief Specialized interface for CSV parsing
 */
class ICSVParser : public IParser {
public:
    /**
     * @brief Split a CSV line into tokens
     * @param line The CSV line to split
     * @return Vector of tokens
     */
    virtual std::vector<std::string> tokenize(const std::string& line) const = 0;

protected:
    ICSVParser() = default;
};

} // namespace nycollision
