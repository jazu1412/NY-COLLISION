#pragma once
#include "IParser.h"
#include <string>

namespace nycollision {

/**
 * @brief CSV parser that handles quoted fields and escaping
 */
class CSVParser : public ICSVParser {
public:
    /**
     * @brief Construct a new CSV Parser
     * @param delimiter The character used to separate fields (default: ',')
     * @param quote The character used for quoting fields (default: '"')
     */
    explicit CSVParser(char delimiter = ',', char quote = '"')
        : delimiter_(delimiter), quote_(quote) {}

    /**
     * @brief Parse a CSV line into a Record object
     * @param line The CSV line to parse
     * @return Parsed record or nullptr if parsing failed
     */
    std::shared_ptr<Record> parseRecord(const std::string& line) const override;

    /**
     * @brief Split a CSV line into tokens, respecting quotes
     * @param line The CSV line to split
     * @return Vector of tokens
     */
    std::vector<std::string> tokenize(const std::string& line) const override;

private:
    char delimiter_;
    char quote_;

    /**
     * @brief Convert a string to a float, with error handling
     * @param str The string to convert
     * @param defaultValue Value to return if conversion fails
     * @return Converted float value or defaultValue
     */
    static float toFloat(const std::string& str, float defaultValue = 0.0f);

    /**
     * @brief Convert a string to an integer, with error handling
     * @param str The string to convert
     * @param defaultValue Value to return if conversion fails
     * @return Converted integer value or defaultValue
     */
    static int toInt(const std::string& str, int defaultValue = 0);
};

} // namespace nycollision
