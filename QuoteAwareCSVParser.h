#pragma once
#include "ICSVParser.h"

/**
 * @brief A quote-aware CSV parser.
 *        Splits on commas only if not inside quotes.
 *        Basic version: doesn't handle escaped quotes, etc.
 */
class QuoteAwareCSVParser : public ICSVParser {
public:
    // Override parseLine from ICSVParser
    std::vector<std::string> parseLine(const std::string &line) const override;
};
