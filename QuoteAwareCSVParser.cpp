#include "QuoteAwareCSVParser.h"

std::vector<std::string> QuoteAwareCSVParser::parseLine(const std::string &line) const {
    std::vector<std::string> tokens;
    std::string current;
    bool inQuotes = false;

    for (char c : line) {
        if (c == '"') {
            inQuotes = !inQuotes;
        }
        else if (c == ',' && !inQuotes) {
            tokens.push_back(current);
            current.clear();
        } else {
            current.push_back(c);
        }
    }
    // push last field
    tokens.push_back(current);

    return tokens;
}
