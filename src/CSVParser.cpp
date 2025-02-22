#include "../include/nycollision/parser/CSVParser.h"
#include "../include/nycollision/core/Record.h"
#include <sstream>
#include <stdexcept>

namespace nycollision {

 


std::vector<std::string> CSVParser::tokenize(const std::string& line) const {
    std::vector<std::string> tokens;
    std::string token;
    bool inQuotes = false;
    std::ostringstream currentToken;

    for (size_t i = 0; i < line.length(); ++i) {
        char c = line[i];
        
        if (c == quote_) {
            if (inQuotes && i + 1 < line.length() && line[i + 1] == quote_) {
                // Handle escaped quotes
                currentToken << c;
                ++i;
            } else {
                inQuotes = !inQuotes;
            }
        } else if (c == delimiter_ && !inQuotes) {
            // End of current token
            tokens.push_back(currentToken.str());
            currentToken.str("");
            currentToken.clear();
        } else {
            currentToken << c;
        }
    }
    
    // Add the last token
    tokens.push_back(currentToken.str());
    return tokens;
}

float CSVParser::toFloat(const std::string& str, float defaultValue) {
    try {
        return str.empty() ? defaultValue : std::stof(str);
    } catch (const std::exception&) {
        return defaultValue;
    }
}

int CSVParser::toInt(const std::string& str, int defaultValue) {
    try {
        return str.empty() ? defaultValue : std::stoi(str);
    } catch (const std::exception&) {
        return defaultValue;
    }
}

std::shared_ptr<Record> CSVParser::parseRecord(const std::string& line) const {
    auto tokens = tokenize(line);
    if (tokens.size() < 29) { // Minimum expected number of fields
        return nullptr;
    }

    auto record = std::make_shared<Record>();

    // Parse date and time
    Date date;
    date.date = tokens[0];
    date.time = tokens[1];
    record->setDateTime(date);

    // Parse location information
    record->setBorough(tokens[2]);
    record->setZipCode(tokens[3]);

    GeoCoordinate location;
    location.latitude = toFloat(tokens[4]);
    location.longitude = toFloat(tokens[5]);
    record->setLocation(location);

    record->setOnStreet(tokens[6]);
    record->setCrossStreet(tokens[7]);
    record->setOffStreet(tokens[8]);

    // Parse casualty statistics
    CasualtyStats stats;
    stats.persons_injured = toInt(tokens[9]);
    stats.persons_killed = toInt(tokens[10]);
    stats.pedestrians_injured = toInt(tokens[11]);
    stats.pedestrians_killed = toInt(tokens[12]);
    stats.cyclists_injured = toInt(tokens[13]);
    stats.cyclists_killed = toInt(tokens[14]);
    stats.motorists_injured = toInt(tokens[15]);
    stats.motorists_killed = toInt(tokens[16]);
    record->setCasualtyStats(stats);

    // Parse vehicle information
    VehicleInfo vehicleInfo;
    // Contributing factors
    for (int i = 0; i < 5; ++i) {
        const auto& factor = tokens[17 + i];
        if (!factor.empty()) {
            vehicleInfo.contributing_factors.push_back(factor);
        }
    }

    // Unique key
    record->setUniqueKey(toInt(tokens[23]));

    // Vehicle types
    for (int i = 0; i < 5; ++i) {
        const auto& type = tokens[24 + i];
        if (!type.empty()) {
            vehicleInfo.vehicle_types.push_back(type);
        }
    }
    record->setVehicleInfo(vehicleInfo);

    return record;
}

} // namespace nycollision
