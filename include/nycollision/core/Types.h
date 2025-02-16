#pragma once
#include <string>
#include <vector>

namespace nycollision {

/**
 * @brief Represents a geographic coordinate
 */
struct GeoCoordinate {
    float latitude = 0.0f;
    float longitude = 0.0f;
};

/**
 * @brief Represents a date in the collision record
 */
struct Date {
    std::string date;
    std::string time;

    // Comparison operators for container operations
    bool operator<(const Date& other) const {
        return (date + time) < (other.date + other.time);
    }
    
    bool operator<=(const Date& other) const {
        return (date + time) <= (other.date + other.time);
    }
    
    bool operator>(const Date& other) const {
        return (date + time) > (other.date + other.time);
    }
    
    bool operator>=(const Date& other) const {
        return (date + time) >= (other.date + other.time);
    }
    
    bool operator==(const Date& other) const {
        return (date + time) == (other.date + other.time);
    }
    
    bool operator!=(const Date& other) const {
        return !(*this == other);
    }
};

/**
 * @brief Statistics about injuries and fatalities
 */
struct CasualtyStats {
    int persons_injured = 0;
    int persons_killed = 0;
    int pedestrians_injured = 0;
    int pedestrians_killed = 0;
    int cyclists_injured = 0;
    int cyclists_killed = 0;
    int motorists_injured = 0;
    int motorists_killed = 0;

    int getTotalInjuries() const {
        return persons_injured + pedestrians_injured + cyclists_injured + motorists_injured;
    }

    int getTotalFatalities() const {
        return persons_killed + pedestrians_killed + cyclists_killed + motorists_killed;
    }
};

/**
 * @brief Information about vehicles involved in the collision
 */
struct VehicleInfo {
    std::vector<std::string> contributing_factors;
    std::vector<std::string> vehicle_types;
};

} // namespace nycollision
