#pragma once
#include "Types.h"
#include <string>

namespace nycollision {

/**
 * @brief Interface for collision records
 */
class IRecord {
public:
    virtual ~IRecord() = default;

    // Location information
    virtual const std::string& getBorough() const = 0;
    virtual const std::string& getZipCode() const = 0;
    virtual GeoCoordinate getLocation() const = 0;
    virtual const std::string& getOnStreet() const = 0;
    virtual const std::string& getCrossStreet() const = 0;
    virtual const std::string& getOffStreet() const = 0;

    // Time information
    virtual Date getDateTime() const = 0;

    // Casualty information
    virtual const CasualtyStats& getCasualtyStats() const = 0;

    // Vehicle information
    virtual const VehicleInfo& getVehicleInfo() const = 0;

    // Unique identifier
    virtual int getUniqueKey() const = 0;
};

} // namespace nycollision
