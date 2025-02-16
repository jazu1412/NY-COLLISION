#pragma once
#include "IRecord.h"

namespace nycollision {

/**
 * @brief Concrete implementation of a collision record
 */
class Record : public IRecord {
public:
    // Location information
    const std::string& getBorough() const override { return borough_; }
    const std::string& getZipCode() const override { return zip_code_; }
    GeoCoordinate getLocation() const override { return location_; }
    const std::string& getOnStreet() const override { return on_street_; }
    const std::string& getCrossStreet() const override { return cross_street_; }
    const std::string& getOffStreet() const override { return off_street_; }

    // Time information
    Date getDateTime() const override { return date_time_; }

    // Casualty information
    const CasualtyStats& getCasualtyStats() const override { return casualty_stats_; }

    // Vehicle information
    const VehicleInfo& getVehicleInfo() const override { return vehicle_info_; }

    // Unique identifier
    int getUniqueKey() const override { return unique_key_; }

    // Setters for building the record
    void setBorough(const std::string& borough) { borough_ = borough; }
    void setZipCode(const std::string& zip) { zip_code_ = zip; }
    void setLocation(const GeoCoordinate& loc) { location_ = loc; }
    void setOnStreet(const std::string& street) { on_street_ = street; }
    void setCrossStreet(const std::string& street) { cross_street_ = street; }
    void setOffStreet(const std::string& street) { off_street_ = street; }
    void setDateTime(const Date& dt) { date_time_ = dt; }
    void setCasualtyStats(const CasualtyStats& stats) { casualty_stats_ = stats; }
    void setVehicleInfo(const VehicleInfo& info) { vehicle_info_ = info; }
    void setUniqueKey(int key) { unique_key_ = key; }

private:
    std::string borough_;
    std::string zip_code_;
    GeoCoordinate location_;
    std::string on_street_;
    std::string cross_street_;
    std::string off_street_;
    Date date_time_;
    CasualtyStats casualty_stats_;
    VehicleInfo vehicle_info_;
    int unique_key_ = 0;
};

} // namespace nycollision
