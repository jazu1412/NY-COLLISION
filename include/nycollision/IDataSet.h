#pragma once
#include "IRecord.h"
#include <memory>
#include <vector>

namespace nycollision {

/**
 * @brief Interface for querying collision records
 */
class IDataSet {
public:
    virtual ~IDataSet() = default;

    using RecordPtr = std::shared_ptr<const IRecord>;
    using Records = std::vector<RecordPtr>;

    /**
     * @brief Geographic bounding box query
     * @return Records within the specified lat/long bounds
     */
    virtual Records queryByGeoBounds(
        float minLat, float maxLat,
        float minLon, float maxLon
    ) const = 0;

    /**
     * @brief Borough-based query
     * @return Records matching the specified borough name
     */
    virtual Records queryByBorough(const std::string& borough) const = 0;

    /**
     * @brief ZIP code-based query
     * @return Records matching the specified ZIP code
     */
    virtual Records queryByZipCode(const std::string& zipCode) const = 0;

    /**
     * @brief Date range query
     * @return Records within the specified date range (inclusive)
     */
    virtual Records queryByDateRange(const Date& start, const Date& end) const = 0;

    /**
     * @brief Vehicle type query
     * @return Records involving the specified vehicle type
     */
    virtual Records queryByVehicleType(const std::string& vehicleType) const = 0;

    /**
     * @brief Total injuries range query
     * @return Records with total injuries within the specified range
     */
    virtual Records queryByInjuryRange(int minInjuries, int maxInjuries) const = 0;

    /**
     * @brief Total fatalities range query
     * @return Records with total fatalities within the specified range
     */
    virtual Records queryByFatalityRange(int minFatalities, int maxFatalities) const = 0;

    /**
     * @brief Unique key query
     * @return Record matching the specified unique key
     */
    virtual RecordPtr queryByUniqueKey(int key) const = 0;

    /**
     * @brief Specific casualty type queries
     */
    virtual Records queryByPedestrianFatalities(int minFatalities, int maxFatalities) const = 0;
    virtual Records queryByCyclistFatalities(int minFatalities, int maxFatalities) const = 0;
    virtual Records queryByMotoristFatalities(int minFatalities, int maxFatalities) const = 0;

    /**
     * @brief Returns total number of records in the dataset
     */
    virtual size_t size() const = 0;

protected:
    // Protected constructor to prevent direct instantiation
    IDataSet() = default;
};

} // namespace nycollision
