#pragma once

#include <vector>
#include <string>
#include "DataRecord.h"
#include "ICSVParser.h"

/**
 * @brief A container for DataRecord rows, plus queries.
 */
class MyDataSet {
public:
    /**
     * @brief Loads data from a CSV file using the provided CSV parser.
     * @param filename Path to the CSV
     * @param parser   The CSV parser (injected dependency)
     */
    void loadFromCSV(const std::string &filename, const ICSVParser &parser);

    /**
     * @brief Example bounding-box query on lat/long
     */
    std::vector<DataRecord> rangeQuery(
        float minLat, float maxLat,
        float minLon, float maxLon
    ) const;

     std::vector<DataRecord> findNoOfRecordsWithMinInjured(
       int minInjured
    ) const;

     std::vector<DataRecord> findNoOfRecordsWithMotoristKilled(
       int motoristsKilled
    ) const;



  /**
     * @brief 2) Searches collisions in a given borough (exact match).
     * @param boroughName The borough to match (e.g. "BROOKLYN").
     * @return All collisions matching that borough.
     */
    std::vector<DataRecord> searchByBorough(const std::string &boroughName) const;

    /**
     * @brief 3) Searches collisions in a given ZIP code.
     * @param zip The ZIP code string.
     * @return All collisions in that ZIP code.
     */
    std::vector<DataRecord> searchByZIP(const std::string &zip) const;

    /**
     * @brief 4) Searches collisions by date range [startDate, endDate].
     *        Date format depends on how you store it (string, parse as YYYY/MM/DD, etc.).
     * @param startDate Inclusive lower bound date string.
     * @param endDate   Inclusive upper bound date string.
     * @return collisions whose date is in [startDate, endDate].
     */
    std::vector<DataRecord> searchByDateRange(
        const std::string &startDate,
        const std::string &endDate
    ) const;

    /**
     * @brief 5) Searches collisions by a particular vehicle type code.
     *        (Any of vehicle_type_code_1..5 might match).
     * @param vehicleType E.g. "Sedan", "Taxi", "Bike", etc.
     * @return collisions that mention this vehicle type in any vehicle code field.
     */
    int searchByVehicleType(const std::string &vehicleType) const;

    /**
     * @brief 6) Searches collisions with total injuries in [minInjury, maxInjury].
     *        Summation of persons_injured, pedestrians_injured, cyclist_injured, motorist_injured.
     * @return collisions matching that total injuries range.
     */
    std::vector<DataRecord> searchByInjuryRange(int minInjury, int maxInjury) const;




    /**
     * @brief Return the total number of loaded records
     */
    size_t size() const { return records_.size(); }
 /**
     * @brief 7) Searches collisions with total fatalities in [minFatal, maxFatal].
     *        Summation of persons_killed, pedestrians_killed, cyclist_killed, motorist_killed.
     * @return collisions matching that fatality count range.
     */
    std::vector<DataRecord> searchByFatalitiesRange(int minFatal, int maxFatal) const;
    /**
     * @brief 8) Finds collisions by unique key (assuming 1 record per key).
     * @param key The unique key integer.
     * @return A vector (possibly 1 or 0) of collisions that match that key.
     */
    std::vector<DataRecord> searchByUniqueKey(int key) const;

    std::vector<DataRecord> searchByPedestrianFatalitiesRange(int minFatal, int maxFatal) const;

     std::vector<DataRecord> searchByCyclistFatalitiesRange(int minFatal, int maxFatal) const;

      std::vector<DataRecord> searchByMotoristFatalitiesRange(int minFatal, int maxFatal) const;

private:
    std::vector<DataRecord> records_;
};
