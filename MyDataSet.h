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
     * @brief Return the total number of loaded records
     */
    size_t size() const { return records_.size(); }

private:
    std::vector<DataRecord> records_;
};
