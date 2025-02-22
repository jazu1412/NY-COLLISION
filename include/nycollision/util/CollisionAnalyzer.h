#pragma once
#include "../data/DataSet.h"
#include "../parser/CSVParser.h"
#include <memory>
#include <string>

namespace nycollision {

/**
 * @brief Facade class providing a simplified interface for collision data analysis
 */
class CollisionAnalyzer {
public:
    /**
     * @brief Load collision data from a CSV file
     * @param filename Path to the CSV file
     * @throws std::runtime_error if file cannot be opened or parsing fails
     */
    void loadData(const std::string& filename) {
        dataset_ = std::make_unique<DataSet>();
        parser_ = std::make_unique<CSVParser>();
        dataset_->loadFromFile(filename, *parser_);
    }

    /**
     * @brief Get total number of records
     */
    size_t getTotalRecords() const {
        return dataset_ ? dataset_->size() : 0;
    }

    /**
     * @brief Find collisions in a specific borough
     */
    std::vector<std::shared_ptr<const IRecord>> findCollisionsInBorough(
        const std::string& borough
    ) const {
        return dataset_ ? dataset_->queryByBorough(borough) : std::vector<std::shared_ptr<const IRecord>>{};
    }

    /**
     * @brief Find collisions in a specific ZIP code
     */
    std::vector<std::shared_ptr<const IRecord>> findCollisionsInZipCode(
        const std::string& zipCode
    ) const {
        return dataset_ ? dataset_->queryByZipCode(zipCode) : std::vector<std::shared_ptr<const IRecord>>{};
    }

    /**
     * @brief Find collisions within a date range
     */
    std::vector<std::shared_ptr<const IRecord>> findCollisionsInDateRange(
        const std::string& startDate,
        const std::string& endDate
    ) const {
        if (!dataset_) return std::vector<std::shared_ptr<const IRecord>>{};
        Date start{startDate, "00:00"};
        Date end{endDate, "23:59"};
        return dataset_->queryByDateRange(start, end);
    }

    /**
     * @brief Find collisions involving a specific vehicle type
     */
    std::vector<std::shared_ptr<const IRecord>> findCollisionsByVehicleType(
        const std::string& vehicleType
    ) const {
        return dataset_ ? dataset_->queryByVehicleType(vehicleType) : std::vector<std::shared_ptr<const IRecord>>{};
    }

    /**
     * @brief Find collisions within a geographic bounding box
     */
    std::vector<std::shared_ptr<const IRecord>> findCollisionsInArea(
        float minLat, float maxLat,
        float minLon, float maxLon
    ) const {
        return dataset_ ? dataset_->queryByGeoBounds(minLat, maxLat, minLon, maxLon) 
                       : std::vector<std::shared_ptr<const IRecord>>{};
    }

    /**
     * @brief Find collisions with injuries in a specific range
     */
    std::vector<std::shared_ptr<const IRecord>> findCollisionsByInjuryCount(
        int minInjuries,
        int maxInjuries
    ) const {
        return dataset_ ? dataset_->queryByInjuryRange(minInjuries, maxInjuries)
                       : std::vector<std::shared_ptr<const IRecord>>{};
    }

    /**
     * @brief Find collisions with fatalities in a specific range
     */
    std::vector<std::shared_ptr<const IRecord>> findCollisionsByFatalityCount(
        int minFatalities,
        int maxFatalities
    ) const {
        return dataset_ ? dataset_->queryByFatalityRange(minFatalities, maxFatalities)
                       : std::vector<std::shared_ptr<const IRecord>>{};
    }

    /**
     * @brief Find a specific collision by its unique key
     */
    std::shared_ptr<const IRecord> findCollisionByKey(int key) const {
        return dataset_ ? dataset_->queryByUniqueKey(key) : nullptr;
    }

    /**
     * @brief Get access to the underlying dataset for benchmarking
     */
    const DataSet& getDataset() const {
        if (!dataset_) {
            throw std::runtime_error("Dataset not loaded");
        }
        return *dataset_;
    }

private:
    std::unique_ptr<DataSet> dataset_;
    std::unique_ptr<CSVParser> parser_;
};

} // namespace nycollision
