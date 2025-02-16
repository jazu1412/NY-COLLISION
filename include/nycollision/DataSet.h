#pragma once
#include "IDataSet.h"
#include "IParser.h"
#include <unordered_map>
#include <map>
#include <memory>
#include <string>
#include <fstream>
#include <stdexcept>

namespace nycollision {

/**
 * @brief Concrete implementation of collision records dataset with optimized querying
 */
class DataSet : public IDataSet {
public:
    /**
     * @brief Load records from a file using the specified parser
     * @param filename Path to the data file
     * @param parser Parser implementation to use
     * @throws std::runtime_error if file cannot be opened or parsing fails
     */
    void loadFromFile(const std::string& filename, const IParser& parser) {
        std::ifstream file(filename);
        if (!file) {
            throw std::runtime_error("Failed to open file: " + filename);
        }

        std::string line;
        // Skip header line
        std::getline(file, line);
        
        while (std::getline(file, line)) {
            if (auto record = parser.parseRecord(line)) {
                addRecord(record);
            }
        }
    }

    // IDataSet interface implementation
    Records queryByGeoBounds(
        float minLat, float maxLat,
        float minLon, float maxLon
    ) const override;

    Records queryByBorough(const std::string& borough) const override;
    Records queryByZipCode(const std::string& zipCode) const override;
    Records queryByDateRange(const Date& start, const Date& end) const override;
    Records queryByVehicleType(const std::string& vehicleType) const override;
    Records queryByInjuryRange(int minInjuries, int maxInjuries) const override;
    Records queryByFatalityRange(int minFatalities, int maxFatalities) const override;
    RecordPtr queryByUniqueKey(int key) const override;
    Records queryByPedestrianFatalities(int minFatalities, int maxFatalities) const override;
    Records queryByCyclistFatalities(int minFatalities, int maxFatalities) const override;
    Records queryByMotoristFatalities(int minFatalities, int maxFatalities) const override;
    
    size_t size() const override { return records_.size(); }

    // Spatial index cell structure
    struct GridCell {
        float minLat, maxLat;
        float minLon, maxLon;
        std::vector<std::shared_ptr<Record>> records;
    };

private:
    void addRecord(std::shared_ptr<Record> record);
    
    // Helper to convert internal records to interface records
    Records convertToInterfaceRecords(const std::vector<std::shared_ptr<Record>>& records) const {
        Records result;
        result.reserve(records.size());
        for (const auto& record : records) {
            result.push_back(std::static_pointer_cast<const IRecord>(record));
        }
        return result;
    }

    // Primary storage
    std::vector<std::shared_ptr<Record>> records_;

    // Indices for efficient querying
    std::unordered_map<int, std::shared_ptr<Record>> keyIndex_;
    std::unordered_map<std::string, std::vector<std::shared_ptr<Record>>> boroughIndex_;
    std::unordered_map<std::string, std::vector<std::shared_ptr<Record>>> zipIndex_;
    std::map<Date, std::vector<std::shared_ptr<Record>>> dateIndex_;
    
    // Spatial index using simple grid-based approach
    std::vector<GridCell> spatialGrid_;
    
    // Indices for range queries
    std::map<int, std::vector<std::shared_ptr<Record>>> injuryIndex_;
    std::map<int, std::vector<std::shared_ptr<Record>>> fatalityIndex_;
    std::map<int, std::vector<std::shared_ptr<Record>>> pedestrianFatalityIndex_;
    std::map<int, std::vector<std::shared_ptr<Record>>> cyclistFatalityIndex_;
    std::map<int, std::vector<std::shared_ptr<Record>>> motoristFatalityIndex_;
    
    // Vehicle type index
    std::unordered_map<std::string, std::vector<std::shared_ptr<Record>>> vehicleTypeIndex_;
};

} // namespace nycollision
