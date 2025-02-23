#pragma once
#include "IDataSet.h"
#include "../parser/IParser.h"
#include "../core/Record.h"
#include <unordered_map>
#include <map>
#include <memory>
#include <string>
#include <fstream>
#include <stdexcept>
#include <chrono>
#include <iostream>
#include <iomanip>
#include <omp.h>
#include "../core/Config.h"


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
    // void loadFromFile(const std::string& filename, const IParser& parser) {
    //     std::ifstream file(filename);
    //     if (!file) {
    //         throw std::runtime_error("Failed to open file: " + filename);
    //     }

    //     std::string line;
    //     // Skip header line
    //     std::getline(file, line);
    //      auto startTime = std::chrono::high_resolution_clock::now();
    //     while (std::getline(file, line)) {
    //         if (auto record = parser.parseRecord(line)) {
    //             addRecord(record);
    //         }
    //     }
    //     auto endTime = std::chrono::high_resolution_clock::now();

    //     std::chrono::duration<double> elapsedSeconds = endTime - startTime;
    //     std::cout << "load from file internal took " << elapsedSeconds.count() << " seconds.\n";
    // }

    void loadFromFile(const std::string& filename, const IParser& parser) {
    auto startTime = std::chrono::high_resolution_clock::now();

    std::ifstream file(filename);
    if (!file) {
        throw std::runtime_error("Failed to open file: " + filename);
    }

    std::string line;
    // Skip header line
    if (!std::getline(file, line)) {
        // Handle empty file or missing header if necessary
        return;
    }
    // Read all lines into memory
    std::vector<std::string> lines;
    lines.reserve(2'700'000); // optional: reserve enough space for 2 million lines
    while (std::getline(file, line)) {
        lines.push_back(std::move(line));
    }

    // Create a temporary vector to hold parsed records
    std::vector<std::shared_ptr<Record>> parsedRecords(lines.size());

      #ifdef _OPENMP
    #pragma omp parallel
    {
        #pragma omp master
        {
         //  std::cout << "Inside normal approach record loading parallel region: " 
             //         << omp_get_num_threads() 
             //         << " threads\n";
        }
    }
     #endif

    // Parallel parse each line
    #pragma omp parallel for
    for (std::size_t i = 0; i < lines.size(); ++i) {
        parsedRecords[i] = parser.parseRecord(lines[i]);
    }

    // Sequentially add parsed records to your data structure since not thread safe
    for (auto& rec : parsedRecords) {
        if (rec) {
            addRecord(rec);
        }
    }

    // auto endTime = std::chrono::high_resolution_clock::now();
    // std::chrono::duration<double> elapsedSeconds = endTime - startTime;
    // std::cout << "loadFromFile took " << elapsedSeconds.count() << " seconds.\n";
}

    // IDataSet interface implementation
    Records queryByGeoBounds(
        float minLat, float maxLat,
        float minLon, float maxLon
    ) const override;

    Records queryByZipCode(const std::string& zipCode) const override;
    Records queryByDateRange(const Date& start, const Date& end) const override;
    Records queryByVehicleType(const std::string& vehicleType) const override;
    Records queryByInjuryRange(int minInjuries, int maxInjuries) const override;
    Records queryByFatalityRange(int minFatalities, int maxFatalities) const override;
    RecordPtr queryByUniqueKey(int key) const override;
    Records queryByPedestrianFatalities(int minFatalities, int maxFatalities) const override;
    Records queryByCyclistFatalities(int minFatalities, int maxFatalities) const override;
    Records queryByMotoristFatalities(int minFatalities, int maxFatalities) const override;
    Records queryByBorough(const std::string& borough) const override;
    size_t countByBorough(const std::string& borough) const override;
    
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

  

     
 

        Records result(records.size());
        
        auto startTime = std::chrono::high_resolution_clock::now();

        #pragma omp parallel for
        for (std::size_t i = 0; i < records.size(); ++i) {
           result[i] = std::static_pointer_cast<const IRecord>(records[i]);
        }

    //  for (const auto& record : records) {
    //         result.push_back(std::static_pointer_cast<const IRecord>(record));
    //     }
        auto endTime = std::chrono::high_resolution_clock::now();

        // std::chrono::duration<double> elapsedSeconds = endTime - startTime;
        // std::cout << "convertToInterfaceRecords took " << elapsedSeconds.count() << " seconds.\n";

        // std::cout << "convertToInterfaceRecords result size is " << result.size() << "  records.\n";
        return result;
    }

       

        
    // Primary storage
    std::vector<std::shared_ptr<Record>> records_;

    // Indices for efficient querying
    std::unordered_map<int, std::shared_ptr<Record>> keyIndex_;
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
