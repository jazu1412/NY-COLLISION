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
#include <shared_mutex>
#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point.hpp>
#include <boost/geometry/geometries/box.hpp>
#include <boost/geometry/index/rtree.hpp>

namespace nycollision {

namespace bg = boost::geometry;
namespace bgi = boost::geometry::index;

/**
 * @brief Concrete implementation of collision records dataset with optimized querying
 */
class DataSet : public IDataSet {
public:
    // Type definitions for R-tree
    using Point = bg::model::point<float, 2, bg::cs::cartesian>;
    using Box = bg::model::box<Point>;
    using Value = std::pair<Point, std::shared_ptr<Record>>;
    using RTree = bgi::rtree<Value, bgi::rstar<16>>;

    // Benchmarking structure
    struct QueryStats {
        double bruteforce_time;
        double rtree_time;
        size_t result_count;
    };

    /**
     * @brief Load records from a file using the specified parser
     * @param filename Path to the data file
     * @param parser Parser implementation to use
     * @throws std::runtime_error if file cannot be opened or parsing fails
     */
    void loadFromFile(const std::string& filename, const IParser& parser) {
        auto startTime = std::chrono::high_resolution_clock::now();

        std::ifstream file(filename);
        if (!file) {
            throw std::runtime_error("Failed to open file: " + filename);
        }

        std::string line;
        // Skip header line
        if (!std::getline(file, line)) {
            return;
        }

        omp_set_num_threads(11);
        // Read all lines into memory
        std::vector<std::string> lines;
        lines.reserve(2'700'000); // Reserve space for ~2.7M lines
        while (std::getline(file, line)) {
            lines.push_back(std::move(line));
        }

        // Create a temporary vector to hold parsed records
        std::vector<std::shared_ptr<Record>> parsedRecords(lines.size());

        // Parallel parse each line
        #pragma omp parallel for
        for (std::size_t i = 0; i < lines.size(); ++i) {
            parsedRecords[i] = parser.parseRecord(lines[i]);
        }

        // Sequentially add parsed records to data structures
        for (auto& rec : parsedRecords) {
            if (rec) {
                addRecord(rec);
            }
        }

        auto endTime = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> elapsedSeconds = endTime - startTime;
        std::cout << "loadFromFile took " << elapsedSeconds.count() << " seconds.\n";
    }

    // IDataSet interface implementation
    Records queryByGeoBounds(
        float minLat, float maxLat,
        float minLon, float maxLon
    ) const override {
        return queryByGeoBoundsRTree(minLat, maxLat, minLon, maxLon);
    }

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

    // Benchmark spatial queries with different methods
    QueryStats benchmarkQuery(float minLat, float maxLat, float minLon, float maxLon) const;

public:
    // Spatial query implementations for benchmarking
    Records queryByGeoBoundsBruteForce(float minLat, float maxLat, float minLon, float maxLon) const;
    Records queryByGeoBoundsRTree(float minLat, float maxLat, float minLon, float maxLon) const;

private:
    void addRecord(std::shared_ptr<Record> record);
    
    // Helper to convert internal records to interface records
    Records convertToInterfaceRecords(const std::vector<std::shared_ptr<Record>>& records) const {
        #ifdef _OPENMP
        omp_set_num_threads(13);
        #endif

        Records result(records.size());
        auto startTime = std::chrono::high_resolution_clock::now();

        #pragma omp parallel for
        for (std::size_t i = 0; i < records.size(); ++i) {
            result[i] = std::static_pointer_cast<const IRecord>(records[i]);
        }

        auto endTime = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> elapsedSeconds = endTime - startTime;
        std::cout << "convertToInterfaceRecords took " << elapsedSeconds.count() << " seconds.\n";
        std::cout << "convertToInterfaceRecords result size is " << result.size() << " records.\n";
        return result;
    }

    // Primary storage
    std::vector<std::shared_ptr<Record>> records_;

    // R-tree spatial index
    mutable RTree rtree_;
    mutable std::shared_mutex spatial_mutex_;

    // Other indices for efficient querying
    std::unordered_map<int, std::shared_ptr<Record>> keyIndex_;
    std::unordered_map<std::string, std::vector<std::shared_ptr<Record>>> boroughIndex_;
    std::unordered_map<std::string, std::vector<std::shared_ptr<Record>>> zipIndex_;
    std::map<Date, std::vector<std::shared_ptr<Record>>> dateIndex_;
    
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
