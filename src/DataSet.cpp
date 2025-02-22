#include "../include/nycollision/data/DataSet.h"
#include <algorithm>

namespace nycollision {

void DataSet::addRecord(std::shared_ptr<Record> record) {
    // Add to primary storage
    records_.push_back(record);
    
    // Update R-tree index with thread safety
    {
        std::unique_lock lock(spatial_mutex_);
        auto loc = record->getLocation();
        Point p(loc.latitude, loc.longitude);
        rtree_.insert(std::make_pair(p, record));
    }
    
    // Update other indices
    keyIndex_[record->getUniqueKey()] = record;
    boroughIndex_[record->getBorough()].push_back(record);
    zipIndex_[record->getZipCode()].push_back(record);
    dateIndex_[record->getDateTime()].push_back(record);
    
    // Update range indices
    const auto& stats = record->getCasualtyStats();
    injuryIndex_[stats.getTotalInjuries()].push_back(record);
    fatalityIndex_[stats.getTotalFatalities()].push_back(record);
    pedestrianFatalityIndex_[stats.pedestrians_killed].push_back(record);
    cyclistFatalityIndex_[stats.cyclists_killed].push_back(record);
    motoristFatalityIndex_[stats.motorists_killed].push_back(record);
    
    // Update vehicle type index
    const auto& vehicleInfo = record->getVehicleInfo();
    for (const auto& type : vehicleInfo.vehicle_types) {
        vehicleTypeIndex_[type].push_back(record);
    }
}

DataSet::Records DataSet::queryByGeoBoundsBruteForce(
    float minLat, float maxLat,
    float minLon, float maxLon
) const {
    std::vector<std::shared_ptr<Record>> result;
    
    for (const auto& record : records_) {
        auto loc = record->getLocation();
        if (loc.latitude >= minLat && loc.latitude <= maxLat &&
            loc.longitude >= minLon && loc.longitude <= maxLon) {
            result.push_back(record);
        }
    }
    
    return convertToInterfaceRecords(result);
}

DataSet::Records DataSet::queryByGeoBoundsRTree(
    float minLat, float maxLat,
    float minLon, float maxLon
) const {
    std::vector<std::shared_ptr<Record>> result;
    
    // Create bounding box for query
    Point min_point(minLat, minLon);
    Point max_point(maxLat, maxLon);
    Box query_box(min_point, max_point);
    
    // Query R-tree with thread safety
    {
        std::shared_lock lock(spatial_mutex_);
        
        // Perform spatial query
        std::vector<Value> found_values;
        rtree_.query(bgi::intersects(query_box), std::back_inserter(found_values));
        
        // Extract records from query results
        result.reserve(found_values.size());
        for (const auto& value : found_values) {
            result.push_back(value.second);
        }
    }
    
    return convertToInterfaceRecords(result);
}

DataSet::QueryStats DataSet::benchmarkQuery(
    float minLat, float maxLat,
    float minLon, float maxLon
) const {
    QueryStats stats;
    
    // Benchmark brute force method
    {
        auto start = std::chrono::high_resolution_clock::now();
        auto results = queryByGeoBoundsBruteForce(minLat, maxLat, minLon, maxLon);
        auto end = std::chrono::high_resolution_clock::now();
        
        std::chrono::duration<double> elapsed = end - start;
        stats.bruteforce_time = elapsed.count();
        stats.result_count = results.size();
    }
    
    // Benchmark R-tree method
    {
        auto start = std::chrono::high_resolution_clock::now();
        auto results = queryByGeoBoundsRTree(minLat, maxLat, minLon, maxLon);
        auto end = std::chrono::high_resolution_clock::now();
        
        std::chrono::duration<double> elapsed = end - start;
        stats.rtree_time = elapsed.count();
        
        // Verify result counts match
        if (results.size() != stats.result_count) {
            std::cerr << "Warning: Result count mismatch between methods!\n"
                      << "Brute force: " << stats.result_count << "\n"
                      << "R-tree: " << results.size() << "\n";
        }
    }
    
    return stats;
}

DataSet::Records DataSet::queryByBorough(const std::string& borough) const {
    auto it = boroughIndex_.find(borough);
    return it != boroughIndex_.end() ? convertToInterfaceRecords(it->second) : Records{};
}

DataSet::Records DataSet::queryByZipCode(const std::string& zipCode) const {
    auto it = zipIndex_.find(zipCode);
    return it != zipIndex_.end() ? convertToInterfaceRecords(it->second) : Records{};
}

DataSet::Records DataSet::queryByDateRange(const Date& start, const Date& end) const {
    std::vector<std::shared_ptr<Record>> result;
    auto startIt = dateIndex_.lower_bound(start);
    auto endIt = dateIndex_.upper_bound(end);
    
    for (auto it = startIt; it != endIt; ++it) {
        result.insert(result.end(), it->second.begin(), it->second.end());
    }
    return convertToInterfaceRecords(result);
}

DataSet::Records DataSet::queryByVehicleType(const std::string& vehicleType) const {
    auto it = vehicleTypeIndex_.find(vehicleType);
    return it != vehicleTypeIndex_.end() ? convertToInterfaceRecords(it->second) : Records{};
}

DataSet::Records DataSet::queryByInjuryRange(int minInjuries, int maxInjuries) const {
    std::vector<std::shared_ptr<Record>> result;
    auto startIt = injuryIndex_.lower_bound(minInjuries);
    auto endIt = injuryIndex_.upper_bound(maxInjuries);
    
    for (auto it = startIt; it != endIt; ++it) {
        result.insert(result.end(), it->second.begin(), it->second.end());
    }
    return convertToInterfaceRecords(result);
}

DataSet::Records DataSet::queryByFatalityRange(int minFatalities, int maxFatalities) const {
    std::vector<std::shared_ptr<Record>> result;
    auto startIt = fatalityIndex_.lower_bound(minFatalities);
    auto endIt = fatalityIndex_.upper_bound(maxFatalities);
    
    for (auto it = startIt; it != endIt; ++it) {
        result.insert(result.end(), it->second.begin(), it->second.end());
    }
    return convertToInterfaceRecords(result);
}

DataSet::RecordPtr DataSet::queryByUniqueKey(int key) const {
    auto it = keyIndex_.find(key);
    return it != keyIndex_.end() ? std::static_pointer_cast<const IRecord>(it->second) : nullptr;
}

DataSet::Records DataSet::queryByPedestrianFatalities(int minFatalities, int maxFatalities) const {
    std::vector<std::shared_ptr<Record>> result;
    auto startIt = pedestrianFatalityIndex_.lower_bound(minFatalities);
    auto endIt = pedestrianFatalityIndex_.upper_bound(maxFatalities);
    
    for (auto it = startIt; it != endIt; ++it) {
        result.insert(result.end(), it->second.begin(), it->second.end());
    }
    return convertToInterfaceRecords(result);
}

DataSet::Records DataSet::queryByCyclistFatalities(int minFatalities, int maxFatalities) const {
    std::vector<std::shared_ptr<Record>> result;
    auto startIt = cyclistFatalityIndex_.lower_bound(minFatalities);
    auto endIt = cyclistFatalityIndex_.upper_bound(maxFatalities);
    
    for (auto it = startIt; it != endIt; ++it) {
        result.insert(result.end(), it->second.begin(), it->second.end());
    }
    return convertToInterfaceRecords(result);
}

DataSet::Records DataSet::queryByMotoristFatalities(int minFatalities, int maxFatalities) const {
    std::vector<std::shared_ptr<Record>> result;
    auto startIt = motoristFatalityIndex_.lower_bound(minFatalities);
    auto endIt = motoristFatalityIndex_.upper_bound(maxFatalities);
    
    for (auto it = startIt; it != endIt; ++it) {
        result.insert(result.end(), it->second.begin(), it->second.end());
    }
    return convertToInterfaceRecords(result);
}













} // namespace nycollision
