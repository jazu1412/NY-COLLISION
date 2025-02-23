#include "nycollision/data/VectorizedDataSet.h"
#include "nycollision/core/Config.h"
#include <algorithm>
#include <fstream>
#include <stdexcept>
#include <iostream>
#include <iterator>

#ifdef _OPENMP
#include <omp.h>
#endif

namespace nycollision {

VectorizedDataSet::VectorizedDataSet() {
  config::initializeOpenMP();
}

IDataSet::Records VectorizedDataSet::queryByBorough(const std::string& borough) const {
    auto it = borough_index.find(borough);
       std::cout << "calling createfromindices in querybyborough" << " \n";

    return it != borough_index.end() ? createRecordsFromIndices(it->second) : Records{};
}

size_t VectorizedDataSet::countByBorough(const std::string& borough) const {

    #ifdef _OPENMP
    #pragma omp parallel
    {
        #pragma omp master
        {
            std::cout << "Inside vectorized borough count parallel region: " 
                      << omp_get_num_threads() 
                      << " threads\n";
        }
    }
    #endif
    size_t count = 0;
    // Iterate over the vector of borough strings
    #pragma omp parallel for reduction(+:count)
    for (size_t i = 0; i < boroughs.size(); ++i) {
        if (boroughs[i] == borough) {
            count++;
        }
    }
    std::cout << "Count for borough in vectorized " << borough << " is " << count << "\n";
    return count;
}

namespace {
    // Initialize spatial grid with default coverage of NYC area
    std::vector<VectorizedDataSet::GridCell> createSpatialGrid() {
        std::vector<VectorizedDataSet::GridCell> grid;
        
        // NYC approximate bounds
        const float MIN_LAT = 40.4774f;
        const float MAX_LAT = 40.9176f;
        const float MIN_LON = -74.2591f;
        const float MAX_LON = -73.7004f;
        
        // Create a 10x10 grid
        const int GRID_SIZE = 10;
        const float LAT_STEP = (MAX_LAT - MIN_LAT) / GRID_SIZE;
        const float LON_STEP = (MAX_LON - MIN_LON) / GRID_SIZE;
        
        grid.reserve(GRID_SIZE * GRID_SIZE);
        
        #ifdef _OPENMP
        #pragma omp parallel for collapse(2)
        #endif
        for (int i = 0; i < GRID_SIZE; ++i) {
            for (int j = 0; j < GRID_SIZE; ++j) {
                VectorizedDataSet::GridCell cell;
                cell.minLat = MIN_LAT + (i * LAT_STEP);
                cell.maxLat = MIN_LAT + ((i + 1) * LAT_STEP);
                cell.minLon = MIN_LON + (j * LON_STEP);
                cell.maxLon = MIN_LON + ((j + 1) * LON_STEP);
                #ifdef _OPENMP
                #pragma omp critical
                #endif
                grid.push_back(cell);
            }
        }
        
        return grid;
    }
}

size_t VectorizedDataSet::addToStringPool(const std::string& str, std::vector<std::string>& pool) {
    if (str.empty()) return static_cast<size_t>(-1);
    
   
    auto it = std::find(pool.begin(), pool.end(), str);
    if (it != pool.end()) {
        return std::distance(pool.begin(), it);
    }
    
    size_t index;
    #ifdef _OPENMP
    #pragma omp critical
    #endif
    {
        index = pool.size();
        pool.push_back(str);
    }
    return index;
}

void VectorizedDataSet::loadFromFile(const std::string& filename, const IParser& parser) {
    std::ifstream file(filename);
    if (!file) {
        throw std::runtime_error("Failed to open file: " + filename);
    }

    std::string line;
    // Skip header line
    std::getline(file, line);
    
    // First pass: count lines to pre-allocate vectors
    size_t lineCount = 0;
    while (std::getline(file, line)) {
        ++lineCount;
    }
    
    // Pre-allocate vectors
    unique_keys.reserve(lineCount);
    boroughs.reserve(lineCount);
    zip_codes.reserve(lineCount);
    latitudes.reserve(lineCount);
    longitudes.reserve(lineCount);
    on_streets.reserve(lineCount);
    cross_streets.reserve(lineCount);
    off_streets.reserve(lineCount);
    dates.reserve(lineCount);
    times.reserve(lineCount);
    persons_injured.reserve(lineCount);
    persons_killed.reserve(lineCount);
    pedestrians_injured.reserve(lineCount);
    pedestrians_killed.reserve(lineCount);
    cyclists_injured.reserve(lineCount);
    cyclists_killed.reserve(lineCount);
    motorists_injured.reserve(lineCount);
    motorists_killed.reserve(lineCount);
    vehicle_type_indices.reserve(lineCount);
    contributing_factor_indices.reserve(lineCount);
    
    // Reset file position and skip header again
    file.clear();
    file.seekg(0);
    std::getline(file, line);
    
    // Second pass: parse records
    std::vector<std::string> lines;
    while (std::getline(file, line)) {
        lines.push_back(line);
    }
    
    // Parse records in parallel
    #ifdef _OPENMP
    #pragma omp parallel
    {
        std::vector<size_t> local_indices;
        local_indices.reserve(lines.size() / omp_get_num_threads());
        
       // std::cout << "no of threads active in loadfromfile - vectorized" << omp_get_num_threads() << " \n";

        #pragma omp for schedule(dynamic)
    #endif
        for (size_t i = 0; i < lines.size(); ++i) {
            if (auto record = parser.parseRecord(lines[i])) {
                size_t index;
                auto dt = record->getDateTime();
                const auto& stats = record->getCasualtyStats();
                
                #ifdef _OPENMP
                #pragma omp critical
                #endif
                {
                    index = unique_keys.size();
                    // Add to vectorized storage
                    unique_keys.push_back(record->getUniqueKey());
                    boroughs.push_back(record->getBorough());
                    zip_codes.push_back(record->getZipCode());
                    auto loc = record->getLocation();
                    latitudes.push_back(loc.latitude);
                    longitudes.push_back(loc.longitude);
                    on_streets.push_back(record->getOnStreet());
                    cross_streets.push_back(record->getCrossStreet());
                    off_streets.push_back(record->getOffStreet());
                    dates.push_back(dt.date);
                    times.push_back(dt.time);
                    persons_injured.push_back(stats.persons_injured);
                    persons_killed.push_back(stats.persons_killed);
                    pedestrians_injured.push_back(stats.pedestrians_injured);
                    pedestrians_killed.push_back(stats.pedestrians_killed);
                    cyclists_injured.push_back(stats.cyclists_injured);
                    cyclists_killed.push_back(stats.cyclists_killed);
                    motorists_injured.push_back(stats.motorists_injured);
                    motorists_killed.push_back(stats.motorists_killed);
                }
                
                // Process vehicle data
                const auto& vehicleInfo = record->getVehicleInfo();
                std::vector<size_t> type_indices;
                for (const auto& type : vehicleInfo.vehicle_types) {
                    type_indices.push_back(addToStringPool(type, vehicle_type_pool));
                }
                
                std::vector<size_t> factor_indices;
                for (const auto& factor : vehicleInfo.contributing_factors) {
                    factor_indices.push_back(addToStringPool(factor, contributing_factor_pool));
                }
                
                #ifdef _OPENMP
                #pragma omp critical
                #endif
                {
                    vehicle_type_indices.push_back(type_indices);
                    contributing_factor_indices.push_back(factor_indices);
                    
                    // Update indices
                    key_to_index[record->getUniqueKey()] = index;
                    zip_index[record->getZipCode()].push_back(index);
                    date_index[dt].push_back(index);
                    borough_index[record->getBorough()].push_back(index);
                    
                    // Update range indices
                    injury_index[stats.getTotalInjuries()].push_back(index);
                    fatality_index[stats.getTotalFatalities()].push_back(index);
                    pedestrian_fatality_index[stats.pedestrians_killed].push_back(index);
                    cyclist_fatality_index[stats.cyclists_killed].push_back(index);
                    motorist_fatality_index[stats.motorists_killed].push_back(index);
                    
                    // Update vehicle type index
                    for (const auto& type : vehicleInfo.vehicle_types) {
                        if (!type.empty()) {
                            vehicle_type_index[type].push_back(index);
                        }
                    }
                }
                
                #ifdef _OPENMP
                local_indices.push_back(index);
                #endif
            }
        }
    #ifdef _OPENMP
    }
    #endif
    
    // Initialize spatial grid
    spatial_grid = createSpatialGrid();
    
    // Populate spatial grid in parallel
    #ifdef _OPENMP
    #pragma omp parallel for
    #endif
    for (size_t i = 0; i < latitudes.size(); ++i) {
        for (auto& cell : spatial_grid) {
            if (latitudes[i] >= cell.minLat && latitudes[i] <= cell.maxLat &&
                longitudes[i] >= cell.minLon && longitudes[i] <= cell.maxLon) {
                #ifdef _OPENMP
                #pragma omp critical
                #endif
                cell.indices.push_back(i);
                break;
            }
        }
    }
}

std::shared_ptr<Record> VectorizedDataSet::createRecord(size_t index) const {
    auto record = std::make_shared<Record>();
    
    record->setUniqueKey(unique_keys[index]);
    record->setBorough(boroughs[index]);
    record->setZipCode(zip_codes[index]);
    
    GeoCoordinate loc{latitudes[index], longitudes[index]};
    record->setLocation(loc);
    
    record->setOnStreet(on_streets[index]);
    record->setCrossStreet(cross_streets[index]);
    record->setOffStreet(off_streets[index]);
    
    Date dt{dates[index], times[index]};
    record->setDateTime(dt);
    
    CasualtyStats stats;
    stats.persons_injured = persons_injured[index];
    stats.persons_killed = persons_killed[index];
    stats.pedestrians_injured = pedestrians_injured[index];
    stats.pedestrians_killed = pedestrians_killed[index];
    stats.cyclists_injured = cyclists_injured[index];
    stats.cyclists_killed = cyclists_killed[index];
    stats.motorists_injured = motorists_injured[index];
    stats.motorists_killed = motorists_killed[index];
    record->setCasualtyStats(stats);
    
    VehicleInfo vehicleInfo;
    for (size_t type_idx : vehicle_type_indices[index]) {
        if (type_idx != static_cast<size_t>(-1)) {
            vehicleInfo.vehicle_types.push_back(vehicle_type_pool[type_idx]);
        }
    }
    for (size_t factor_idx : contributing_factor_indices[index]) {
        if (factor_idx != static_cast<size_t>(-1)) {
            vehicleInfo.contributing_factors.push_back(contributing_factor_pool[factor_idx]);
        }
    }
    record->setVehicleInfo(vehicleInfo);
    
    return record;
}

VectorizedDataSet::Records VectorizedDataSet::createRecordsFromIndices(
    const std::vector<size_t>& indices
) const {
    Records result;
    result.reserve(indices.size());
    
    // Use a single parallel region for better efficiency
    #ifdef _OPENMP
    std::vector<std::shared_ptr<Record>> temp_results(indices.size());
    #pragma omp parallel
   {
     #pragma omp for nowait
     for (size_t i = 0; i < indices.size(); ++i) {
        temp_results[i] = createRecord(indices[i]);
      }
   }
    result.insert(result.end(), 
                 std::make_move_iterator(temp_results.begin()),
                 std::make_move_iterator(temp_results.end()));
    #else
    for (size_t idx : indices) {
        result.push_back(createRecord(idx));
    }
    #endif
    
    return result;
}

VectorizedDataSet::Records VectorizedDataSet::queryByGeoBounds(
    float minLat, float maxLat,
    float minLon, float maxLon
) const {
    std::vector<size_t> result_indices;
    const size_t size = latitudes.size();
    result_indices.reserve(size / 4); // Reserve some space to avoid reallocations
    
    #ifdef _OPENMP
    #pragma omp parallel
    {
        std::vector<size_t> local_indices;
        local_indices.reserve(size / (4 * omp_get_num_threads()));
        
        #pragma omp for simd schedule(static)
        for (size_t i = 0; i < size; ++i) {
            if (latitudes[i] >= minLat && latitudes[i] <= maxLat &&
                longitudes[i] >= minLon && longitudes[i] <= maxLon) {
                local_indices.push_back(i);
            }
        }
        
        #pragma omp critical
        result_indices.insert(result_indices.end(), local_indices.begin(), local_indices.end());
    }
    #else
    for (size_t i = 0; i < size; ++i) {
        if (latitudes[i] >= minLat && latitudes[i] <= maxLat &&
            longitudes[i] >= minLon && longitudes[i] <= maxLon) {
            result_indices.push_back(i);
        }
    }
    #endif
    
    return createRecordsFromIndices(result_indices);
}

VectorizedDataSet::Records VectorizedDataSet::queryByZipCode(const std::string& zipCode) const {
    auto it = zip_index.find(zipCode);
    return it != zip_index.end() ? createRecordsFromIndices(it->second) : Records{};
}

VectorizedDataSet::Records VectorizedDataSet::queryByDateRange(
    const Date& start,
    const Date& end
) const {
    std::vector<size_t> result_indices;
    auto startIt = date_index.lower_bound(start);
    auto endIt = date_index.upper_bound(end);
    
    // Compute range size properly
    size_t total_size = std::distance(startIt, endIt);
    
    #ifdef _OPENMP
    #pragma omp parallel
    {
        std::vector<size_t> local_indices;
        
        // Corrected OpenMP syntax
        #pragma omp for schedule(dynamic)
        for (size_t i = 0; i < total_size; ++i) {
            auto it = std::next(startIt, i);  // Safely advance iterator
            local_indices.insert(local_indices.end(),
                               it->second.begin(),
                               it->second.end());
        }
        
        #pragma omp critical
        result_indices.insert(result_indices.end(),
                            local_indices.begin(),
                            local_indices.end());
    }
    #else
    for (auto it = startIt; it != endIt; ++it) {
        result_indices.insert(result_indices.end(),
                            it->second.begin(),
                            it->second.end());
    }
    #endif
    
    return createRecordsFromIndices(result_indices);
}

VectorizedDataSet::Records VectorizedDataSet::queryByVehicleType(
    const std::string& vehicleType
) const {
    auto it = vehicle_type_index.find(vehicleType);
    return it != vehicle_type_index.end() ? createRecordsFromIndices(it->second) : Records{};
}

VectorizedDataSet::Records VectorizedDataSet::queryByInjuryRange(int minInjuries, int maxInjuries) const
{
    size_t n = persons_injured.size();

    // Step 1: Compute a mask array (1 if condition is met, 0 otherwise)
    std::vector<int> mask(n, 0);
    #pragma omp simd
    for (size_t i = 0; i < n; ++i) {
        mask[i] = (persons_injured[i] >= minInjuries && persons_injured[i] <= maxInjuries) ? 1 : 0;
    }

    // Step 2: Compute an exclusive prefix sum on the mask.
    // We'll implement our own exclusive scan inline.
    std::vector<size_t> prefix(n, 0);
    {
        size_t sum = 0;
        for (size_t i = 0; i < n; ++i) {
            prefix[i] = sum;
            sum += mask[i];
        }
    }
    
    // Total number of matching elements.
    size_t count = (n > 0 ? prefix[n - 1] + mask[n - 1] : 0);
    std::vector<size_t> resultIndices(count);

    // Step 3: Scatter the matching indices into the result array.
    // For each index i, if mask[i] is 1, prefix[i] gives the output position.
    #pragma omp simd
    for (size_t i = 0; i < n; ++i) {
        if (mask[i])
            resultIndices[prefix[i]] = i;
    }

    return createRecordsFromIndices(resultIndices);
}

VectorizedDataSet::Records VectorizedDataSet::queryByFatalityRange(
    int minFatalities,
    int maxFatalities
) const {
    std::vector<size_t> result_indices;
    auto startIt = fatality_index.lower_bound(minFatalities);
    auto endIt = fatality_index.upper_bound(maxFatalities);
    
    size_t total_size = std::distance(startIt, endIt);
    
    #ifdef _OPENMP
    #pragma omp parallel
    {
        std::vector<size_t> local_indices;
        
        #pragma omp for schedule(dynamic)
        for (size_t i = 0; i < total_size; ++i) {
            auto it = std::next(startIt, i);
            local_indices.insert(local_indices.end(),
                               it->second.begin(),
                               it->second.end());
        }
        
        #pragma omp critical
        result_indices.insert(result_indices.end(),
                            local_indices.begin(),
                            local_indices.end());
    }
    #else
    for (auto it = startIt; it != endIt; ++it) {
        result_indices.insert(result_indices.end(),
                            it->second.begin(),
                            it->second.end());
    }
    #endif
    
    return createRecordsFromIndices(result_indices);
}

VectorizedDataSet::RecordPtr VectorizedDataSet::queryByUniqueKey(int key) const {
    auto it = key_to_index.find(key);
    return it != key_to_index.end() ? createRecord(it->second) : nullptr;
}

VectorizedDataSet::Records VectorizedDataSet::queryByPedestrianFatalities(
    int minFatalities,
    int maxFatalities
) const {
    std::vector<size_t> result_indices;
    auto startIt = pedestrian_fatality_index.lower_bound(minFatalities);
    auto endIt = pedestrian_fatality_index.upper_bound(maxFatalities);
    
    size_t total_size = std::distance(startIt, endIt);
    
    #ifdef _OPENMP
    #pragma omp parallel
    {
        std::vector<size_t> local_indices;
        
        #pragma omp for schedule(dynamic)
        for (size_t i = 0; i < total_size; ++i) {
            auto it = std::next(startIt, i);
            local_indices.insert(local_indices.end(),
                               it->second.begin(),
                               it->second.end());
        }
        
        #pragma omp critical
        result_indices.insert(result_indices.end(),
                            local_indices.begin(),
                            local_indices.end());
    }
    #else
    for (auto it = startIt; it != endIt; ++it) {
        result_indices.insert(result_indices.end(),
                            it->second.begin(),
                            it->second.end());
    }
    #endif
    
    return createRecordsFromIndices(result_indices);
}

VectorizedDataSet::Records VectorizedDataSet::queryByCyclistFatalities(
    int minFatalities,
    int maxFatalities
) const {
    std::vector<size_t> result_indices;
    auto startIt = cyclist_fatality_index.lower_bound(minFatalities);
    auto endIt = cyclist_fatality_index.upper_bound(maxFatalities);
    
    size_t total_size = std::distance(startIt, endIt);
    
    #ifdef _OPENMP
    #pragma omp parallel
    {
        std::vector<size_t> local_indices;
        
        #pragma omp for schedule(dynamic)
        for (size_t i = 0; i < total_size; ++i) {
            auto it = std::next(startIt, i);
            local_indices.insert(local_indices.end(),
                               it->second.begin(),
                               it->second.end());
        }
        
        #pragma omp critical
        result_indices.insert(result_indices.end(),
                            local_indices.begin(),
                            local_indices.end());
    }
    #else
    for (auto it = startIt; it != endIt; ++it) {
        result_indices.insert(result_indices.end(),
                            it->second.begin(),
                            it->second.end());
    }
    #endif
    
    return createRecordsFromIndices(result_indices);
}

VectorizedDataSet::Records VectorizedDataSet::queryByMotoristFatalities(
    int minFatalities,
    int maxFatalities
) const {
    std::vector<size_t> result_indices;
    auto startIt = motorist_fatality_index.lower_bound(minFatalities);
    auto endIt = motorist_fatality_index.upper_bound(maxFatalities);
    
    size_t total_size = std::distance(startIt, endIt);
    
    #ifdef _OPENMP
    #pragma omp parallel
    {
        std::vector<size_t> local_indices;
        
        #pragma omp for schedule(dynamic)
        for (size_t i = 0; i < total_size; ++i) {
            auto it = std::next(startIt, i);
            local_indices.insert(local_indices.end(),
                               it->second.begin(),
                               it->second.end());
        }
        
        #pragma omp critical
        result_indices.insert(result_indices.end(),
                            local_indices.begin(),
                            local_indices.end());
    }
    #else
    for (auto it = startIt; it != endIt; ++it) {
        result_indices.insert(result_indices.end(),
                            it->second.begin(),
                            it->second.end());
    }
    #endif
    
    return createRecordsFromIndices(result_indices);
}

} // namespace nycollision
