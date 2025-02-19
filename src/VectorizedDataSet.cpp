#include "../include/nycollision/data/VectorizedDataSet.h"
#include <algorithm>
#include <fstream>
#include <stdexcept>
#include <iostream>
#include <iterator>
#include "../include/nycollision/core/Config.h"

#ifdef _OPENMP
#include <omp.h>
#endif

namespace nycollision {

VectorizedDataSet::VectorizedDataSet() {
  config::initializeOpenMP();
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
    
    // Linear search is fine for small pools, could be optimized with a hash map for larger datasets
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
        
           std::cout << "no of threads active in loadfromfile - vectorized" << omp_get_num_threads() << " \n";


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
                    borough_index[record->getBorough()].push_back(index);
                    zip_index[record->getZipCode()].push_back(index);
                    date_index[dt].push_back(index);
                    
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
    
    #ifdef _OPENMP
    #pragma omp parallel
    {
        Records local_results;
        local_results.reserve(indices.size() / omp_get_num_threads());
          std::cout << "no of threads active in createRecordsFromIndices - vectorized" << omp_get_num_threads() << " \n";


        #pragma omp for schedule(dynamic)
        for (size_t i = 0; i < indices.size(); ++i) {
            local_results.push_back(createRecord(indices[i]));
        }
        
        #pragma omp critical
        result.insert(result.end(), local_results.begin(), local_results.end());
    }
    #else
    for (size_t i = 0; i < indices.size(); ++i) {
        result.push_back(createRecord(indices[i]));
    }
    #endif
    
    return result;
}

VectorizedDataSet::Records VectorizedDataSet::queryByGeoBounds(
    float minLat, float maxLat,
    float minLon, float maxLon
) const {
    std::vector<size_t> result_indices;
    
    #ifdef _OPENMP
    #pragma omp parallel
    {
        std::vector<size_t> local_indices;
        
        #pragma omp for schedule(dynamic)
        for (size_t cell_idx = 0; cell_idx < spatial_grid.size(); ++cell_idx) {
            const auto& cell = spatial_grid[cell_idx];
            if (cell.maxLat >= minLat && cell.minLat <= maxLat &&
                cell.maxLon >= minLon && cell.minLon <= maxLon) {
                for (size_t idx : cell.indices) {
                    if (latitudes[idx] >= minLat && latitudes[idx] <= maxLat &&
                        longitudes[idx] >= minLon && longitudes[idx] <= maxLon) {
                        local_indices.push_back(idx);
                    }
                }
            }
        }
        
        #pragma omp critical
        result_indices.insert(result_indices.end(), local_indices.begin(), local_indices.end());
    }
    #else
    for (size_t cell_idx = 0; cell_idx < spatial_grid.size(); ++cell_idx) {
        const auto& cell = spatial_grid[cell_idx];
        if (cell.maxLat >= minLat && cell.minLat <= maxLat &&
            cell.maxLon >= minLon && cell.minLon <= maxLon) {
            for (size_t idx : cell.indices) {
                if (latitudes[idx] >= minLat && latitudes[idx] <= maxLat &&
                    longitudes[idx] >= minLon && longitudes[idx] <= maxLon) {
                    result_indices.push_back(idx);
                }
            }
        }
    }
    #endif
    
    return createRecordsFromIndices(result_indices);
}

VectorizedDataSet::Records VectorizedDataSet::queryByBorough(const std::string& borough) const {
    auto it = borough_index.find(borough);
    return it != borough_index.end() ? createRecordsFromIndices(it->second) : Records{};
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

VectorizedDataSet::Records VectorizedDataSet::queryByInjuryRange(
    int minInjuries,
    int maxInjuries
) const {
    std::vector<size_t> result_indices;
    auto startIt = injury_index.lower_bound(minInjuries);
    auto endIt = injury_index.upper_bound(maxInjuries);
    
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
