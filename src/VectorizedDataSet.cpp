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
        // NYC approximate bounds
        const float MIN_LAT = 40.4774f;
        const float MAX_LAT = 40.9176f;
        const float MIN_LON = -74.2591f;
        const float MAX_LON = -73.7004f;
        
        // Create a 10x10 grid
        const int GRID_SIZE = 10;
        const float LAT_STEP = (MAX_LAT - MIN_LAT) / GRID_SIZE;
        const float LON_STEP = (MAX_LON - MIN_LON) / GRID_SIZE;
        
        // Pre-allocate grid with exact size
        std::vector<VectorizedDataSet::GridCell> grid(GRID_SIZE * GRID_SIZE);
        
        #ifdef _OPENMP
        #pragma omp parallel for collapse(2)
        #endif
        for (int i = 0; i < GRID_SIZE; ++i) {
            for (int j = 0; j < GRID_SIZE; ++j) {
                const size_t idx = i * GRID_SIZE + j;
                grid[idx].minLat = MIN_LAT + (i * LAT_STEP);
                grid[idx].maxLat = MIN_LAT + ((i + 1) * LAT_STEP);
                grid[idx].minLon = MIN_LON + (j * LON_STEP);
                grid[idx].maxLon = MIN_LON + ((j + 1) * LON_STEP);
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
    std::getline(file, line); // Skip header

    // Read all lines first
    std::vector<std::string> lines;
    lines.reserve(2'700'000); // Reserve space for approximately 2.7M records
    while (std::getline(file, line)) {
        lines.push_back(std::move(line));
    }

    const size_t lineCount = lines.size();

    // Pre-allocate all vectors to exact size
    unique_keys.resize(lineCount);
    
    // Location data
    location_data.latitudes.resize(lineCount);
    location_data.longitudes.resize(lineCount);
    location_data.boroughs.resize(lineCount);
    location_data.zip_codes.resize(lineCount);
    location_data.on_streets.resize(lineCount);
    location_data.cross_streets.resize(lineCount);
    location_data.off_streets.resize(lineCount);
    
    // Time data
    time_data.dates.resize(lineCount);
    time_data.times.resize(lineCount);
    
    // Casualty data
    casualty_data.persons_injured.resize(lineCount);
    casualty_data.persons_killed.resize(lineCount);
    casualty_data.pedestrians_injured.resize(lineCount);
    casualty_data.pedestrians_killed.resize(lineCount);
    casualty_data.cyclists_injured.resize(lineCount);
    casualty_data.cyclists_killed.resize(lineCount);
    casualty_data.motorists_injured.resize(lineCount);
    casualty_data.motorists_killed.resize(lineCount);
    
    // Vehicle data
    vehicle_data.type_indices.resize(lineCount);
    vehicle_data.factor_indices.resize(lineCount);

    // Use unordered_map for string pooling
    std::unordered_map<std::string, size_t> vehicle_type_map;
    std::unordered_map<std::string, size_t> contributing_factor_map;
    vehicle_type_map.reserve(1000);  // Reserve space for estimated unique types
    contributing_factor_map.reserve(1000);

    // First pass: Parse records and fill arrays
    #ifdef _OPENMP
    #pragma omp parallel
    {
        // Thread-local maps for string pooling
        std::unordered_map<std::string, size_t> local_vehicle_type_map;
        std::unordered_map<std::string, size_t> local_contributing_factor_map;
        local_vehicle_type_map.reserve(1000);
        local_contributing_factor_map.reserve(1000);

        #pragma omp for schedule(dynamic, 1000)
    #endif
        for (size_t i = 0; i < lines.size(); ++i) {
            if (auto record = parser.parseRecord(lines[i])) {
                // Store basic data
                unique_keys[i] = record->getUniqueKey();
                
                // Location data
                auto loc = record->getLocation();
                location_data.latitudes[i] = loc.latitude;
                location_data.longitudes[i] = loc.longitude;
                location_data.boroughs[i] = record->getBorough();
                location_data.zip_codes[i] = record->getZipCode();
                location_data.on_streets[i] = record->getOnStreet();
                location_data.cross_streets[i] = record->getCrossStreet();
                location_data.off_streets[i] = record->getOffStreet();
                
                // Time data
                auto dt = record->getDateTime();
                time_data.dates[i] = dt.date;
                time_data.times[i] = dt.time;
                
                // Casualty data
                const auto& stats = record->getCasualtyStats();
                casualty_data.persons_injured[i] = stats.persons_injured;
                casualty_data.persons_killed[i] = stats.persons_killed;
                casualty_data.pedestrians_injured[i] = stats.pedestrians_injured;
                casualty_data.pedestrians_killed[i] = stats.pedestrians_killed;
                casualty_data.cyclists_injured[i] = stats.cyclists_injured;
                casualty_data.cyclists_killed[i] = stats.cyclists_killed;
                casualty_data.motorists_injured[i] = stats.motorists_injured;
                casualty_data.motorists_killed[i] = stats.motorists_killed;

                // Process vehicle data with thread-local maps
                const auto& vehicleInfo = record->getVehicleInfo();
                std::vector<size_t>& type_indices = vehicle_data.type_indices[i];
                std::vector<size_t>& factor_indices = vehicle_data.factor_indices[i];
                
                type_indices.reserve(vehicleInfo.vehicle_types.size());
                factor_indices.reserve(vehicleInfo.contributing_factors.size());

                #ifdef _OPENMP
                auto& type_map = local_vehicle_type_map;
                auto& factor_map = local_contributing_factor_map;
                #else
                auto& type_map = vehicle_type_map;
                auto& factor_map = contributing_factor_map;
                #endif

                for (const auto& type : vehicleInfo.vehicle_types) {
                    if (!type.empty()) {
                        auto [it, inserted] = type_map.try_emplace(type, type_map.size());
                        type_indices.push_back(it->second);
                    }
                }

                for (const auto& factor : vehicleInfo.contributing_factors) {
                    if (!factor.empty()) {
                        auto [it, inserted] = factor_map.try_emplace(factor, factor_map.size());
                        factor_indices.push_back(it->second);
                    }
                }
            }
        }

    #ifdef _OPENMP
        // Merge thread-local maps into global maps
        #pragma omp critical
        {
            for (const auto& [str, idx] : local_vehicle_type_map) {
                if (vehicle_type_map.find(str) == vehicle_type_map.end()) {
                    vehicle_type_map[str] = vehicle_data.type_pool.size();
                    vehicle_data.type_pool.push_back(str);
                }
            }
            for (const auto& [str, idx] : local_contributing_factor_map) {
                if (contributing_factor_map.find(str) == contributing_factor_map.end()) {
                    contributing_factor_map[str] = vehicle_data.factor_pool.size();
                    vehicle_data.factor_pool.push_back(str);
                }
            }
        }
    }
    #endif

    // Build indices using thread-local storage
    #ifdef _OPENMP
    const size_t num_threads = omp_get_max_threads();
    std::vector<std::unordered_map<int, size_t>> thread_key_indices(num_threads);
    std::vector<std::unordered_map<std::string, std::vector<size_t>>> thread_borough_indices(num_threads);
    std::vector<std::unordered_map<std::string, std::vector<size_t>>> thread_zip_indices(num_threads);
    std::vector<std::map<Date, std::vector<size_t>>> thread_date_indices(num_threads);
    std::vector<std::map<int, std::vector<size_t>>> thread_injury_indices(num_threads);
    std::vector<std::map<int, std::vector<size_t>>> thread_fatality_indices(num_threads);
    std::vector<std::map<int, std::vector<size_t>>> thread_ped_fatality_indices(num_threads);
    std::vector<std::map<int, std::vector<size_t>>> thread_cyclist_fatality_indices(num_threads);
    std::vector<std::map<int, std::vector<size_t>>> thread_motorist_fatality_indices(num_threads);
    std::vector<std::unordered_map<std::string, std::vector<size_t>>> thread_vehicle_type_indices(num_threads);

    #pragma omp parallel
    {
        const int thread_id = omp_get_thread_num();
        
        #pragma omp for schedule(static, 10000)
        for (size_t i = 0; i < lineCount; ++i) {
            // Update key index
            thread_key_indices[thread_id][unique_keys[i]] = i;
            
            // Update other indices
            thread_borough_indices[thread_id][location_data.boroughs[i]].push_back(i);
            thread_zip_indices[thread_id][location_data.zip_codes[i]].push_back(i);
            thread_date_indices[thread_id][{time_data.dates[i], time_data.times[i]}].push_back(i);
            
            const int total_injuries = casualty_data.persons_injured[i] + 
                                     casualty_data.pedestrians_injured[i] + 
                                     casualty_data.cyclists_injured[i] + 
                                     casualty_data.motorists_injured[i];
            thread_injury_indices[thread_id][total_injuries].push_back(i);
            
            const int total_fatalities = casualty_data.persons_killed[i] + 
                                       casualty_data.pedestrians_killed[i] + 
                                       casualty_data.cyclists_killed[i] + 
                                       casualty_data.motorists_killed[i];
            thread_fatality_indices[thread_id][total_fatalities].push_back(i);
            
            thread_ped_fatality_indices[thread_id][casualty_data.pedestrians_killed[i]].push_back(i);
            thread_cyclist_fatality_indices[thread_id][casualty_data.cyclists_killed[i]].push_back(i);
            thread_motorist_fatality_indices[thread_id][casualty_data.motorists_killed[i]].push_back(i);
            
            for (size_t type_idx : vehicle_data.type_indices[i]) {
                const std::string& type = vehicle_data.type_pool[type_idx];
                thread_vehicle_type_indices[thread_id][type].push_back(i);
            }
        }
    }

    // Merge thread-local indices into global indices
    for (const auto& thread_key_idx : thread_key_indices) {
        key_to_index.insert(thread_key_idx.begin(), thread_key_idx.end());
    }

    auto mergeIndices = [](auto& global_index, const auto& thread_indices) {
        for (const auto& thread_idx : thread_indices) {
            for (const auto& [key, indices] : thread_idx) {
                auto& global_vec = global_index[key];
                global_vec.insert(global_vec.end(), indices.begin(), indices.end());
            }
        }
    };

    mergeIndices(borough_index, thread_borough_indices);
    mergeIndices(zip_index, thread_zip_indices);
    mergeIndices(date_index, thread_date_indices);
    mergeIndices(injury_index, thread_injury_indices);
    mergeIndices(fatality_index, thread_fatality_indices);
    mergeIndices(pedestrian_fatality_index, thread_ped_fatality_indices);
    mergeIndices(cyclist_fatality_index, thread_cyclist_fatality_indices);
    mergeIndices(motorist_fatality_index, thread_motorist_fatality_indices);
    mergeIndices(vehicle_type_index, thread_vehicle_type_indices);
    #else
    for (size_t i = 0; i < lineCount; ++i) {
        key_to_index[unique_keys[i]] = i;
        borough_index[location_data.boroughs[i]].push_back(i);
        zip_index[location_data.zip_codes[i]].push_back(i);
        date_index[{time_data.dates[i], time_data.times[i]}].push_back(i);
        
        injury_index[casualty_data.persons_injured[i] + casualty_data.pedestrians_injured[i] + 
                    casualty_data.cyclists_injured[i] + casualty_data.motorists_injured[i]].push_back(i);
        fatality_index[casualty_data.persons_killed[i] + casualty_data.pedestrians_killed[i] + 
                      casualty_data.cyclists_killed[i] + casualty_data.motorists_killed[i]].push_back(i);
        
        pedestrian_fatality_index[casualty_data.pedestrians_killed[i]].push_back(i);
        cyclist_fatality_index[casualty_data.cyclists_killed[i]].push_back(i);
        motorist_fatality_index[casualty_data.motorists_killed[i]].push_back(i);
        
        for (size_t type_idx : vehicle_data.type_indices[i]) {
            const std::string& type = vehicle_data.type_pool[type_idx];
            vehicle_type_index[type].push_back(i);
        }
    }
    #endif
    
    // Initialize spatial grid
    spatial_grid = createSpatialGrid();
    
    // Populate spatial grid in parallel with thread-local buffers
    #ifdef _OPENMP
    std::vector<std::vector<std::vector<size_t>>> thread_buffers(omp_get_max_threads(), 
        std::vector<std::vector<size_t>>(spatial_grid.size()));

    #pragma omp parallel
    {
        const int thread_id = omp_get_thread_num();
        auto& local_buffers = thread_buffers[thread_id];
        
        // Pre-reserve space in local buffers
        const size_t avg_size = location_data.latitudes.size() / (100 * omp_get_max_threads()); // 10x10 grid
        for (auto& buffer : local_buffers) {
            buffer.reserve(avg_size);
        }
        
        #pragma omp for schedule(static, 10000)
        for (size_t i = 0; i < location_data.latitudes.size(); ++i) {
            for (size_t cell_idx = 0; cell_idx < spatial_grid.size(); ++cell_idx) {
                const auto& cell = spatial_grid[cell_idx];
                if (location_data.latitudes[i] >= cell.minLat && location_data.latitudes[i] <= cell.maxLat &&
                    location_data.longitudes[i] >= cell.minLon && location_data.longitudes[i] <= cell.maxLon) {
                    local_buffers[cell_idx].push_back(i);
                    break;
                }
            }
        }
    }
    
    // Merge thread-local buffers into spatial grid
    for (size_t cell_idx = 0; cell_idx < spatial_grid.size(); ++cell_idx) {
        size_t total_size = 0;
        for (const auto& thread_buffer : thread_buffers) {
            total_size += thread_buffer[cell_idx].size();
        }
        
        spatial_grid[cell_idx].indices.reserve(total_size);
        for (const auto& thread_buffer : thread_buffers) {
            spatial_grid[cell_idx].indices.insert(
                spatial_grid[cell_idx].indices.end(),
                thread_buffer[cell_idx].begin(),
                thread_buffer[cell_idx].end()
            );
        }
    }
    #else
    for (size_t i = 0; i < location_data.latitudes.size(); ++i) {
        for (size_t cell_idx = 0; cell_idx < spatial_grid.size(); ++cell_idx) {
            const auto& cell = spatial_grid[cell_idx];
            if (location_data.latitudes[i] >= cell.minLat && location_data.latitudes[i] <= cell.maxLat &&
                location_data.longitudes[i] >= cell.minLon && location_data.longitudes[i] <= cell.maxLon) {
                spatial_grid[cell_idx].indices.push_back(i);
                break;
            }
        }
    }
    #endif
}

std::shared_ptr<Record> VectorizedDataSet::createRecord(size_t index) const {
    auto record = std::make_shared<Record>();
    
    // Pre-allocate vectors to avoid reallocations
    const auto& type_indices = vehicle_data.type_indices[index];
    const auto& factor_indices = vehicle_data.factor_indices[index];
    
    VehicleInfo vehicleInfo;
    vehicleInfo.vehicle_types.reserve(type_indices.size());
    vehicleInfo.contributing_factors.reserve(factor_indices.size());
    
    // Set all fields at once to minimize function calls
    record->setUniqueKey(unique_keys[index]);
    record->setBorough(location_data.boroughs[index]);
    record->setZipCode(location_data.zip_codes[index]);
    record->setLocation({location_data.latitudes[index], location_data.longitudes[index]});
    record->setOnStreet(location_data.on_streets[index]);
    record->setCrossStreet(location_data.cross_streets[index]);
    record->setOffStreet(location_data.off_streets[index]);
    record->setDateTime({time_data.dates[index], time_data.times[index]});
    
    // Set casualty stats directly
    record->setCasualtyStats({
        casualty_data.persons_injured[index],
        casualty_data.persons_killed[index],
        casualty_data.pedestrians_injured[index],
        casualty_data.pedestrians_killed[index],
        casualty_data.cyclists_injured[index],
        casualty_data.cyclists_killed[index],
        casualty_data.motorists_injured[index],
        casualty_data.motorists_killed[index]
    });
    
    // Efficiently build vehicle info
    for (size_t type_idx : type_indices) {
        vehicleInfo.vehicle_types.push_back(vehicle_data.type_pool[type_idx]);
    }
    for (size_t factor_idx : factor_indices) {
        vehicleInfo.contributing_factors.push_back(vehicle_data.factor_pool[factor_idx]);
    }
    record->setVehicleInfo(std::move(vehicleInfo));
    
    return record;
}

VectorizedDataSet::Records VectorizedDataSet::createRecordsFromIndices(
    const std::vector<size_t>& indices
) const {
    // Only parallelize for large result sets
    if (indices.size() < 10000) {
        Records result;
        result.reserve(indices.size());
        for (size_t idx : indices) {
            result.push_back(createRecord(idx));
        }
        return result;
    }

    Records result(indices.size());
    
    const size_t CHUNK_SIZE = 10000;  // Increased chunk size
    
    #ifdef _OPENMP
    #pragma omp parallel for schedule(static, CHUNK_SIZE)
    #endif
    for (size_t i = 0; i < indices.size(); ++i) {
        result[i] = createRecord(indices[i]);
    }
    
    return result;
}

VectorizedDataSet::Records VectorizedDataSet::queryByGeoBounds(
    float minLat, float maxLat,
    float minLon, float maxLon
) const {
    // First pass: count matching points to pre-allocate
    size_t total_points = 0;
    std::vector<size_t> matching_cells;
    matching_cells.reserve(spatial_grid.size());
    
    for (size_t cell_idx = 0; cell_idx < spatial_grid.size(); ++cell_idx) {
        const auto& cell = spatial_grid[cell_idx];
        if (cell.maxLat >= minLat && cell.minLat <= maxLat &&
            cell.maxLon >= minLon && cell.minLon <= maxLon) {
            matching_cells.push_back(cell_idx);
            total_points += cell.indices.size();
        }
    }
    
    // For small result sets, avoid parallelization overhead
    if (total_points < 10000) {
        std::vector<size_t> result_indices;
        result_indices.reserve(total_points);
        
        for (size_t cell_idx : matching_cells) {
            const auto& cell = spatial_grid[cell_idx];
            for (size_t idx : cell.indices) {
                if (location_data.latitudes[idx] >= minLat && location_data.latitudes[idx] <= maxLat &&
                    location_data.longitudes[idx] >= minLon && location_data.longitudes[idx] <= maxLon) {
                    result_indices.push_back(idx);
                }
            }
        }
        return createRecordsFromIndices(result_indices);
    }
    
    // For large result sets, use parallel processing
    #ifdef _OPENMP
    const size_t num_threads = omp_get_max_threads();
    std::vector<std::vector<size_t>> thread_buffers(num_threads);
    
    #pragma omp parallel
    {
        const int thread_id = omp_get_thread_num();
        auto& local_indices = thread_buffers[thread_id];
        local_indices.reserve(total_points / num_threads);
        
        #pragma omp for schedule(static)
        for (size_t i = 0; i < matching_cells.size(); ++i) {
            const auto& cell = spatial_grid[matching_cells[i]];
            for (size_t idx : cell.indices) {
                if (location_data.latitudes[idx] >= minLat && location_data.latitudes[idx] <= maxLat &&
                    location_data.longitudes[idx] >= minLon && location_data.longitudes[idx] <= maxLon) {
                    local_indices.push_back(idx);
                }
            }
        }
    }
    
    // Merge results
    std::vector<size_t> result_indices;
    result_indices.reserve(total_points);
    for (const auto& buffer : thread_buffers) {
        result_indices.insert(result_indices.end(),
                            buffer.begin(),
                            buffer.end());
    }
    #else
    std::vector<size_t> result_indices;
    result_indices.reserve(total_points);
    
    for (size_t cell_idx : matching_cells) {
        const auto& cell = spatial_grid[cell_idx];
        for (size_t idx : cell.indices) {
            if (location_data.latitudes[idx] >= minLat && location_data.latitudes[idx] <= maxLat &&
                location_data.longitudes[idx] >= minLon && location_data.longitudes[idx] <= maxLon) {
                result_indices.push_back(idx);
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
    // For small ranges, avoid parallelization overhead
    auto startIt = date_index.lower_bound(start);
    auto endIt = date_index.upper_bound(end);
    
    // Count number of elements in range
    size_t range_size = 0;
    for (auto it = startIt; it != endIt && range_size < 100; ++it) {
        range_size += it->second.size();
    }
    
    if (range_size < 100) {
        std::vector<size_t> result_indices;
        for (auto it = startIt; it != endIt; ++it) {
            result_indices.insert(result_indices.end(),
                                it->second.begin(),
                                it->second.end());
        }
        return createRecordsFromIndices(result_indices);
    }
    
    // For large ranges, use parallel processing
    size_t total_indices = 0;
    for (auto it = startIt; it != endIt; ++it) {
        total_indices += it->second.size();
    }
    
    std::vector<size_t> result_indices;
    result_indices.reserve(total_indices);
    
    // First collect all indices
    std::vector<std::pair<size_t, const std::vector<size_t>*>> all_indices;
    all_indices.reserve(std::distance(startIt, endIt));
    for (auto it = startIt; it != endIt; ++it) {
        all_indices.emplace_back(0, &it->second);
    }
    
    #ifdef _OPENMP
    const size_t num_threads = omp_get_max_threads();
    std::vector<std::vector<size_t>> thread_buffers(num_threads);
    
    #pragma omp parallel
    {
        const int thread_id = omp_get_thread_num();
        auto& local_indices = thread_buffers[thread_id];
        local_indices.reserve(total_indices / num_threads);
        
        #pragma omp for schedule(static)
        for (size_t i = 0; i < all_indices.size(); ++i) {
            const auto& indices = *all_indices[i].second;
            local_indices.insert(local_indices.end(), indices.begin(), indices.end());
        }
    }
    
    // Single-threaded merge of results
    for (const auto& buffer : thread_buffers) {
        result_indices.insert(result_indices.end(), buffer.begin(), buffer.end());
    }
    #else
    for (const auto& [_, indices] : all_indices) {
        result_indices.insert(result_indices.end(), indices->begin(), indices->end());
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
    // For small ranges, avoid parallelization overhead
    auto startIt = injury_index.lower_bound(minInjuries);
    auto endIt = injury_index.upper_bound(maxInjuries);
    
    // Count number of elements in range
    size_t range_size = 0;
    for (auto it = startIt; it != endIt && range_size < 100; ++it) {
        range_size += it->second.size();
    }
    
    if (range_size < 100) {
        std::vector<size_t> result_indices;
        for (auto it = startIt; it != endIt; ++it) {
            result_indices.insert(result_indices.end(),
                                it->second.begin(),
                                it->second.end());
        }
        return createRecordsFromIndices(result_indices);
    }
    
    // For large ranges, use parallel processing
    size_t total_indices = 0;
    for (auto it = startIt; it != endIt; ++it) {
        total_indices += it->second.size();
    }
    
    std::vector<size_t> result_indices;
    result_indices.reserve(total_indices);
    
    // First collect all indices
    std::vector<std::pair<size_t, const std::vector<size_t>*>> all_indices;
    all_indices.reserve(std::distance(startIt, endIt));
    for (auto it = startIt; it != endIt; ++it) {
        all_indices.emplace_back(0, &it->second);
    }
    
    #ifdef _OPENMP
    const size_t num_threads = omp_get_max_threads();
    std::vector<std::vector<size_t>> thread_buffers(num_threads);
    
    #pragma omp parallel
    {
        const int thread_id = omp_get_thread_num();
        auto& local_indices = thread_buffers[thread_id];
        local_indices.reserve(total_indices / num_threads);
        
        #pragma omp for schedule(static)
        for (size_t i = 0; i < all_indices.size(); ++i) {
            const auto& indices = *all_indices[i].second;
            local_indices.insert(local_indices.end(), indices.begin(), indices.end());
        }
    }
    
    // Single-threaded merge of results
    for (const auto& buffer : thread_buffers) {
        result_indices.insert(result_indices.end(), buffer.begin(), buffer.end());
    }
    #else
    for (const auto& [_, indices] : all_indices) {
        result_indices.insert(result_indices.end(), indices->begin(), indices->end());
    }
    #endif
    
    return createRecordsFromIndices(result_indices);
}


VectorizedDataSet::RecordPtr VectorizedDataSet::queryByUniqueKey(int key) const {
    auto it = key_to_index.find(key);
    return it != key_to_index.end() ? createRecord(it->second) : nullptr;
}

VectorizedDataSet::Records VectorizedDataSet::queryByFatalityRange(
    int minFatalities,
    int maxFatalities
) const {
    // For small ranges, avoid parallelization overhead
    auto startIt = fatality_index.lower_bound(minFatalities);
    auto endIt = fatality_index.upper_bound(maxFatalities);
    
    // Count number of elements in range
    size_t range_size = 0;
    for (auto it = startIt; it != endIt && range_size < 100; ++it) {
        range_size += it->second.size();
    }
    
    if (range_size < 100) {
        std::vector<size_t> result_indices;
        for (auto it = startIt; it != endIt; ++it) {
            result_indices.insert(result_indices.end(),
                                it->second.begin(),
                                it->second.end());
        }
        return createRecordsFromIndices(result_indices);
    }
    
    // For large ranges, use parallel processing
    size_t total_indices = 0;
    for (auto it = startIt; it != endIt; ++it) {
        total_indices += it->second.size();
    }
    
    std::vector<size_t> result_indices;
    result_indices.reserve(total_indices);
    
    // First collect all indices
    std::vector<std::pair<size_t, const std::vector<size_t>*>> all_indices;
    all_indices.reserve(std::distance(startIt, endIt));
    for (auto it = startIt; it != endIt; ++it) {
        all_indices.emplace_back(0, &it->second);
    }
    
    #ifdef _OPENMP
    const size_t num_threads = omp_get_max_threads();
    std::vector<std::vector<size_t>> thread_buffers(num_threads);
    
    #pragma omp parallel
    {
        const int thread_id = omp_get_thread_num();
        auto& local_indices = thread_buffers[thread_id];
        local_indices.reserve(total_indices / num_threads);
        
        #pragma omp for schedule(static)
        for (size_t i = 0; i < all_indices.size(); ++i) {
            const auto& indices = *all_indices[i].second;
            local_indices.insert(local_indices.end(), indices.begin(), indices.end());
        }
    }
    
    // Single-threaded merge of results
    for (const auto& buffer : thread_buffers) {
        result_indices.insert(result_indices.end(), buffer.begin(), buffer.end());
    }
    #else
    for (const auto& [_, indices] : all_indices) {
        result_indices.insert(result_indices.end(), indices->begin(), indices->end());
    }
    #endif
    
    return createRecordsFromIndices(result_indices);
}

VectorizedDataSet::Records VectorizedDataSet::queryByPedestrianFatalities(
    int minFatalities,
    int maxFatalities
) const {
    // For small ranges, avoid parallelization overhead
    auto startIt = pedestrian_fatality_index.lower_bound(minFatalities);
    auto endIt = pedestrian_fatality_index.upper_bound(maxFatalities);
    
    // Count number of elements in range
    size_t range_size = 0;
    for (auto it = startIt; it != endIt && range_size < 100; ++it) {
        range_size += it->second.size();
    }
    
    if (range_size < 100) {
        std::vector<size_t> result_indices;
        for (auto it = startIt; it != endIt; ++it) {
            result_indices.insert(result_indices.end(),
                                it->second.begin(),
                                it->second.end());
        }
        return createRecordsFromIndices(result_indices);
    }
    
    // For large ranges, use parallel processing
    size_t total_indices = 0;
    for (auto it = startIt; it != endIt; ++it) {
        total_indices += it->second.size();
    }
    
    std::vector<size_t> result_indices;
    result_indices.reserve(total_indices);
    
    // First collect all indices
    std::vector<std::pair<size_t, const std::vector<size_t>*>> all_indices;
    all_indices.reserve(std::distance(startIt, endIt));
    for (auto it = startIt; it != endIt; ++it) {
        all_indices.emplace_back(0, &it->second);
    }
    
    #ifdef _OPENMP
    const size_t num_threads = omp_get_max_threads();
    std::vector<std::vector<size_t>> thread_buffers(num_threads);
    
    #pragma omp parallel
    {
        const int thread_id = omp_get_thread_num();
        auto& local_indices = thread_buffers[thread_id];
        local_indices.reserve(total_indices / num_threads);
        
        #pragma omp for schedule(static)
        for (size_t i = 0; i < all_indices.size(); ++i) {
            const auto& indices = *all_indices[i].second;
            local_indices.insert(local_indices.end(), indices.begin(), indices.end());
        }
    }
    
    // Single-threaded merge of results
    for (const auto& buffer : thread_buffers) {
        result_indices.insert(result_indices.end(), buffer.begin(), buffer.end());
    }
    #else
    for (const auto& [_, indices] : all_indices) {
        result_indices.insert(result_indices.end(), indices->begin(), indices->end());
    }
    #endif
    
    return createRecordsFromIndices(result_indices);
}

VectorizedDataSet::Records VectorizedDataSet::queryByCyclistFatalities(
    int minFatalities,
    int maxFatalities
) const {
    // For small ranges, avoid parallelization overhead
    auto startIt = cyclist_fatality_index.lower_bound(minFatalities);
    auto endIt = cyclist_fatality_index.upper_bound(maxFatalities);
    
    // Count number of elements in range
    size_t range_size = 0;
    for (auto it = startIt; it != endIt && range_size < 100; ++it) {
        range_size += it->second.size();
    }
    
    if (range_size < 100) {
        std::vector<size_t> result_indices;
        for (auto it = startIt; it != endIt; ++it) {
            result_indices.insert(result_indices.end(),
                                it->second.begin(),
                                it->second.end());
        }
        return createRecordsFromIndices(result_indices);
    }
    
    // For large ranges, use parallel processing
    size_t total_indices = 0;
    for (auto it = startIt; it != endIt; ++it) {
        total_indices += it->second.size();
    }
    
    std::vector<size_t> result_indices;
    result_indices.reserve(total_indices);
    
    // First collect all indices
    std::vector<std::pair<size_t, const std::vector<size_t>*>> all_indices;
    all_indices.reserve(std::distance(startIt, endIt));
    for (auto it = startIt; it != endIt; ++it) {
        all_indices.emplace_back(0, &it->second);
    }
    
    #ifdef _OPENMP
    const size_t num_threads = omp_get_max_threads();
    std::vector<std::vector<size_t>> thread_buffers(num_threads);
    
    #pragma omp parallel
    {
        const int thread_id = omp_get_thread_num();
        auto& local_indices = thread_buffers[thread_id];
        local_indices.reserve(total_indices / num_threads);
        
        #pragma omp for schedule(static)
        for (size_t i = 0; i < all_indices.size(); ++i) {
            const auto& indices = *all_indices[i].second;
            local_indices.insert(local_indices.end(), indices.begin(), indices.end());
        }
    }
    
    // Single-threaded merge of results
    for (const auto& buffer : thread_buffers) {
        result_indices.insert(result_indices.end(), buffer.begin(), buffer.end());
    }
    #else
    for (const auto& [_, indices] : all_indices) {
        result_indices.insert(result_indices.end(), indices->begin(), indices->end());
    }
    #endif
    
    return createRecordsFromIndices(result_indices);
}

VectorizedDataSet::Records VectorizedDataSet::queryByMotoristFatalities(
    int minFatalities,
    int maxFatalities
) const {
    // For small ranges, avoid parallelization overhead
    auto startIt = motorist_fatality_index.lower_bound(minFatalities);
    auto endIt = motorist_fatality_index.upper_bound(maxFatalities);
    
    // Count number of elements in range
    size_t range_size = 0;
    for (auto it = startIt; it != endIt && range_size < 100; ++it) {
        range_size += it->second.size();
    }
    
    if (range_size < 100) {
        std::vector<size_t> result_indices;
        for (auto it = startIt; it != endIt; ++it) {
            result_indices.insert(result_indices.end(),
                                it->second.begin(),
                                it->second.end());
        }
        return createRecordsFromIndices(result_indices);
    }
    
    // For large ranges, use parallel processing
    size_t total_indices = 0;
    for (auto it = startIt; it != endIt; ++it) {
        total_indices += it->second.size();
    }
    
    std::vector<size_t> result_indices;
    result_indices.reserve(total_indices);
    
    // First collect all indices
    std::vector<std::pair<size_t, const std::vector<size_t>*>> all_indices;
    all_indices.reserve(std::distance(startIt, endIt));
    for (auto it = startIt; it != endIt; ++it) {
        all_indices.emplace_back(0, &it->second);
    }
    
    #ifdef _OPENMP
    const size_t num_threads = omp_get_max_threads();
    std::vector<std::vector<size_t>> thread_buffers(num_threads);
    
    #pragma omp parallel
    {
        const int thread_id = omp_get_thread_num();
        auto& local_indices = thread_buffers[thread_id];
        local_indices.reserve(total_indices / num_threads);
        
        #pragma omp for schedule(static)
        for (size_t i = 0; i < all_indices.size(); ++i) {
            const auto& indices = *all_indices[i].second;
            local_indices.insert(local_indices.end(), indices.begin(), indices.end());
        }
    }
    
    // Single-threaded merge of results
    for (const auto& buffer : thread_buffers) {
        result_indices.insert(result_indices.end(), buffer.begin(), buffer.end());
    }
    #else
    for (const auto& [_, indices] : all_indices) {
        result_indices.insert(result_indices.end(), indices->begin(), indices->end());
    }
    #endif
    
    return createRecordsFromIndices(result_indices);
}

} // namespace nycollision
