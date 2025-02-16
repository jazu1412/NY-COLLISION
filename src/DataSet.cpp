#include "../include/nycollision/data/DataSet.h"
#include <algorithm>

namespace nycollision {

namespace {
    // Initialize spatial grid with default coverage of NYC area
    std::vector<DataSet::GridCell> createSpatialGrid() {
        std::vector<DataSet::GridCell> grid;
        
        // NYC approximate bounds
        const float MIN_LAT = 40.4774f;
        const float MAX_LAT = 40.9176f;
        const float MIN_LON = -74.2591f;
        const float MAX_LON = -73.7004f;
        
        // Create a 10x10 grid
        const int GRID_SIZE = 10;
        const float LAT_STEP = (MAX_LAT - MIN_LAT) / GRID_SIZE;
        const float LON_STEP = (MAX_LON - MIN_LON) / GRID_SIZE;
        
        for (int i = 0; i < GRID_SIZE; ++i) {
            for (int j = 0; j < GRID_SIZE; ++j) {
                DataSet::GridCell cell;
                cell.minLat = MIN_LAT + (i * LAT_STEP);
                cell.maxLat = MIN_LAT + ((i + 1) * LAT_STEP);
                cell.minLon = MIN_LON + (j * LON_STEP);
                cell.maxLon = MIN_LON + ((j + 1) * LON_STEP);
                grid.push_back(cell);
            }
        }
        
        return grid;
    }
}

void DataSet::addRecord(std::shared_ptr<Record> record) {
    // Initialize spatial grid if not already done
    if (spatialGrid_.empty()) {
        spatialGrid_ = createSpatialGrid();
    }

    // Add to primary storage
    records_.push_back(record);
    
    // Update indices
    keyIndex_[record->getUniqueKey()] = record;
    boroughIndex_[record->getBorough()].push_back(record);
    zipIndex_[record->getZipCode()].push_back(record);
    dateIndex_[record->getDateTime()].push_back(record);
    
    // Update spatial index
    auto loc = record->getLocation();
    for (auto& cell : spatialGrid_) {
        if (loc.latitude >= cell.minLat && loc.latitude <= cell.maxLat &&
            loc.longitude >= cell.minLon && loc.longitude <= cell.maxLon) {
            cell.records.push_back(record);
            break;
        }
    }
    
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

DataSet::Records DataSet::queryByGeoBounds(
    float minLat, float maxLat,
    float minLon, float maxLon
) const {
    std::vector<std::shared_ptr<Record>> result;
    for (const auto& cell : spatialGrid_) {
        // Check if cell overlaps with query bounds
        if (cell.maxLat >= minLat && cell.minLat <= maxLat &&
            cell.maxLon >= minLon && cell.minLon <= maxLon) {
            // Check individual records in overlapping cells
            for (const auto& record : cell.records) {
                auto loc = record->getLocation();
                if (loc.latitude >= minLat && loc.latitude <= maxLat &&
                    loc.longitude >= minLon && loc.longitude <= maxLon) {
                    result.push_back(record);
                }
            }
        }
    }
    return convertToInterfaceRecords(result);
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
