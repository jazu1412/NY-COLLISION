#include "../include/nycollision/data/DataSet.h"
#include <algorithm>

namespace nycollision {

namespace {
     // If you want to fix the thread count:
   
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
    // Simple storage without indexing
    records_.push_back(record);
}

DataSet::Records DataSet::queryByGeoBounds(
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
    std::cout << "calling from queryByGeoBounds" << " .\n";
    return convertToInterfaceRecords(result);
}

DataSet::Records DataSet::queryByZipCode(const std::string& zipCode) const {
    std::vector<std::shared_ptr<Record>> result;
    for (const auto& record : records_) {
        if (record->getZipCode() == zipCode) {
            result.push_back(record);
        }
    }
    std::cout << "calling from queryByZipCode" << " .\n";
    return convertToInterfaceRecords(result);
}

DataSet::Records DataSet::queryByDateRange(const Date& start, const Date& end) const {
    std::vector<std::shared_ptr<Record>> result;
    for (const auto& record : records_) {
        const auto& date = record->getDateTime();
        if (date >= start && date <= end) {
            result.push_back(record);
        }
    }
    std::cout << "calling from queryByDateRange" << " .\n";
    return convertToInterfaceRecords(result);
}

DataSet::Records DataSet::queryByVehicleType(const std::string& vehicleType) const {
    std::vector<std::shared_ptr<Record>> result;
    for (const auto& record : records_) {
        const auto& info = record->getVehicleInfo();
        if (std::find(info.vehicle_types.begin(), info.vehicle_types.end(), vehicleType) != info.vehicle_types.end()) {
            result.push_back(record);
        }
    }
    std::cout << "calling from queryByVehicleType" << " .\n";
    return convertToInterfaceRecords(result);
}

DataSet::Records DataSet::queryByInjuryRange(int minInjuries, int maxInjuries) const {
    std::vector<std::shared_ptr<Record>> result;
    for (const auto& record : records_) {
        const auto& stats = record->getCasualtyStats();
        int total = stats.getTotalInjuries();
        if (total >= minInjuries && total <= maxInjuries) {
            result.push_back(record);
        }
    }
    std::cout << "calling from queryByInjuryRange" << " .\n";
    return convertToInterfaceRecords(result);
}

DataSet::Records DataSet::queryByFatalityRange(int minFatalities, int maxFatalities) const {
    std::vector<std::shared_ptr<Record>> result;
    for (const auto& record : records_) {
        const auto& stats = record->getCasualtyStats();
        int total = stats.getTotalFatalities();
        if (total >= minFatalities && total <= maxFatalities) {
            result.push_back(record);
        }
    }
    std::cout << "calling from queryByFatalityRange" << " .\n";
    return convertToInterfaceRecords(result);
}

DataSet::RecordPtr DataSet::queryByUniqueKey(int key) const {
    for (const auto& record : records_) {
        if (record->getUniqueKey() == key) {
            return std::static_pointer_cast<const IRecord>(record);
        }
    }
    return nullptr;
}

DataSet::Records DataSet::queryByPedestrianFatalities(int minFatalities, int maxFatalities) const {
    std::vector<std::shared_ptr<Record>> result;
    for (const auto& record : records_) {
        const auto& stats = record->getCasualtyStats();
        if (stats.pedestrians_killed >= minFatalities && stats.pedestrians_killed <= maxFatalities) {
            result.push_back(record);
        }
    }
    std::cout << "calling from queryByPedestrianFatalities" << " .\n";
    return convertToInterfaceRecords(result);
}

DataSet::Records DataSet::queryByCyclistFatalities(int minFatalities, int maxFatalities) const {
    std::vector<std::shared_ptr<Record>> result;
    for (const auto& record : records_) {
        const auto& stats = record->getCasualtyStats();
        if (stats.cyclists_killed >= minFatalities && stats.cyclists_killed <= maxFatalities) {
            result.push_back(record);
        }
    }
    std::cout << "calling from queryByCyclistFatalities" << " .\n";
    return convertToInterfaceRecords(result);
}

DataSet::Records DataSet::queryByMotoristFatalities(int minFatalities, int maxFatalities) const {
    std::vector<std::shared_ptr<Record>> result;
    for (const auto& record : records_) {
        const auto& stats = record->getCasualtyStats();
        if (stats.motorists_killed >= minFatalities && stats.motorists_killed <= maxFatalities) {
            result.push_back(record);
        }
    }
    std::cout << "calling from queryByMotoristFatalities" << " .\n";
    return convertToInterfaceRecords(result);
}

} // namespace nycollision
