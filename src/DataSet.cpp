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
    // Simple storage without indexing
    records_.push_back(record);
}

DataSet::Records DataSet::queryByGeoBounds(
    float minLat, float maxLat,
    float minLon, float maxLon
) const {
    std::vector<std::shared_ptr<Record>> result;
    
    #pragma omp parallel
    {
        std::vector<std::shared_ptr<Record>> local_matches;
        
        #pragma omp for nowait
        for (size_t i = 0; i < records_.size(); ++i) {
            auto loc = records_[i]->getLocation();
            if (loc.latitude >= minLat && loc.latitude <= maxLat &&
                loc.longitude >= minLon && loc.longitude <= maxLon) {
                local_matches.push_back(records_[i]);
            }
        }
        
        #pragma omp critical
        result.insert(result.end(), local_matches.begin(), local_matches.end());
    }
    
    std::cout << "calling from queryByGeoBounds" << " .\n";
    return convertToInterfaceRecords(result);
}

DataSet::Records DataSet::queryByZipCode(const std::string& zipCode) const {
    std::vector<std::shared_ptr<Record>> result;
    
    #pragma omp parallel
    {
        std::vector<std::shared_ptr<Record>> local_matches;
        
        #pragma omp for nowait
        for (size_t i = 0; i < records_.size(); ++i) {
            if (records_[i]->getZipCode() == zipCode) {
                local_matches.push_back(records_[i]);
            }
        }
        
        #pragma omp critical
        result.insert(result.end(), local_matches.begin(), local_matches.end());
    }
    
    std::cout << "calling from queryByZipCode" << " .\n";
    return convertToInterfaceRecords(result);
}

DataSet::Records DataSet::queryByDateRange(const Date& start, const Date& end) const {
    std::vector<std::shared_ptr<Record>> result;
    
    #pragma omp parallel
    {
        std::vector<std::shared_ptr<Record>> local_matches;
        
        #pragma omp for nowait
        for (size_t i = 0; i < records_.size(); ++i) {
            const auto& date = records_[i]->getDateTime();
            if (date >= start && date <= end) {
                local_matches.push_back(records_[i]);
            }
        }
        
        #pragma omp critical
        result.insert(result.end(), local_matches.begin(), local_matches.end());
    }
    
    std::cout << "calling from queryByDateRange" << " .\n";
    return convertToInterfaceRecords(result);
}

DataSet::Records DataSet::queryByVehicleType(const std::string& vehicleType) const {
    std::vector<std::shared_ptr<Record>> result;
    
    #pragma omp parallel
    {
        std::vector<std::shared_ptr<Record>> local_matches;
        
        #pragma omp for nowait
        for (size_t i = 0; i < records_.size(); ++i) {
            const auto& info = records_[i]->getVehicleInfo();
            if (std::find(info.vehicle_types.begin(), info.vehicle_types.end(), vehicleType) != info.vehicle_types.end()) {
                local_matches.push_back(records_[i]);
            }
        }
        
        #pragma omp critical
        result.insert(result.end(), local_matches.begin(), local_matches.end());
    }
    
    std::cout << "calling from queryByVehicleType" << " .\n";
    return convertToInterfaceRecords(result);
}

DataSet::Records DataSet::queryByInjuryRange(int minInjuries, int maxInjuries) const {
    std::vector<std::shared_ptr<Record>> result;
    
    #pragma omp parallel
    {
        std::vector<std::shared_ptr<Record>> local_matches;
        
        #pragma omp for nowait
        for (size_t i = 0; i < records_.size(); ++i) {
            const auto& stats = records_[i]->getCasualtyStats();
            int total = stats.getTotalInjuries();
            if (total >= minInjuries && total <= maxInjuries) {
                local_matches.push_back(records_[i]);
            }
        }
        
        #pragma omp critical
        result.insert(result.end(), local_matches.begin(), local_matches.end());
    }
    
    std::cout << "calling from queryByInjuryRange" << " .\n";
    return convertToInterfaceRecords(result);
}

DataSet::Records DataSet::queryByFatalityRange(int minFatalities, int maxFatalities) const {
    std::vector<std::shared_ptr<Record>> result;
    
    #pragma omp parallel
    {
        std::vector<std::shared_ptr<Record>> local_matches;
        
        #pragma omp for nowait
        for (size_t i = 0; i < records_.size(); ++i) {
            const auto& stats = records_[i]->getCasualtyStats();
            int total = stats.getTotalFatalities();
            if (total >= minFatalities && total <= maxFatalities) {
                local_matches.push_back(records_[i]);
            }
        }
        
        #pragma omp critical
        result.insert(result.end(), local_matches.begin(), local_matches.end());
    }
    
    std::cout << "calling from queryByFatalityRange" << " .\n";
    return convertToInterfaceRecords(result);
}

DataSet::RecordPtr DataSet::queryByUniqueKey(int key) const {
    std::shared_ptr<Record> result = nullptr;
    bool found = false;
    
    #pragma omp parallel for
    for (size_t i = 0; i < records_.size(); ++i) {
        if (!found && records_[i]->getUniqueKey() == key) {
            #pragma omp critical
            {
                if (!found) {
                    result = records_[i];
                    found = true;
                }
            }
        }
    }
    
    return result ? std::static_pointer_cast<const IRecord>(result) : nullptr;
}

DataSet::Records DataSet::queryByPedestrianFatalities(int minFatalities, int maxFatalities) const {
    std::vector<std::shared_ptr<Record>> result;
    
    #pragma omp parallel
    {
        std::vector<std::shared_ptr<Record>> local_matches;
        
        #pragma omp for nowait
        for (size_t i = 0; i < records_.size(); ++i) {
            const auto& stats = records_[i]->getCasualtyStats();
            if (stats.pedestrians_killed >= minFatalities && stats.pedestrians_killed <= maxFatalities) {
                local_matches.push_back(records_[i]);
            }
        }
        
        #pragma omp critical
        result.insert(result.end(), local_matches.begin(), local_matches.end());
    }
    
    std::cout << "calling from queryByPedestrianFatalities" << " .\n";
    return convertToInterfaceRecords(result);
}

DataSet::Records DataSet::queryByCyclistFatalities(int minFatalities, int maxFatalities) const {
    std::vector<std::shared_ptr<Record>> result;
    
    #pragma omp parallel
    {
        std::vector<std::shared_ptr<Record>> local_matches;
        
        #pragma omp for nowait
        for (size_t i = 0; i < records_.size(); ++i) {
            const auto& stats = records_[i]->getCasualtyStats();
            if (stats.cyclists_killed >= minFatalities && stats.cyclists_killed <= maxFatalities) {
                local_matches.push_back(records_[i]);
            }
        }
        
        #pragma omp critical
        result.insert(result.end(), local_matches.begin(), local_matches.end());
    }
    
    std::cout << "calling from queryByCyclistFatalities" << " .\n";
    return convertToInterfaceRecords(result);
}

DataSet::Records DataSet::queryByMotoristFatalities(int minFatalities, int maxFatalities) const {
    std::vector<std::shared_ptr<Record>> result;
    
    #pragma omp parallel
    {
        std::vector<std::shared_ptr<Record>> local_matches;
        
        #pragma omp for nowait
        for (size_t i = 0; i < records_.size(); ++i) {
            const auto& stats = records_[i]->getCasualtyStats();
            if (stats.motorists_killed >= minFatalities && stats.motorists_killed <= maxFatalities) {
                local_matches.push_back(records_[i]);
            }
        }
        
        #pragma omp critical
        result.insert(result.end(), local_matches.begin(), local_matches.end());
    }
    
    std::cout << "calling from queryByMotoristFatalities" << " .\n";
    return convertToInterfaceRecords(result);
}

DataSet::Records DataSet::queryByBorough(const std::string& borough) const {
    std::vector<std::shared_ptr<Record>> result;
    
    #pragma omp parallel
    {
        std::vector<std::shared_ptr<Record>> local_matches;
        
        #pragma omp for nowait
        for (size_t i = 0; i < records_.size(); ++i) {
            if (records_[i]->getBorough() == borough) {
                local_matches.push_back(records_[i]);
            }
        }
        
        #pragma omp critical
        result.insert(result.end(), local_matches.begin(), local_matches.end());
    }
    
    std::cout << "calling from queryByBorough" << " .\n";
    return convertToInterfaceRecords(result);
}
size_t DataSet::countByBorough(const std::string& borough) const {
    size_t count = 0;

    #ifdef _OPENMP
    #pragma omp parallel
    {
        #pragma omp master
        {
            std::cout << "Inside normal approach borough count parallel region: " 
                      << omp_get_num_threads() 
                      << " threads\n";
        }
    }
     #endif

    #pragma omp parallel for reduction(+:count)
    for (size_t i = 0; i < records_.size(); ++i) {
        if (records_[i]->getBorough() == borough) {
            count++;
        }
    }
    std::cout << "calling from countByBorough in normal approach, count: " << count << "\n";
    return count;
}

} // namespace nycollision
