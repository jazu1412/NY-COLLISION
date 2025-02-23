#include <nycollision/data/DataSet.h>
#include <nycollision/data/VectorizedDataSet.h>
#include <nycollision/parser/CSVParser.h>
#include <iostream>
#include <iomanip>
#include <chrono>
#include <map>

// Helper function to print collision records
void printCollisions(const std::vector<std::shared_ptr<const nycollision::IRecord>>& records, size_t limit = 3) {
    std::cout << "Found " << records.size() << " collisions:\n";
    for (size_t i = 0; i < std::min(limit, records.size()); ++i) {
        const auto& record = records[i];
        const auto& stats = record->getCasualtyStats();
        const auto& vehicleInfo = record->getVehicleInfo();
        
        std::cout << "\nDate: " << record->getDateTime().date
                  << " " << record->getDateTime().time << "\n"
                  << " (ZIP: " << record->getZipCode() << ")\n"
                  << "Coordinates: " << std::fixed << std::setprecision(6)
                  << record->getLocation().latitude << ", "
                  << record->getLocation().longitude << "\n"
                  << "Casualties:\n"
                  << "  Total: " << stats.getTotalInjuries() << " injured, "
                  << stats.getTotalFatalities() << " killed\n"
                  << "  Pedestrians: " << stats.pedestrians_injured << " injured, "
                  << stats.pedestrians_killed << " killed\n"
                  << "  Cyclists: " << stats.cyclists_injured << " injured, "
                  << stats.cyclists_killed << " killed\n"
                  << "  Motorists: " << stats.motorists_injured << " injured, "
                  << stats.motorists_killed << " killed\n"
                  << "Street: " << record->getOnStreet()
                  << " at " << record->getCrossStreet() << "\n";
        
        if (!vehicleInfo.vehicle_types.empty()) {
            std::cout << "Vehicles involved:";
            for (const auto& type : vehicleInfo.vehicle_types) {
                if (!type.empty()) std::cout << " " << type;
            }
            std::cout << "\n";
        }
    }
    std::cout << std::endl;
}

// Helper function to measure execution time
template<typename Func>
double measureTime(Func&& func) {
    auto start = std::chrono::high_resolution_clock::now();
    func();
    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration<double, std::milli>(end - start).count();
}

// Structure to hold query times
struct QueryTimes {
    double dateRange;
    double geoBounds;
    double injury;
    double vehicle;
    double borough;
    double boroughCount;
    double total;
};

// Helper function to run performance tests
QueryTimes runPerformanceTests(const nycollision::IDataSet& dataset) {

    
    std::cout << "Running performance tests...\n";
    
    QueryTimes times;
    
    // // Date range queries
    // times.dateRange = measureTime([&]() {
    //     nycollision::Date start{"2024-01-01", "00:00"};
    //     nycollision::Date end{"2024-01-31", "23:59"};
    //     auto records = dataset.queryByDateRange(start, end);
    //     std::cout << "Date range query found " << records.size() << " records\n";
    // });
    
    // // Geographic bounds queries
    // times.geoBounds = measureTime([&]() {
    //     auto records = dataset.queryByGeoBounds(
    //         40.7000, 40.7200,  // latitude range
    //         -74.0100, -73.9900 // longitude range
    //     );
    //     std::cout << "Geographic bounds query found " << records.size() << " records\n";
    // });
    
    // Injury range queries
    times.injury = measureTime([&]() {
        auto records = dataset.queryByInjuryRange(3, 10);
        std::cout << "Injury range query found " << records.size() << " records\n";
    });
    
    // Vehicle type queries
    times.vehicle = measureTime([&]() {
        auto records = dataset.queryByVehicleType("TAXI");
        std::cout << "Vehicle type query found " << records.size() << " records\n";
    });
    
    // Borough queries
    times.borough = measureTime([&]() {
        auto records = dataset.queryByBorough("BROOKLYN");
        std::cout << "Borough query found " << records.size() << " records\n";
    });
    
    // Borough count aggregation
    times.boroughCount = measureTime([&]() {
        auto count = dataset.countByBorough("BROOKLYN");
        std::cout << "Borough count aggregation found " << count << " records in BROOKLYN\n";
    });

    times.total = times.injury + times.vehicle + times.borough + times.boroughCount;
    
    std::cout << "\nPerformance Results:\n"
            //   << "Date range query: " << times.dateRange << "ms\n"
            //   << "Geographic bounds query: " << times.geoBounds << "ms\n"
              << "Injury range query: " << times.injury << "ms\n"
              << "Vehicle type query: " << times.vehicle << "ms\n"
              << "Borough query: " << times.borough << "ms\n"
              << "Borough count aggregation: " << times.boroughCount << "ms\n"
              << "Total time: " << times.total << "ms\n\n";
              
    return times;
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <collision_data.csv>\n";
        return 1;
    }

    // Initialize OpenMP settings once at program start
    nycollision::config::initializeOpenMP();

    try {
        nycollision::CSVParser parser;
        
        // Test original implementation
        std::cout << "=== Testing Original Implementation ===\n";
        nycollision::DataSet originalDataset;
        originalDataset.loadFromFile(argv[1], parser);
        QueryTimes originalTimes = runPerformanceTests(originalDataset);
        
        // Test vectorized implementation
        std::cout << "\n=== Testing Vectorized Implementation ===\n";
        nycollision::VectorizedDataSet vectorizedDataset;
        vectorizedDataset.loadFromFile(argv[1], parser);
        QueryTimes vectorizedTimes = runPerformanceTests(vectorizedDataset);
        
        // Calculate and print performance improvements
        std::cout << "\n=== Performance Improvements ===\n"
                  << std::fixed << std::setprecision(2)
                //   << "Date range query: " 
                //   << ((originalTimes.dateRange - vectorizedTimes.dateRange) / originalTimes.dateRange * 100) << "%\n"
                //   << "Geographic bounds query: "
                //   << ((originalTimes.geoBounds - vectorizedTimes.geoBounds) / originalTimes.geoBounds * 100) << "%\n"
                  << "Injury range query: "
                  << ((originalTimes.injury - vectorizedTimes.injury) / originalTimes.injury * 100) << "%\n"
                  << "Vehicle type query: "
                  << ((originalTimes.vehicle - vectorizedTimes.vehicle) / originalTimes.vehicle * 100) << "%\n"
                  << "Borough query: "
                  << ((originalTimes.borough - vectorizedTimes.borough) / originalTimes.borough * 100) << "%\n"
                  << "Borough count aggregation: "
                  << ((originalTimes.boroughCount - vectorizedTimes.boroughCount) / originalTimes.boroughCount * 100) << "%\n"
                  << "Total improvement: "
                  << ((originalTimes.total - vectorizedTimes.total) / originalTimes.total * 100) << "%\n";

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
