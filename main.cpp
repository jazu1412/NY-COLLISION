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
                  << "Location: " << record->getBorough()
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

// Helper function to run performance tests
void runPerformanceTests(const nycollision::IDataSet& dataset) {
    std::cout << "Running performance tests...\n";
    
    // Test 1: Borough queries
    double boroughTime = measureTime([&]() {
        auto records = dataset.queryByBorough("BROOKLYN");
        std::cout << "Borough query found " << records.size() << " records\n";
    });
    
    // Test 2: Date range queries
    double dateTime = measureTime([&]() {
        nycollision::Date start{"2024-01-01", "00:00"};
        nycollision::Date end{"2024-01-31", "23:59"};
        auto records = dataset.queryByDateRange(start, end);
        std::cout << "Date range query found " << records.size() << " records\n";
    });
    
    // Test 3: Geographic bounds queries
    double geoTime = measureTime([&]() {
        auto records = dataset.queryByGeoBounds(
            40.7000, 40.7200,  // latitude range
            -74.0100, -73.9900 // longitude range
        );
        std::cout << "Geographic bounds query found " << records.size() << " records\n";
    });
    
    // Test 4: Injury range queries
    double injuryTime = measureTime([&]() {
        auto records = dataset.queryByInjuryRange(3, 10);
        std::cout << "Injury range query found " << records.size() << " records\n";
    });
    
    // Test 5: Vehicle type queries
    double vehicleTime = measureTime([&]() {
        auto records = dataset.queryByVehicleType("TAXI");
        std::cout << "Vehicle type query found " << records.size() << " records\n";
    });
    
    std::cout << "\nPerformance Results:\n"
              << "Borough query: " << std::fixed << std::setprecision(2) << boroughTime << "ms\n"
              << "Date range query: " << dateTime << "ms\n"
              << "Geographic bounds query: " << geoTime << "ms\n"
              << "Injury range query: " << injuryTime << "ms\n"
              << "Vehicle type query: " << vehicleTime << "ms\n"
              << "Total time: " << (boroughTime + dateTime + geoTime + injuryTime + vehicleTime) << "ms\n\n";
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <collision_data.csv>\n";
        return 1;
    }

    try {
        nycollision::CSVParser parser;
        
        // Test original implementation
        std::cout << "=== Testing Original Implementation ===\n";
        nycollision::DataSet originalDataset;
        double originalLoadTime = measureTime([&]() {
            originalDataset.loadFromFile(argv[1], parser);
        });
        std::cout << "Loaded " << originalDataset.size() << " records in "
                  << std::fixed << std::setprecision(2) << originalLoadTime << "ms\n\n";
        runPerformanceTests(originalDataset);
        
        // Test vectorized implementation
        std::cout << "=== Testing Vectorized Implementation ===\n";
        nycollision::VectorizedDataSet vectorizedDataset;
        double vectorizedLoadTime = measureTime([&]() {
            vectorizedDataset.loadFromFile(argv[1], parser);
        });
        std::cout << "Loaded " << vectorizedDataset.size() << " records in "
                  << vectorizedLoadTime << "ms\n\n";
        runPerformanceTests(vectorizedDataset);
        
        // Print performance comparison
        std::cout << "=== Performance Comparison ===\n"
                  << "Load time improvement: "
                  << std::setprecision(1)
                  << ((originalLoadTime - vectorizedLoadTime) / originalLoadTime * 100)
                  << "%\n";

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
