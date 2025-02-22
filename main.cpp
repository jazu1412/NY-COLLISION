#include <nycollision/util/CollisionAnalyzer.h>
#include <iostream>
#include <iomanip>
#include <map>
#include <chrono>

using Clock = std::chrono::high_resolution_clock;
using Duration = std::chrono::duration<double>;

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
        
        if (!vehicleInfo.contributing_factors.empty()) {
            std::cout << "Contributing factors:";
            for (const auto& factor : vehicleInfo.contributing_factors) {
                if (!factor.empty()) std::cout << " " << factor;
            }
            std::cout << "\n";
        }
    }
    std::cout << std::endl;
}

// Helper function to analyze casualty statistics
void analyzeCasualties(const std::vector<std::shared_ptr<const nycollision::IRecord>>& records) {
    int totalInjuries = 0, totalFatalities = 0;
    int pedInjuries = 0, pedFatalities = 0;
    int cycInjuries = 0, cycFatalities = 0;
    int motInjuries = 0, motFatalities = 0;
    
    for (const auto& record : records) {
        const auto& stats = record->getCasualtyStats();
        totalInjuries += stats.getTotalInjuries();
        totalFatalities += stats.getTotalFatalities();
        pedInjuries += stats.pedestrians_injured;
        pedFatalities += stats.pedestrians_killed;
        cycInjuries += stats.cyclists_injured;
        cycFatalities += stats.cyclists_killed;
        motInjuries += stats.motorists_injured;
        motFatalities += stats.motorists_killed;
    }
    
    std::cout << "Casualty Analysis:\n"
              << "Total: " << totalInjuries << " injured, " << totalFatalities << " killed\n"
              << "Pedestrians: " << pedInjuries << " injured, " << pedFatalities << " killed\n"
              << "Cyclists: " << cycInjuries << " injured, " << cycFatalities << " killed\n"
              << "Motorists: " << motInjuries << " injured, " << motFatalities << " killed\n\n";
}

// Helper function to analyze vehicle types
void analyzeVehicleTypes(const std::vector<std::shared_ptr<const nycollision::IRecord>>& records) {
    std::map<std::string, int> vehicleCount;
    for (const auto& record : records) {
        for (const auto& type : record->getVehicleInfo().vehicle_types) {
            if (!type.empty()) vehicleCount[type]++;
        }
    }
    
    std::cout << "Vehicle Types Involved:\n";
    for (const auto& [type, count] : vehicleCount) {
        std::cout << std::setw(20) << std::left << type << ": " << count << "\n";
    }
    std::cout << std::endl;
}

// Helper function to measure and print execution time
template<typename Func>
auto measureTime(const std::string& description, Func&& func) {
    auto start = Clock::now();
    auto result = func();
    auto end = Clock::now();
    Duration elapsed = end - start;
    std::cout << description << " took " << elapsed.count() << " seconds\n";
    return result;
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <collision_data.csv>\n";
        return 1;
    }

    try {
        // Create analyzer and load data
        nycollision::CollisionAnalyzer analyzer;
        auto startTime = Clock::now();
        analyzer.loadData(argv[1]);
        auto endTime = Clock::now();
        Duration loadTime = endTime - startTime;
        std::cout << "Data loaded in " << loadTime.count() << " seconds.\n";
        std::cout << "Total records: " << analyzer.getTotalRecords() << "\n\n";

        // Example 1: Find collisions in Brooklyn
        std::cout << "\n=== Collisions in Brooklyn ===\n";
        auto brooklynCollisions = measureTime("Normal query", [&]() {
            return analyzer.findCollisionsInBorough("BROOKLYN");
        });
        printCollisions(brooklynCollisions);
        analyzeCasualties(brooklynCollisions);

        // Example 2: Find severe collisions
        std::cout << "\n=== Severe Collisions (5+ injuries) ===\n";
        auto severeCollisions = measureTime("Normal query", [&]() {
            return analyzer.findCollisionsByInjuryCount(5, 999);
        });
        printCollisions(severeCollisions);
        analyzeVehicleTypes(severeCollisions);

        // Example 3: Find collisions in Lower Manhattan
        std::cout << "\n=== Collisions in Lower Manhattan ===\n";
        std::cout << "Normal Query:\n";
        auto areaCollisions = measureTime("Normal query", [&]() {
            return analyzer.findCollisionsInArea(
                40.7000, 40.7200,  // latitude range
                -74.0100, -73.9900 // longitude range
            );
        });
        std::cout << "R-tree Query:\n";
        auto areaCollisionsRTree = measureTime("R-tree query", [&]() {
            return analyzer.getDataset().queryByGeoBoundsRTree(
                40.7000, 40.7200,  // latitude range
                -74.0100, -73.9900 // longitude range
            );
        });
        
        std::cout << "Normal query found: " << areaCollisions.size() << " records\n";
        std::cout << "R-tree query found: " << areaCollisionsRTree.size() << " records\n";
        printCollisions(areaCollisions);

        // Example 4: Find taxi-involved collisions
        std::cout << "\n=== Taxi-involved Collisions ===\n";
        auto taxiCollisions = measureTime("Normal query", [&]() {
            return analyzer.findCollisionsByVehicleType("TAXI");
        });
        printCollisions(taxiCollisions);
        analyzeCasualties(taxiCollisions);

        // Example 5: Find collisions by date range
        std::cout << "\n=== Collisions in January 2024 ===\n";
        auto januaryCollisions = measureTime("Normal query", [&]() {
            return analyzer.findCollisionsInDateRange("2024-01-01", "2024-01-31");
        });
        printCollisions(januaryCollisions);
        analyzeVehicleTypes(januaryCollisions);

        // Example 6: Find collisions with cyclist fatalities
        std::cout << "\n=== Collisions with Cyclist Fatalities ===\n";
        auto cyclistFatalities = measureTime("Normal query", [&]() {
            return analyzer.findCollisionsByFatalityCount(1, 999);
        });
        printCollisions(cyclistFatalities);
        analyzeCasualties(cyclistFatalities);

        // Performance comparison for different area sizes
        std::cout << "\n=== Spatial Query Performance Comparison ===\n";
        struct TestCase {
            std::string name;
            float minLat, maxLat, minLon, maxLon;
        };

        std::vector<TestCase> testCases = {
            {
                "Small Area (Few Blocks)",
                40.7100f, 40.7120f,  // ~0.2km
                -73.9900f, -73.9880f
            },
            {
                "Medium Area (Neighborhood)",
                40.7000f, 40.7200f,  // ~2km
                -74.0100f, -73.9900f
            },
            {
                "Large Area (Borough)",
                40.5000f, 40.9000f,  // ~40km
                -74.1000f, -73.7000f
            }
        };

        for (const auto& test : testCases) {
            std::cout << "\nTesting " << test.name << ":\n";
            auto stats = analyzer.getDataset().benchmarkQuery(
                test.minLat, test.maxLat,
                test.minLon, test.maxLon
            );
            std::cout << "Records found: " << stats.result_count << "\n"
                      << "Brute Force: " << stats.bruteforce_time << " seconds\n"
                      << "R-tree: " << stats.rtree_time << " seconds\n"
                      << "Speedup: " << (stats.bruteforce_time / stats.rtree_time) << "x\n";
        }

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
