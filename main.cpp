#include <nycollision/CollisionAnalyzer.h>
#include <iostream>
#include <iomanip>

// Helper function to print collision records
void printCollisions(const std::vector<std::shared_ptr<const nycollision::IRecord>>& records) {
    std::cout << "Found " << records.size() << " collisions:\n";
    for (const auto& record : records) {
        const auto& stats = record->getCasualtyStats();
        std::cout << "\nDate: " << record->getDateTime().date
                  << " " << record->getDateTime().time << "\n"
                  << "Location: " << record->getBorough()
                  << " (ZIP: " << record->getZipCode() << ")\n"
                  << "Coordinates: " << std::fixed << std::setprecision(6)
                  << record->getLocation().latitude << ", "
                  << record->getLocation().longitude << "\n"
                  << "Casualties: "
                  << stats.getTotalInjuries() << " injured, "
                  << stats.getTotalFatalities() << " killed\n"
                  << "Street: " << record->getOnStreet()
                  << " at " << record->getCrossStreet() << "\n";
    }
    std::cout << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <collision_data.csv>\n";
        return 1;
    }

    try {
        // Create analyzer and load data
        nycollision::CollisionAnalyzer analyzer;
        analyzer.loadData(argv[1]);
        std::cout << "Loaded " << analyzer.getTotalRecords() << " collision records\n\n";

        // Example 1: Find collisions in Brooklyn
        std::cout << "=== Collisions in Brooklyn ===\n";
        auto brooklynCollisions = analyzer.findCollisionsInBorough("BROOKLYN");
        printCollisions(std::vector<std::shared_ptr<const nycollision::IRecord>>(
            brooklynCollisions.begin(),
            brooklynCollisions.begin() + std::min(size_t(3), brooklynCollisions.size())
        ));

        // Example 2: Find collisions with high casualty count
        std::cout << "\n=== Severe Collisions (5+ injuries) ===\n";
        auto severeCollisions = analyzer.findCollisionsByInjuryCount(5, 999);
        printCollisions(std::vector<std::shared_ptr<const nycollision::IRecord>>(
            severeCollisions.begin(),
            severeCollisions.begin() + std::min(size_t(3), severeCollisions.size())
        ));

        // Example 3: Find collisions in a specific area
        std::cout << "\n=== Collisions in Lower Manhattan ===\n";
        auto areaCollisions = analyzer.findCollisionsInArea(
            40.7000, 40.7200,  // latitude range
            -74.0100, -73.9900 // longitude range
        );
        printCollisions(std::vector<std::shared_ptr<const nycollision::IRecord>>(
            areaCollisions.begin(),
            areaCollisions.begin() + std::min(size_t(3), areaCollisions.size())
        ));

        // Example 4: Find collisions by vehicle type
        std::cout << "\n=== Taxi-involved Collisions ===\n";
        auto taxiCollisions = analyzer.findCollisionsByVehicleType("TAXI");
        printCollisions(std::vector<std::shared_ptr<const nycollision::IRecord>>(
            taxiCollisions.begin(),
            taxiCollisions.begin() + std::min(size_t(3), taxiCollisions.size())
        ));

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
