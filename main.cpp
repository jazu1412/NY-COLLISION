#include <iostream>
#include "MyDataSet.h"
#include "QuoteAwareCSVParser.h"
#include <chrono>

int main() {
    // 1) Create your dataset
    MyDataSet dataset;

    // 2) Create a parser (QuoteAwareCSVParser)
    QuoteAwareCSVParser parser;

    // 3)  a CSV file (place "sample_data.csv" in same folder)
    dataset.loadFromCSV("Motor_Vehicle_Collisions_-_Crashes_20250212.csv", parser);

    // 4) Example: bounding-box query
    float minLat = 40.0f, maxLat = 41.0f;
    float minLon = -74.5f, maxLon = -73.0f;

    auto matches = dataset.rangeQuery(minLat, maxLat, minLon, maxLon);
    std::cout << "rangeQuery found " << matches.size()
              << " records in bounding box.\n";

    // 5) Print final size
    std::cout << "Total records loaded: " << dataset.size() << "\n";

     auto serious = dataset.findNoOfRecordsWithMinInjured(7);
   std::cout << "Found " << serious.size() << " collisions with >=7 injuries.\n";



    // 2) searchByBorough
    auto bk = dataset.searchByBorough("BROOKLYN");
    std::cout << "searchByBorough(\"BROOKLYN\") -> " << bk.size() << " records.\n";

    // 3) searchByZIP
    auto zipMatches = dataset.searchByZIP("11208");
    std::cout << "searchByZIP(\"11208\") -> " << zipMatches.size() << " records.\n";

    // 4) searchByDateRange
    auto dateRange = dataset.searchByDateRange("2021-01-01", "2021-12-31");
    std::cout << "searchByDateRange(2021-01-01, 2021-12-31) -> " << dateRange.size() << " records.\n";

    // 5) searchByVehicleType

    {
       auto start = std::chrono::high_resolution_clock::now();
       int sedans = dataset.searchByVehicleType("Sedan");
       std::cout << "searchByVehicleType(\"Sedan\") -> " << sedans << " records.\n";
       auto end = std::chrono::high_resolution_clock::now();
       double dur = std::chrono::duration<double>(end - start).count();
      std::cout << "searchByVehicleType time: " << dur << " seconds\n";
    }
   

    // 6) searchByInjuryRange
    auto inj2to5 = dataset.searchByInjuryRange(2, 5);
    std::cout << "searchByInjuryRange(2,5) -> " << inj2to5.size() << " records.\n";



     auto killed = dataset.findNoOfRecordsWithMotoristKilled(1);
   std::cout << "Found " << killed.size() << " collisions with killed motorists greater than one\n";

// 7) searchByFatalitiesRange
    auto fatal1to3 = dataset.searchByFatalitiesRange(1, 3);
    std::cout << "searchByFatalitiesRange(1,3) -> " << fatal1to3.size() << " records.\n";


    // 7) searchByFatalitiesRange
    auto fatal1to3a = dataset.searchByPedestrianFatalitiesRange(1, 3);
    std::cout << "searchByPedestrianFatalitiesRange(1,3) -> " << fatal1to3a.size() << " records.\n";


// 7) searchByFatalitiesRange
    auto fatal1to3b = dataset.searchByCyclistFatalitiesRange(1, 3);
    std::cout << "searchByMotoristFatalitiesRange(1,3) -> " << fatal1to3b.size() << " records.\n";



// 7) searchByFatalitiesRange
    auto fatal1to3c = dataset.searchByMotoristFatalitiesRange(1, 3);
    std::cout << "searchByMotoristFatalitiesRange(1,3) -> " << fatal1to3c.size() << " records.\n";


    // 8) searchByUniqueKey
    auto keyMatches = dataset.searchByUniqueKey(4456314);
    std::cout << "searchByUniqueKey(4456314) -> " << keyMatches.size() << " records.\n";

    return 0;
}
