#include <iostream>
#include "MyDataSet.h"
#include "QuoteAwareCSVParser.h"

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

     auto killed = dataset.findNoOfRecordsWithMotoristKilled(1);
   std::cout << "Found " << killed.size() << "collisions with killed motorists greater than one\n";

    return 0;
}
