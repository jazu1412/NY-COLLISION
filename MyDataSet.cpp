#include "MyDataSet.h"
#include <fstream>
#include <iostream>
#include <cstdlib> // std::stoi, std::stof
#include <chrono>
#include <omp.h>

// safe integer parse
static int parseInt(const std::string &s) {
    try {
        return std::stoi(s);
    } catch (...) {
        return 0;
    }
}

// safe float parse
static float parseFloat(const std::string &s) {
    try {
        return std::stof(s);
    } catch (...) {
        return 0.0f;
    }
}

void MyDataSet::loadFromCSV(const std::string &filename, const ICSVParser &parser) {
    std::ifstream file(filename);
    if (!file.is_open()) {
        std::cerr << "Error: Cannot open file " << filename << "\n";
        return;
    }

    // Optionally read/discard header row
    std::string header;
    if (std::getline(file, header)) {
        // parse or skip
    }

    std::string line;
    while (std::getline(file, line)) {
        if (line.empty()) continue;

        // parseLine from the provided CSV parser
        auto tokens = parser.parseLine(line);

        // ensure enough columns
        if (tokens.size() < 15) {
            // skip or handle
            continue;
        }

        DataRecord rec;
        // map columns: adjust indexes to your CSV's real layout
        rec.crash_date  = tokens[0];
        rec.crash_time  = tokens[1];
        rec.borough     = tokens[2];
        rec.zip_code    = tokens[3];

        rec.latitude    = parseFloat(tokens[4]);
        rec.longitude   = parseFloat(tokens[5]);
        rec.location    = tokens[6];

        rec.on_street_name     = tokens[7];
        rec.cross_street_name  = tokens[8];
        rec.off_street_name    = tokens[9];

        rec.number_of_persons_injured       = parseInt(tokens[10]);
        rec.number_of_persons_killed        = parseInt(tokens[11]);
        rec.number_of_pedestrians_injured   = parseInt(tokens[12]);
        rec.number_of_pedestrians_killed    = parseInt(tokens[13]);
        rec.number_of_cyclist_injured       = parseInt(tokens[14]);


        // 15) NUMBER OF CYCLIST KILLED
    rec.number_of_cyclist_killed        = parseInt(tokens[15]);

    // 16) NUMBER OF MOTORIST INJURED
    rec.number_of_motorist_injured      = parseInt(tokens[16]);

    // 17) NUMBER OF MOTORIST KILLED
    rec.number_of_motorist_killed       = parseInt(tokens[17]);

    // 18) CONTRIBUTING FACTOR VEHICLE 1
    rec.contributing_factor_vehicle_1   = tokens[18];

    // 19) CONTRIBUTING FACTOR VEHICLE 2
    rec.contributing_factor_vehicle_2   = tokens[19];

    // 20) CONTRIBUTING FACTOR VEHICLE 3
    rec.contributing_factor_vehicle_3   = tokens[20];

    // 21) CONTRIBUTING FACTOR VEHICLE 4
    rec.contributing_factor_vehicle_4   = tokens[21];

    // 22) CONTRIBUTING_FACTOR VEHICLE 5
    rec.contributing_factor_vehicle_5   = tokens[22];

    // 23) UNIQUE KEY (often numeric, but verify your CSV)
    rec.unique_key = parseInt(tokens[23]);

    // 24) VEHICLE TYPE CODE 1
    rec.vehicle_type_code_1 = tokens[24];

    // 25) VEHICLE TYPE CODE 2
    rec.vehicle_type_code_2 = tokens[25];

    // 26) VEHICLE TYPE CODE 3
    rec.vehicle_type_code_3 = tokens[26];

    // 27) VEHICLE TYPE CODE 4
    rec.vehicle_type_code_4 = tokens[27];

    // 28) VEHICLE TYPE CODE 5
    rec.vehicle_type_code_5 = tokens[28];

        records_.push_back(rec);
    }

    file.close();
    std::cout << "Finished loading " << records_.size()
              << " records from " << filename << std::endl;
}

std::vector<DataRecord> MyDataSet::rangeQuery(
    float minLat, float maxLat,
    float minLon, float maxLon
) const {
    std::vector<DataRecord> results;
    results.reserve(records_.size());

    for (const auto &rec : records_) {
        if (rec.latitude >= minLat && rec.latitude <= maxLat &&
            rec.longitude >= minLon && rec.longitude <= maxLon) {
            results.push_back(rec);
        }
    }
    return results;
}

std::vector<DataRecord> MyDataSet::findNoOfRecordsWithMinInjured(
       int minInjured
    ) const{
std::vector<DataRecord> results;
    results.reserve(records_.size());

    for (const auto &rec : records_) {
        if (rec.number_of_persons_injured>=minInjured) {
            results.push_back(rec);
  //  std::cout << "min injury 3 date is : " <<  rec.crash_date << " and lat and longitude is "<<rec.latitude <<"  "<< rec.longitude <<" and no of person injured is " << rec.number_of_persons_injured <<"\n";

        }
    }
    return results;




    }


std::vector<DataRecord> MyDataSet::findNoOfRecordsWithMotoristKilled(
       int motoristsKilled
    ) const{
std::vector<DataRecord> results;
    results.reserve(records_.size());

    for (const auto &rec : records_) {
        if (rec.number_of_motorist_killed > motoristsKilled) {
            results.push_back(rec);
  //  std::cout << "min injury 3 date is : " <<  rec.crash_date << " and lat and longitude is "<<rec.latitude <<"  "<< rec.longitude <<" and no of person injured is " << rec.number_of_persons_injured <<"\n";

        }
    }
    return results;




    }



/******************************************************
 * 2) searchByBorough
 ******************************************************/
std::vector<DataRecord> MyDataSet::searchByBorough(const std::string &boroughName) const {
    std::vector<DataRecord> results;
    results.reserve(records_.size());

    for (const auto &r : records_) {
        // case-sensitive match
        if (r.borough == boroughName) {
            results.push_back(r);
        }
    }
    return results;
}

/******************************************************
 * 3) searchByZIP
 ******************************************************/
std::vector<DataRecord> MyDataSet::searchByZIP(const std::string &zip) const {
    std::vector<DataRecord> results;
    results.reserve(records_.size());

    for (const auto &r : records_) {
        if (r.zip_code == zip) {
            results.push_back(r);
        }
    }
    return results;
}

/******************************************************
 * 4) searchByDateRange
 ******************************************************
 * We assume date strings are comparable lexically,
 * e.g. "2021-09-11" < "2022-01-01".
 * If your date is "MM/DD/YYYY", or you want real date logic,
 * parse them into a date struct or use chrono.
 */
std::vector<DataRecord> MyDataSet::searchByDateRange(
    const std::string &startDate,
    const std::string &endDate
) const {
    std::vector<DataRecord> results;
    results.reserve(records_.size());

    for (const auto &r : records_) {
        if (r.crash_date >= startDate && r.crash_date <= endDate) {
            results.push_back(r);
        }
    }
    return results;
}

/******************************************************
 * 5) searchByVehicleType
 ******************************************************/
// int MyDataSet::searchByVehicleType(const std::string &vehicleType) const {
//      std::vector<DataRecord> results;
//     results.reserve(records_.size());
//    int totalCount = 0;

//     for (const auto &r : records_) {
//         // If any vehicle_type_code_* matches vehicleType
//         if (r.vehicle_type_code_1 == vehicleType ||
//             r.vehicle_type_code_2 == vehicleType ||
//             r.vehicle_type_code_3 == vehicleType ||
//             r.vehicle_type_code_4 == vehicleType ||
//             r.vehicle_type_code_5 == vehicleType)
//         {
//             totalCount++;
//         }
//     }
//     return totalCount;
// }

  int MyDataSet::searchByVehicleType(const std::string &vehicleType) const {
    // (A) Create a vector of partial results for each thread
    //     Each thread writes to partials[tid] to avoid data races.
    omp_set_num_threads(11);
    int nThreads = omp_get_max_threads();  // or #pragma omp parallel to get it
 std::vector<DataRecord> results;
  results.reserve(records_.size());
     std::cout << "In threads flow -- searchByVehicleType(\"Sedan\") -> no of threads is " << nThreads<< "  threads\n";

    // (B) Parallel region

    auto start = std::chrono::high_resolution_clock::now();
    
  int totalCount = 0;

    #pragma omp parallel for reduction(+:totalCount)
    for (size_t i = 0; i < records_.size(); i++) {
        const auto &r = records_[i];
        if (r.vehicle_type_code_1 == vehicleType ||
            r.vehicle_type_code_2 == vehicleType ||
            r.vehicle_type_code_3 == vehicleType ||
            r.vehicle_type_code_4 == vehicleType ||
            r.vehicle_type_code_5 == vehicleType)
        {
            totalCount++;  // each thread increments local counter,
                           // then OpenMP reduction adds them up
        }
    }
    

//     //  auto end = std::chrono::high_resolution_clock::now();
//     //    double dur = std::chrono::duration<double>(end - start).count();
//     //   std::cout << "Thread area over--> searchByVehicleType time: " << dur << " seconds\n";


return totalCount;

}

/******************************************************
 * 6) searchByInjuryRange
 ******************************************************/
std::vector<DataRecord> MyDataSet::searchByInjuryRange(int minInjury, int maxInjury) const {
    std::vector<DataRecord> results;
    results.reserve(records_.size());

    for (const auto &r : records_) {
        int totalInj = r.number_of_persons_injured
                     + r.number_of_pedestrians_injured
                     + r.number_of_cyclist_injured
                     + r.number_of_motorist_injured;

        if (totalInj >= minInjury && totalInj <= maxInjury) {
            results.push_back(r);
        }
    }
    return results;
}



/******************************************************
 * 7) searchByFatalitiesRange
 ******************************************************/
std::vector<DataRecord> MyDataSet::searchByFatalitiesRange(int minFatal, int maxFatal) const {
    std::vector<DataRecord> results;
    results.reserve(records_.size());

    for (const auto &r : records_) {
        int totalFat = r.number_of_persons_killed;
                   

        if (totalFat >= minFatal && totalFat <= maxFatal) {
            results.push_back(r);
        }
    }
    return results;
}

/******************************************************
 * 7) searchByFatalitiesRange
 ******************************************************/
std::vector<DataRecord> MyDataSet::searchByPedestrianFatalitiesRange(int minFatal, int maxFatal) const {
    std::vector<DataRecord> results;
    results.reserve(records_.size());

    for (const auto &r : records_) {
        int totalFat = r.number_of_pedestrians_killed;
                   

        if (totalFat >= minFatal && totalFat <= maxFatal) {
            results.push_back(r);
        }
    }
    return results;
}


/******************************************************
 * 7) searchByFatalitiesRange
 ******************************************************/
std::vector<DataRecord> MyDataSet::searchByCyclistFatalitiesRange(int minFatal, int maxFatal) const {
    std::vector<DataRecord> results;
    results.reserve(records_.size());

    for (const auto &r : records_) {
        int totalFat = r.number_of_cyclist_killed;
                   

        if (totalFat >= minFatal && totalFat <= maxFatal) {
            results.push_back(r);
        }
    }
    return results;
}


/******************************************************
 * 7) searchByFatalitiesRange
 ******************************************************/
std::vector<DataRecord> MyDataSet::searchByMotoristFatalitiesRange(int minFatal, int maxFatal) const {
    std::vector<DataRecord> results;
    results.reserve(records_.size());

    for (const auto &r : records_) {
        int totalFat = r.number_of_motorist_killed;
                   

        if (totalFat >= minFatal && totalFat <= maxFatal) {
            results.push_back(r);
        }
    }
    return results;
}

    /******************************************************
 * 8) searchByUniqueKey
 ******************************************************/
std::vector<DataRecord> MyDataSet::searchByUniqueKey(int key) const {
    std::vector<DataRecord> results;
    results.reserve(1); // typically 1 or 0 matches

    for (const auto &r : records_) {
        if (r.unique_key == key) {
            results.push_back(r);
              std::cout << "collision id based search data  is : " <<  r.crash_date << " and lat and longitude is "<<r.latitude <<"  "<< r.longitude <<" and no of person injured is " << r.number_of_persons_injured <<"\n";
            // break if you assume it's unique, or keep searching if collisions might share a key
            // break;
        }
    }
    return results;
}


