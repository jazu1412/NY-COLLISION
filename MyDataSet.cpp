#include "MyDataSet.h"
#include <fstream>
#include <iostream>
#include <cstdlib> // std::stoi, std::stof

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


