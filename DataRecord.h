#pragma once
#include <string>

/**
 * @brief Represents one row/record from the CSV data.
 */
class DataRecord {
public:
    // Example fields: Adjust to your CSV
    std::string crash_date;
    std::string crash_time;
    std::string borough;
    std::string zip_code;

    float latitude = 0.0f;
    float longitude = 0.0f;
    std::string location;

    std::string on_street_name;
    std::string cross_street_name;
    std::string off_street_name;

    int number_of_persons_injured       = 0;
    int number_of_persons_killed        = 0;
    int number_of_pedestrians_injured   = 0;
    int number_of_pedestrians_killed    = 0;
    int number_of_cyclist_injured       = 0;
    
    // 16) NUMBER OF CYCLIST KILLED
    int number_of_cyclist_killed = 0;

    // 17) NUMBER OF MOTORIST INJURED
    int number_of_motorist_injured = 0;

    // 18) NUMBER OF MOTORIST KILLED
    int number_of_motorist_killed = 0;

    // 19) CONTRIBUTING FACTOR VEHICLE 1
    std::string contributing_factor_vehicle_1;

    // 20) CONTRIBUTING FACTOR VEHICLE 2
    std::string contributing_factor_vehicle_2;

    // 21) CONTRIBUTING FACTOR VEHICLE 3
    std::string contributing_factor_vehicle_3;

    // 22) CONTRIBUTING FACTOR VEHICLE 4
    std::string contributing_factor_vehicle_4;

    // 23) CONTRIBUTING FACTOR VEHICLE 5
    std::string contributing_factor_vehicle_5;

    // 24) UNIQUE KEY
    //   Often an integer (e.g. "4456314"), 
    //   but some folks keep it as a string if it can’t be strictly guaranteed numeric.
    int unique_key = 0;

    // 25) VEHICLE TYPE CODE 1
    std::string vehicle_type_code_1;

    // 26) VEHICLE TYPE CODE 2
    std::string vehicle_type_code_2;

    // 27) VEHICLE TYPE CODE 3
    std::string vehicle_type_code_3;

    // 28) VEHICLE TYPE CODE 4
    std::string vehicle_type_code_4;

    // 29) VEHICLE TYPE CODE 5
    std::string vehicle_type_code_5;
   
};
