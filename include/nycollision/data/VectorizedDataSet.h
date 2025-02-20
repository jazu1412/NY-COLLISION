#pragma once
#include "../core/Types.h"
#include "../core/Record.h"
#include "IDataSet.h"
#include "../parser/IParser.h"
#include <vector>
#include <string>
#include <memory>
#include <unordered_map>
#include <map>

namespace nycollision {

/**
 * @brief Optimized dataset using Object-of-Arrays (SoA) pattern for better cache locality
 * 
 * This implementation uses parallel arrays for better memory locality and SIMD optimization potential.
 * String data is pooled to reduce memory fragmentation and improve cache utilization.
 */
class VectorizedDataSet : public IDataSet {
public:
    using Records = std::vector<std::shared_ptr<const IRecord>>;
    using RecordPtr = std::shared_ptr<const IRecord>;

    // Spatial index cell structure
    struct GridCell {
        float minLat, maxLat;
        float minLon, maxLon;
        std::vector<size_t> indices;
    };

    /**
     * @brief Constructor that initializes OpenMP settings
     */
    VectorizedDataSet();

    /**
     * @brief Load records from a file using the specified parser
     */
    void loadFromFile(const std::string& filename, const IParser& parser);

    // IDataSet interface implementation
    Records queryByGeoBounds(float minLat, float maxLat, float minLon, float maxLon) const override;
    Records queryByBorough(const std::string& borough) const override;
    Records queryByZipCode(const std::string& zipCode) const override;
    Records queryByDateRange(const Date& start, const Date& end) const override;
    Records queryByVehicleType(const std::string& vehicleType) const override;
    Records queryByInjuryRange(int minInjuries, int maxInjuries) const override;
    Records queryByFatalityRange(int minFatalities, int maxFatalities) const override;
    RecordPtr queryByUniqueKey(int key) const override;
    Records queryByPedestrianFatalities(int minFatalities, int maxFatalities) const override;
    Records queryByCyclistFatalities(int minFatalities, int maxFatalities) const override;
    Records queryByMotoristFatalities(int minFatalities, int maxFatalities) const override;
    
    size_t size() const override { return unique_keys.size(); }

private:
    // Helper to create a Record object from vectorized data at given index
    std::shared_ptr<Record> createRecord(size_t index) const;
    
    // Helper to convert indices to Records
    Records createRecordsFromIndices(const std::vector<size_t>& indices) const;

    // Helper to add a string to a pool and return its index
    size_t addToStringPool(const std::string& str, std::vector<std::string>& pool);

    // Vectorized storage (SoA pattern) with cache line alignment
    alignas(64) std::vector<int> unique_keys;
    
    // Location data - group frequently accessed data together
    struct alignas(64) LocationData {
        std::vector<float> latitudes;
        std::vector<float> longitudes;
        std::vector<std::string> boroughs;
        std::vector<std::string> zip_codes;
        std::vector<std::string> on_streets;
        std::vector<std::string> cross_streets;
        std::vector<std::string> off_streets;
    } location_data;
    
    // Date/Time data
    struct alignas(64) TimeData {
        std::vector<std::string> dates;
        std::vector<std::string> times;
    } time_data;
    
    // Casualty data grouped for SIMD operations
    struct alignas(64) CasualtyData {
        std::vector<int> persons_injured;
        std::vector<int> persons_killed;
        std::vector<int> pedestrians_injured;
        std::vector<int> pedestrians_killed;
        std::vector<int> cyclists_injured;
        std::vector<int> cyclists_killed;
        std::vector<int> motorists_injured;
        std::vector<int> motorists_killed;
    } casualty_data;
    
    // Vehicle data grouped for better cache locality
    struct alignas(64) VehicleData {
        std::vector<std::vector<size_t>> type_indices;
        std::vector<std::vector<size_t>> factor_indices;
        std::vector<std::string> type_pool;
        std::vector<std::string> factor_pool;
    } vehicle_data;
    
    // Indices for efficient querying
    std::unordered_map<int, size_t> key_to_index;
    std::unordered_map<std::string, std::vector<size_t>> borough_index;
    std::unordered_map<std::string, std::vector<size_t>> zip_index;
    std::map<Date, std::vector<size_t>> date_index;
    
    // Spatial index using grid-based approach
    std::vector<GridCell> spatial_grid;
    
    // Range indices
    std::map<int, std::vector<size_t>> injury_index;
    std::map<int, std::vector<size_t>> fatality_index;
    std::map<int, std::vector<size_t>> pedestrian_fatality_index;
    std::map<int, std::vector<size_t>> cyclist_fatality_index;
    std::map<int, std::vector<size_t>> motorist_fatality_index;
    
    // Vehicle type index
    std::unordered_map<std::string, std::vector<size_t>> vehicle_type_index;
};

} // namespace nycollision
