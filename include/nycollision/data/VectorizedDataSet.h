#pragma once

#include <vector>
#include <string>
#include <memory>
#include <algorithm>
#include <unordered_map>
#include <map>
#include <chrono>
#include <iostream>
#include <omp.h>

#include "nycollision/core/IRecord.h"
#include "nycollision/core/Record.h"
#include "nycollision/core/Config.h"
#include "nycollision/data/IDataSet.h"

namespace nycollision {
namespace data {

class VectorizedDataSet : public IDataSet {
public:
    VectorizedDataSet(size_t initialCapacity = 2'700'000) {
        reserve(initialCapacity);
        nycollision::config::initializeOpenMP();
    }

    void reserve(size_t newCapacity) {
        #pragma omp parallel sections
        {
            #pragma omp section
            names_.reserve(newCapacity);
            #pragma omp section
            emails_.reserve(newCapacity);
            #pragma omp section
            levels_.reserve(newCapacity);
            #pragma omp section
            ranks_.reserve(newCapacity);
            #pragma omp section
            keys_.reserve(newCapacity);
            #pragma omp section
            boroughs_.reserve(newCapacity);
            #pragma omp section
            zipCodes_.reserve(newCapacity);
        }
    }

    void loadFromFile(const std::string& filename, const IParser& parser) override {
        auto startTime = std::chrono::high_resolution_clock::now();

        std::ifstream file(filename);
        if (!file) {
            throw std::runtime_error("Failed to open file: " + filename);
        }

        std::string line;
        // Skip header
        std::getline(file, line);

        std::vector<std::string> lines;
        lines.reserve(2'700'000);
        while (std::getline(file, line)) {
            lines.push_back(std::move(line));
        }

        std::vector<std::shared_ptr<Record>> parsedRecords(lines.size());

        #pragma omp parallel for
        for (std::size_t i = 0; i < lines.size(); ++i) {
            parsedRecords[i] = parser.parseRecord(lines[i]);
        }

        // Sequentially add to maintain thread safety
        for (auto& rec : parsedRecords) {
            if (rec) {
                addRecord(rec);
            }
        }

        auto endTime = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> elapsedSeconds = endTime - startTime;
        std::cout << "VectorizedDataSet loadFromFile took " << elapsedSeconds.count() << " seconds.\n";
    }

    void addRecord(std::shared_ptr<Record> record) {
        names_.push_back(record->name);
        emails_.push_back(record->email);
        levels_.push_back(record->level);
        ranks_.push_back(record->rank);
        keys_.push_back(record->key);
        boroughs_.push_back(record->borough);
        zipCodes_.push_back(record->zipCode);

        // Update indices
        keyIndex_[record->key] = record;
        boroughIndex_[record->borough].push_back(record);
        zipIndex_[record->zipCode].push_back(record);
    }

    // Query methods with parallel processing
    Records queryByBorough(const std::string& borough) const override {
        auto it = boroughIndex_.find(borough);
        return it != boroughIndex_.end() ? convertToInterfaceRecords(it->second) : Records();
    }

    Records queryByZipCode(const std::string& zipCode) const override {
        auto it = zipIndex_.find(zipCode);
        return it != zipIndex_.end() ? convertToInterfaceRecords(it->second) : Records();
    }

    RecordPtr queryByUniqueKey(int key) const override {
        auto it = keyIndex_.find(key);
        return it != keyIndex_.end() ? std::static_pointer_cast<const IRecord>(it->second) : nullptr;
    }

    size_t size() const override { 
        return names_.size(); 
    }

private:
    // Contiguous storage for better cache performance
    std::vector<std::string> names_;
    std::vector<std::string> emails_;
    std::vector<int> levels_;
    std::vector<float> ranks_;
    std::vector<int> keys_;
    std::vector<std::string> boroughs_;
    std::vector<std::string> zipCodes_;

    // Indices for efficient querying
    std::unordered_map<int, std::shared_ptr<Record>> keyIndex_;
    std::unordered_map<std::string, std::vector<std::shared_ptr<Record>>> boroughIndex_;
    std::unordered_map<std::string, std::vector<std::shared_ptr<Record>>> zipIndex_;

    // Helper to convert internal records to interface records
    Records convertToInterfaceRecords(const std::vector<std::shared_ptr<Record>>& records) const {
        Records result(records.size());

        #pragma omp parallel for
        for (std::size_t i = 0; i < records.size(); ++i) {
            result[i] = std::static_pointer_cast<const IRecord>(records[i]);
        }

        return result;
    }
};

} // namespace data
} // namespace nycollision
