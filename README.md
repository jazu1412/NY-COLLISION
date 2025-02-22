# NY Collision Data Analysis Library

A C++ library for processing and analyzing New York collision data with emphasis on performance and object-oriented design principles.

## Overview

This library provides a robust framework for processing collision data through an object-oriented design approach. It features a serial processing implementation (Phase 1) and a parallel processing enhancement (Phase 2) using OpenMP.

## Features

- Object-oriented design with emphasis on abstraction
- CSV data parsing with type-safe field representation
- Efficient data storage and retrieval mechanisms
- Range-based search capabilities
- Performance benchmarking infrastructure
- Parallel processing support (Phase 2)

## Dependencies

- C++17 or higher
- CMake 3.12 or higher
- OpenMP (for Phase 2 parallel processing)

## Building and Running the Project

### Building
To build the project from scratch:

```bash
rm -rf build && mkdir build && cd build && cmake .. && make
```

### Running the Application
To run the application with collision data:

```bash
./collision_example Motor_Vehicle_Collisions_-_Crashes_20250212.csv
```

## Project Structure

```
.
├── CMakeLists.txt                      # Main CMake configuration
├── cmake/
│   └── nycollision-config.cmake.in     # CMake package configuration
├── include/
│   └── nycollision/
│       ├── core/                       # Core data structures
│       │   ├── IRecord.h              # Record interface
│       │   ├── Record.h               # Concrete record implementation
│       │   └── Types.h                # Type definitions
│       ├── data/                      # Data management
│       │   ├── DataSet.h             # Dataset container
│       │   └── IDataSet.h            # Dataset interface
│       ├── parser/                    # Data parsing
│       │   ├── CSVParser.h           # CSV parser implementation
│       │   └── IParser.h             # Parser interface
│       └── util/                      # Utility functions
│           └── CollisionAnalyzer.h    # Analysis tools
└── src/                               # Implementation files
    ├── CSVParser.cpp
    └── DataSet.cpp
```

## API Documentation

### Core Components

#### IRecord Interface
```cpp
class IRecord {
    // Abstract interface for collision records
    virtual void setField(const std::string& field, const std::string& value) = 0;
    virtual std::string getField(const std::string& field) const = 0;
};
```

#### DataSet Interface
```cpp
class IDataSet {
    // Interface for dataset operations
    virtual void addRecord(std::unique_ptr<IRecord> record) = 0;
    virtual const IRecord* getRecord(size_t index) const = 0;
    virtual size_t size() const = 0;
};
```

#### Parser Interface
```cpp
class IParser {
    // Interface for data parsing
    virtual std::unique_ptr<IDataSet> parse(const std::string& filename) = 0;
};
```

### Usage Example

```cpp
#include <nycollision/parser/CSVParser.h>
#include <nycollision/util/CollisionAnalyzer.h>

int main() {
    // Create parser instance
    CSVParser parser;
    
    // Parse data file
    auto dataset = parser.parse("collision_data.csv");
    
    // Create analyzer
    CollisionAnalyzer analyzer(dataset.get());
    
    // Perform analysis
    auto results = analyzer.analyzeTimeRange("2020-01-01", "2020-12-31");
}
```

## Performance Benchmarking

### Phase 1: Serial Processing

The initial implementation focuses on efficient serial processing with the following benchmarks:

1. Data Loading Performance
   - CSV parsing speed
   - Memory usage optimization
   - Data structure initialization

2. Search Operations
   - Range queries
   - Field-specific searches
   - Aggregate operations

### Phase 2: Parallel Processing

The parallel implementation using OpenMP demonstrates:

1. Data Processing Speedup
   - Multi-threaded parsing
   - Concurrent search operations
   - Load balancing strategies

2. Performance Comparison
   - Thread scaling efficiency
   - Memory usage impact
   - Operation latency improvements

## Implementation Details

### Phase 1: Serial Implementation

The serial implementation emphasizes:
- Clean object-oriented design
- Efficient data structures
- Type-safe field representation
- Minimal memory overhead
- Optimized search algorithms

Key design patterns used:
- Factory Pattern: For record creation
- Strategy Pattern: For parsing strategies
- Facade Pattern: For simplified API access

### Phase 2: Parallel Enhancement

Parallel processing improvements include:
- OpenMP parallel sections for data parsing

