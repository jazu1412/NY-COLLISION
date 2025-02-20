# NY Collision Data Analysis Library

A C++ library for analyzing New York City collision data with an emphasis on object-oriented design, abstraction, efficient querying, and optimized performance.

## Features

- Modern C++17 implementation
- Clean object-oriented design with proper abstraction
- Efficient data structures with vectorized organization
- Thread-safe immutable records
- Flexible CSV parsing with quote handling
- Comprehensive query capabilities:
  - Geographic bounding box searches
  - Borough and ZIP code filtering
  - Date range queries
  - Vehicle type searches
  - Casualty statistics filtering
  - Unique key lookups

## Architecture

The library follows SOLID principles and implements several design patterns:

- **Facade Pattern**: `CollisionAnalyzer` provides a simplified interface
- **Strategy Pattern**: Flexible parsing through `IParser` interface
- **Interface Segregation**: Clear separation of concerns with interfaces
- **Dependency Injection**: Parser and dataset implementations are injectable

### Key Components

- `IRecord`: Interface for collision records
- `IDataSet`: Interface for querying collision data
- `IParser`: Interface for data parsing
- `DataSet`: Original implementation with multiple indices
- `VectorizedDataSet`: Optimized implementation using Structure of Arrays (SoA)
- `CSVParser`: CSV parsing with quote handling
- `CollisionAnalyzer`: Facade for easy library usage

## Project Phases

### Phase 1: Basic Implementation
- Core data structures for collision records
- CSV parser for data ingestion
- Basic query functionality
- Project structure and build system setup
- Abstract interfaces (IRecord, IDataSet, IParser)
- Concrete class implementations
- Enhanced modularity and extensibility
- Configuration management

### Phase 3: Performance Optimization
- **Vectorized Data Organization**
  - Implemented Object of Arrays (OoA) design in VectorizedDataSet
  - Optimized memory layout for cache efficiency
  - Aligned data for SIMD operations
  - Reduced memory fragmentation
  


## Directory Structure

```
nycollision/
├── CMakeLists.txt
├── main.cpp
├── cmake/
│   └── nycollision-config.cmake.in
├── docs/
├── examples/
├── include/
│   └── nycollision/
│       ├── core/
│       │   ├── Config.h
│       │   ├── IRecord.h
│       │   ├── Record.h
│       │   └── Types.h
│       ├── data/
│       │   ├── DataSet.h
│       │   ├── IDataSet.h
│       │   └── VectorizedDataSet.h
│       ├── parser/
│       │   ├── CSVParser.h
│       │   └── IParser.h
│       └── util/
│           └── CollisionAnalyzer.h
├── src/
│   ├── CSVParser.cpp
│   ├── DataSet.cpp
│   └── VectorizedDataSet.cpp
└── tests/
```


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
