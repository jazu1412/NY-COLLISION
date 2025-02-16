# NY Collision Data Analysis Library

A C++ library for analyzing New York City collision data with an emphasis on object-oriented design, abstraction, and efficient querying.

## Features

- Modern C++17 implementation
- Clean object-oriented design with proper abstraction
- Efficient data structures for various query types
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
- `DataSet`: Efficient implementation with multiple indices
- `CSVParser`: CSV parsing with quote handling
- `CollisionAnalyzer`: Facade for easy library usage

## Building

```bash
# Create build directory
mkdir build && cd build

# Configure
cmake ..

# Build
cmake --build .

# Install (optional)
sudo cmake --install .
```

## Usage Example

```cpp
#include <nycollision/CollisionAnalyzer.h>
#include <iostream>

int main() {
    nycollision::CollisionAnalyzer analyzer;
    
    // Load collision data
    analyzer.loadData("collision_data.csv");
    
    // Find collisions in Brooklyn
    auto collisions = analyzer.findCollisionsInBorough("BROOKLYN");
    
    // Print results
    for (const auto& collision : collisions) {
        std::cout << "Date: " << collision->getDateTime().date
                  << " Location: " << collision->getBorough()
                  << " Injuries: " << collision->getCasualtyStats().getTotalInjuries()
                  << "\n";
    }
}
```

## Using as a Library

To use this library in your CMake project:

```cmake
find_package(nycollision REQUIRED)
target_link_libraries(your_target PRIVATE nycollision::nycollision)
```

## Directory Structure

```
nycollision/
├── include/nycollision/    # Public headers
│   ├── Types.h            # Common types and structures
│   ├── IRecord.h          # Record interface
│   ├── Record.h           # Record implementation
│   ├── IDataSet.h         # Dataset interface
│   ├── DataSet.h          # Dataset implementation
│   ├── IParser.h          # Parser interface
│   ├── CSVParser.h        # CSV parser implementation
│   └── CollisionAnalyzer.h # Facade class
├── src/                    # Implementation files
│   ├── DataSet.cpp
│   └── CSVParser.cpp
├── examples/               # Example code
├── tests/                  # Unit tests
└── docs/                   # Documentation
```

## Performance Considerations

The library uses several optimization techniques:

- Multiple indices for O(1) or O(log n) query performance
- Spatial indexing for geographic queries
- Smart pointers for memory management
- Const correctness for thread safety
- Move semantics for efficient data handling

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
