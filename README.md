# NY Collision Data Analysis Library

### Key Components

- `IRecord`: Interface for collision records
- `IDataSet`: Interface for querying collision data
- `IParser`: Interface for data parsing
- `DataSet`: Original implementation with multiple indices
- `VectorizedDataSet`: Optimized implementation using Structure of Arrays (SoA)
- `CSVParser`: CSV parsing with quote handling
- `CollisionAnalyzer`: Facade for easy library usage

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
│       │   ├── AlignedAllocator.h
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
