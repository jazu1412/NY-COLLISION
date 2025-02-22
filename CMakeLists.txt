cmake_minimum_required(VERSION 3.10)
project(nycollision VERSION 1.0.0 LANGUAGES CXX)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Find required packages
find_package(OpenMP REQUIRED)
find_package(Boost 1.71.0 REQUIRED)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")

# Library source files
set(LIB_SOURCES
    src/DataSet.cpp
    src/CSVParser.cpp
)

# Library headers
set(CORE_HEADERS
    include/nycollision/core/Types.h
    include/nycollision/core/IRecord.h
    include/nycollision/core/Record.h
)

set(DATA_HEADERS
    include/nycollision/data/IDataSet.h
    include/nycollision/data/DataSet.h
)

set(PARSER_HEADERS
    include/nycollision/parser/IParser.h
    include/nycollision/parser/CSVParser.h
)

set(UTIL_HEADERS
    include/nycollision/util/CollisionAnalyzer.h
)

set(LIB_HEADERS
    ${CORE_HEADERS}
    ${DATA_HEADERS}
    ${PARSER_HEADERS}
    ${UTIL_HEADERS}
)

# Create library
add_library(nycollision ${LIB_SOURCES} ${LIB_HEADERS})
target_include_directories(nycollision
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

# Link dependencies
target_link_libraries(nycollision 
    PUBLIC 
        OpenMP::OpenMP_CXX
        Boost::boost
)

# Create example executable
add_executable(collision_example main.cpp)
target_link_libraries(collision_example PRIVATE nycollision OpenMP::OpenMP_CXX)  # ✅ Add OpenMP
