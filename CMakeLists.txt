cmake_minimum_required(VERSION 3.10)
project(nycollision VERSION 1.0.0 LANGUAGES CXX)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Find OpenMP (optional)
find_package(OpenMP)

# Enable optimizations and vectorization
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -mcpu=native")
    if(OpenMP_CXX_FOUND)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
        add_definitions(-D_OPENMP)
    endif()
elseif(MSVC)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /O2 /arch:AVX2")
    if(OpenMP_CXX_FOUND)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
        add_definitions(-D_OPENMP)
    endif()
endif()

# Library source files
set(LIB_SOURCES
    src/DataSet.cpp
    src/CSVParser.cpp
    src/VectorizedDataSet.cpp
)

# Library headers by category
set(CORE_HEADERS
    include/nycollision/core/Types.h
    include/nycollision/core/IRecord.h
    include/nycollision/core/Record.h
)

set(DATA_HEADERS
    include/nycollision/data/IDataSet.h
    include/nycollision/data/DataSet.h
    include/nycollision/data/VectorizedDataSet.h
)

set(PARSER_HEADERS
    include/nycollision/parser/IParser.h
    include/nycollision/parser/CSVParser.h
)

set(UTIL_HEADERS
    include/nycollision/util/CollisionAnalyzer.h
)

# Combine all headers
set(LIB_HEADERS
    ${CORE_HEADERS}
    ${DATA_HEADERS}
    ${PARSER_HEADERS}
    ${UTIL_HEADERS}
)

# Create library target
add_library(nycollision ${LIB_SOURCES} ${LIB_HEADERS})
target_include_directories(nycollision
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

# Link OpenMP if found
if(OpenMP_CXX_FOUND)
    target_link_libraries(nycollision PUBLIC OpenMP::OpenMP_CXX)
endif()

# Example executable
add_executable(collision_example main.cpp)
target_link_libraries(collision_example PRIVATE nycollision)
if(OpenMP_CXX_FOUND)
    target_link_libraries(collision_example PRIVATE OpenMP::OpenMP_CXX)
endif()

# Installation rules
include(GNUInstallDirs)
install(TARGETS nycollision
    EXPORT nycollision-targets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

install(DIRECTORY include/nycollision
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    FILES_MATCHING PATTERN "*.h"
)

install(EXPORT nycollision-targets
    FILE nycollision-targets.cmake
    NAMESPACE nycollision::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/nycollision
)

# Generate and install package configuration
include(CMakePackageConfigHelpers)
write_basic_package_version_file(
    "nycollision-config-version.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)

configure_package_config_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/nycollision-config.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/nycollision-config.cmake"
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/nycollision
)

install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/nycollision-config.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/nycollision-config-version.cmake"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/nycollision
)

# Export targets from build tree
export(EXPORT nycollision-targets
    FILE "${CMAKE_CURRENT_BINARY_DIR}/nycollision-targets.cmake"
    NAMESPACE nycollision::
)

# Create package configuration for build tree
configure_file(cmake/nycollision-config.cmake.in
    "${CMAKE_CURRENT_BINARY_DIR}/nycollision-config.cmake"
    @ONLY
)
