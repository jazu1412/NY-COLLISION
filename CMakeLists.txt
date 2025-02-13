cmake_minimum_required(VERSION 3.10)
project(MyDataProject)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -O2")

add_executable(my_program
    main.cpp
    MyDataSet.cpp
    QuoteAwareCSVParser.cpp
)
# These .hpp files are included automatically
