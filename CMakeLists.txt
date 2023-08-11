cmake_minimum_required(VERSION 3.18)

project(sort)

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -O3")

add_executable(sort ${CMAKE_CURRENT_SOURCE_DIR}/sort.cpp)
target_link_libraries(sort PRIVATE tbb)