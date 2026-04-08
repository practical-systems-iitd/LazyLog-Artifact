cmake_minimum_required(VERSION 3.14)

project(LazyLog)
set(CMAKE_CXX_STANDARD 17)
set(ABSL_PROPAGATE_CXX_STD ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

include(FetchContent)

FetchContent_Declare(
    glog
    GIT_REPOSITORY https://github.com/google/glog.git
    GIT_TAG v0.6.0
)
FetchContent_MakeAvailable(glog)

FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG release-1.12.1
)
FetchContent_MakeAvailable(googletest)

find_package(Threads REQUIRED)

find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)

include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/cppkafka/include)

add_subdirectory(abseil-cpp)
add_subdirectory(HdrHistogram_c)
add_subdirectory(cppkafka)
add_subdirectory(src)
add_subdirectory(tst)
