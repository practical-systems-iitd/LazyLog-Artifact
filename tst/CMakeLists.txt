add_executable(lazylog_test
    log_entry_test.cc
    ${CMAKE_SOURCE_DIR}/src/rpc/log_entry.cc
    ${CMAKE_SOURCE_DIR}/src/dur_log/dur_log.cc)

target_link_libraries(lazylog_test
    absl::flat_hash_map
    GTest::gtest_main
    lazylogcli
)

target_include_directories(lazylog_test PRIVATE ${CMAKE_BINARY_DIR}/src)
include(GoogleTest)
gtest_discover_tests(lazylog_test)
