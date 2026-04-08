set(DUR_LOG_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/dur_log.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/dur_log_mock.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/dur_svr.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/grpc_dl_transport.cc)

add_executable(dursvr ${DUR_LOG_SRC})

target_link_libraries(dursvr
    absl::flat_hash_map
    glog::glog
    lazylogcli
    gRPC::grpc++
    protobuf::libprotobuf)
