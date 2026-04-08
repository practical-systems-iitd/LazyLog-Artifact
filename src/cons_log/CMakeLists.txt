set(CONS_LOG_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/cons_log.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/cons_svr.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/grpc_cl_transport.cc)

add_executable(conssvr ${CONS_LOG_SRC})

target_link_libraries(conssvr
    glog::glog
    lazylogcli
    backendcli
    gRPC::grpc++
    protobuf::libprotobuf)

add_subdirectory(storage)
