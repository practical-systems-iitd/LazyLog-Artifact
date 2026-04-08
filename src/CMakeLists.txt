set(PROTO_SRC ${CMAKE_CURRENT_SOURCE_DIR}/proto/lazylog.proto)
set(PROTO_HDRS ${CMAKE_CURRENT_BINARY_DIR}/lazylog.pb.h ${CMAKE_CURRENT_BINARY_DIR}/lazylog.grpc.pb.h)
set(PROTO_SRCS ${CMAKE_CURRENT_BINARY_DIR}/lazylog.pb.cc ${CMAKE_CURRENT_BINARY_DIR}/lazylog.grpc.pb.cc)

find_program(_GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)

# Generate gRPC and Protobuf sources
add_custom_command(
      OUTPUT ${PROTO_SRCS} ${PROTO_HDRS}
      COMMAND protoc
      ARGS --grpc_out=${CMAKE_CURRENT_BINARY_DIR}
        --cpp_out=${CMAKE_CURRENT_BINARY_DIR}
        -I ${CMAKE_CURRENT_SOURCE_DIR}/proto
        --plugin=protoc-gen-grpc=${_GRPC_CPP_PLUGIN_EXECUTABLE}
        ${PROTO_SRC}
      DEPENDS ${PROTO_SRC}
)

include_directories(${CMAKE_CURRENT_BINARY_DIR})

add_library(lazylogcli STATIC
    ${PROTO_SRCS}
    ${PROTO_HDRS}
    ${CMAKE_CURRENT_SOURCE_DIR}/dur_log/dur_log_grpc_cli.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/cons_log/cons_log_grpc_cli.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/client/lazylog_cli.cc)

target_link_libraries(lazylogcli
    PUBLIC glog::glog
    PUBLIC gRPC::grpc++
    PUBLIC protobuf::libprotobuf
)

option(CORFU "Enable CORFU" OFF)

if(CORFU)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DCORFU")
endif(CORFU)

add_subdirectory(client)
add_subdirectory(dur_log)
add_subdirectory(cons_log)
add_subdirectory(rpc)
add_subdirectory(utils)
# add_subdirectory(benchmark)
# add_subdirectory(app)