cmake_minimum_required(VERSION 3.20)
project(distributed_vector_search LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

find_package(gRPC REQUIRED)
find_package(OpenMP QUIET)

find_package(FAISS QUIET)

if(NOT TARGET FAISS::FAISS)
    set(_faiss_include_dirs "")
    set(_faiss_libraries "")

    if(DEFINED FAISS_INCLUDE_DIRS)
        set(_faiss_include_dirs "${FAISS_INCLUDE_DIRS}")
    elseif(DEFINED FAISS_INCLUDE_DIR)
        set(_faiss_include_dirs "${FAISS_INCLUDE_DIR}")
    endif()

    if(_faiss_include_dirs MATCHES "NOTFOUND")
        set(_faiss_include_dirs "")
    endif()

    if(DEFINED FAISS_LIBRARIES)
        set(_faiss_libraries "${FAISS_LIBRARIES}")
    elseif(DEFINED FAISS_LIBRARY)
        set(_faiss_libraries "${FAISS_LIBRARY}")
    endif()

    if(_faiss_libraries MATCHES "NOTFOUND")
        set(_faiss_libraries "")
    endif()

    if(_faiss_include_dirs AND _faiss_libraries)
        add_library(FAISS::FAISS INTERFACE IMPORTED)
        set_target_properties(FAISS::FAISS PROPERTIES
            INTERFACE_INCLUDE_DIRECTORIES "${_faiss_include_dirs}"
            INTERFACE_LINK_LIBRARIES "${_faiss_libraries}"
        )
    else()
        find_path(FAISS_INCLUDE_DIR faiss/Index.h
            HINTS /opt/homebrew/include /usr/local/include /usr/include)
        find_library(FAISS_LIBRARY NAMES faiss faiss_avx2
            HINTS /opt/homebrew/lib /usr/local/lib /usr/lib)
        if(FAISS_INCLUDE_DIR AND FAISS_LIBRARY)
            add_library(FAISS::FAISS INTERFACE IMPORTED)
            set_target_properties(FAISS::FAISS PROPERTIES
                INTERFACE_INCLUDE_DIRECTORIES "${FAISS_INCLUDE_DIR}"
                INTERFACE_LINK_LIBRARIES "${FAISS_LIBRARY}"
            )
            message(STATUS "FAISS located via manual search at ${FAISS_LIBRARY}")
        else()
            message(FATAL_ERROR "FAISS not found. Install faiss (e.g. brew install faiss) or set FAISS_INCLUDE_DIR/FAISS_LIBRARY.")
        endif()
    endif()
endif()

if(NOT OpenMP_CXX_FOUND AND CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang")
    find_path(LIBOMP_INCLUDE_DIR omp.h HINTS /opt/homebrew/opt/libomp/include /usr/local/opt/libomp/include)
    find_library(LIBOMP_LIBRARY omp HINTS /opt/homebrew/opt/libomp/lib /usr/local/opt/libomp/lib)
    if(LIBOMP_INCLUDE_DIR AND LIBOMP_LIBRARY)
        set(OpenMP_CXX_FOUND TRUE)
        set(OpenMP_CXX_FLAGS "-Xpreprocessor;-fopenmp")
        set(OpenMP_CXX_LIBRARIES ${LIBOMP_LIBRARY})
        set(OpenMP_CXX_INCLUDE_DIRS ${LIBOMP_INCLUDE_DIR})
    endif()
endif()

if(OpenMP_CXX_FOUND)
    if(NOT TARGET OpenMP::OpenMP_CXX)
        add_library(OpenMP::OpenMP_CXX INTERFACE IMPORTED)
        set_target_properties(OpenMP::OpenMP_CXX PROPERTIES
            INTERFACE_COMPILE_OPTIONS "${OpenMP_CXX_FLAGS}"
            INTERFACE_LINK_LIBRARIES "${OpenMP_CXX_LIBRARIES}"
            INTERFACE_INCLUDE_DIRECTORIES "${OpenMP_CXX_INCLUDE_DIRS}"
        )
    endif()
else()
    message(FATAL_ERROR "OpenMP not found. Install libomp (e.g. brew install libomp) or specify OpenMP paths.")
endif()

if(NOT TARGET protobuf::libprotobuf)
    find_package(Protobuf REQUIRED)
endif()

if(TARGET protobuf::libprotobuf)
    get_target_property(Protobuf_INCLUDE_DIRS protobuf::libprotobuf INTERFACE_INCLUDE_DIRECTORIES)
endif()

if(NOT Protobuf_INCLUDE_DIRS)
    if(DEFINED Protobuf_INCLUDE_DIR)
        set(Protobuf_INCLUDE_DIRS "${Protobuf_INCLUDE_DIR}")
    elseif(DEFINED PROTOBUF_INCLUDE_DIRS)
        set(Protobuf_INCLUDE_DIRS "${PROTOBUF_INCLUDE_DIRS}")
    endif()
endif()

if(Protobuf_INCLUDE_DIRS MATCHES "NOTFOUND")
    set(Protobuf_INCLUDE_DIRS "")
endif()

set(PROTO_SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/proto)
set(PROTO_GENERATED_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)

file(MAKE_DIRECTORY ${PROTO_GENERATED_DIR})

set(PROTO_FILES
    ${PROTO_SRC_DIR}/search.proto
    ${PROTO_SRC_DIR}/raft.proto
    ${PROTO_SRC_DIR}/config.proto
    ${PROTO_SRC_DIR}/mutation.proto)

set(PROTO_SRCS)
set(PROTO_HDRS)

foreach(proto ${PROTO_FILES})
    get_filename_component(proto_abs ${proto} ABSOLUTE)
    get_filename_component(proto_name ${proto_abs} NAME_WE)
    set(proto_src "${PROTO_GENERATED_DIR}/${proto_name}.pb.cc")
    set(proto_hdr "${PROTO_GENERATED_DIR}/${proto_name}.pb.h")
    set(grpc_src "${PROTO_GENERATED_DIR}/${proto_name}.grpc.pb.cc")
    set(grpc_hdr "${PROTO_GENERATED_DIR}/${proto_name}.grpc.pb.h")

    add_custom_command(
        OUTPUT ${proto_src} ${proto_hdr} ${grpc_src} ${grpc_hdr}
        COMMAND $<TARGET_FILE:protobuf::protoc>
        ARGS --grpc_out ${PROTO_GENERATED_DIR}
             --cpp_out ${PROTO_GENERATED_DIR}
             --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
             -I ${PROTO_SRC_DIR}
             ${proto_abs}
        DEPENDS ${proto_abs} protobuf::protoc gRPC::grpc_cpp_plugin
    )

    list(APPEND PROTO_SRCS ${proto_src} ${grpc_src})
    list(APPEND PROTO_HDRS ${proto_hdr} ${grpc_hdr})
endforeach()

add_library(proto_objects OBJECT ${PROTO_SRCS})
target_include_directories(proto_objects PUBLIC ${PROTO_GENERATED_DIR} ${Protobuf_INCLUDE_DIRS})
target_link_libraries(proto_objects PUBLIC protobuf::libprotobuf gRPC::grpc++)
set_target_properties(proto_objects PROPERTIES POSITION_INDEPENDENT_CODE ON)

add_library(core
    src/index/vector_shard.cc
    src/index/shard_manager.cc
    src/cache/query_cache.cc
    src/raft/raft_log.cc
    src/raft/raft_state.cc
    src/rpc/search_service_impl.cc
    src/rpc/raft_service_impl.cc
    src/util/config.cc
)

target_include_directories(core
    PUBLIC
        include
        ${PROTO_GENERATED_DIR}
        ${Protobuf_INCLUDE_DIRS}
)

target_link_libraries(core
    PUBLIC
        proto_objects
        FAISS::FAISS
        gRPC::grpc++
        protobuf::libprotobuf
        OpenMP::OpenMP_CXX
)

add_executable(vector_search_node
    src/node_main.cc
)

add_dependencies(vector_search_node proto_objects)
add_dependencies(core proto_objects)

target_link_libraries(vector_search_node
    PRIVATE
        core
        proto_objects
)

install(TARGETS vector_search_node RUNTIME DESTINATION bin)
install(DIRECTORY ${PROTO_SRC_DIR}/ DESTINATION share/protos)
