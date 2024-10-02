cmake_minimum_required(VERSION 3.16)
project(ACGraph)

message(STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0 -Wall -Wextra -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address,undefined")

set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -g -O3 -march=native")

set(THIRD_PARTY_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third_party")
add_subdirectory(${THIRD_PARTY_DIR})

include_directories(src include)

find_package(Threads REQUIRED)
find_package(OpenMP  REQUIRED)

# executable for examples
set(EXAMPLES bfs kcore ppr pr wcc_lp mis)
foreach(EXAMPLE ${EXAMPLES})
    add_executable(${EXAMPLE} example/${EXAMPLE}.cpp src/block_info.cpp src/util.cpp)
    target_link_libraries(${EXAMPLE} 
                          PUBLIC OpenMP::OpenMP_CXX
                          PUBLIC Threads::Threads  
                          PUBLIC liburing
                          PRIVATE phmap
                          concurrentqueue)
endforeach()

add_custom_target(examples DEPENDS ${EXAMPLES})

# executable for tools
set(TOOLS el2csr formatter)
foreach(TOOL ${TOOLS})
    add_executable(${TOOL} tools/${TOOL}.cpp)
    target_link_libraries(${TOOL} PUBLIC Threads::Threads pigo)
    target_link_libraries(${TOOL} PUBLIC OpenMP::OpenMP_CXX)
endforeach()
