# -------------- support for concurrentqueue (https://github.com/cameron314/concurrentqueue) --------------
add_subdirectory(concurrentqueue)


# -------------- support for PIGO (https://github.com/GT-TDAlab/PIGO) --------------
add_subdirectory(PIGO)



# -------------- support for parallel-hashmap (https://github.com/greg7mdp/parallel-hashmap) --------------
add_subdirectory(parallel-hashmap)


# -------------- support for liburing (https://github.com/axboe/liburing) --------------
# Partial reference from: https://github.com/axboe/liburing/issues/946#issuecomment-1720437458
set(LIBURING_REPO_PATH ${CMAKE_CURRENT_LIST_DIR}/liburing)
set(LIBURING_LIB_PATH  ${LIBURING_REPO_PATH}/src/liburing.a)
set(LIBURING_INCLUDE_PATH ${LIBURING_REPO_PATH}/src/include)
set(LIBURING_LIB liburing)

if (NOT EXISTS ${LIBURING_LIB_PATH})
    message(STATUS "Building liburing")
    execute_process(
            COMMAND ./configure
            WORKING_DIRECTORY ${LIBURING_REPO_PATH}
            OUTPUT_QUIET
    )

    execute_process(
            COMMAND make -j32
            WORKING_DIRECTORY ${LIBURING_REPO_PATH}
            OUTPUT_QUIET
    )
endif ()

add_library(${LIBURING_LIB} STATIC IMPORTED GLOBAL)
set_property(TARGET ${LIBURING_LIB} PROPERTY IMPORTED_LOCATION ${LIBURING_LIB_PATH})
target_include_directories(${LIBURING_LIB} INTERFACE ${LIBURING_INCLUDE_PATH})
