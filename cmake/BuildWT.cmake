#
# A macro to build the ws

macro(wiredtiger_build)
    message(STATUS "1111111 ${CMAKE_CXX_FLAGS} ${CMAKE_COMMAND}")

    set(WT_DIR ${PROJECT_BINARY_DIR}/third_party/wiredtiger)

    message(STATUS "WT_DIR: ${WT_DIR}")

    if (${PROJECT_BINARY_DIR} STREQUAL ${PROJECT_SOURCE_DIR})
        add_custom_command(OUTPUT ${PROJECT_SOURCE_DIR}/third_party/wiredtiger
            WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/third_party/wiredtiger
            COMMAND pwd
	    #TODO modify this condition
       )
    else()
        add_custom_command(OUTPUT ${WT_DIR}
            COMMAND ${CMAKE_COMMAND} -E make_directory ${WT_DIR}
            )
        add_custom_command(OUTPUT ${WT_DIR}/configure
            WORKING_DIRECTORY ${WT_DIR}
            COMMAND ${CMAKE_COMMAND} -E copy_directory ${PROJECT_SOURCE_DIR}/third_party/wiredtiger ${WT_DIR}
            COMMAND ./autogen.sh
            COMMAND ./configure
	    COMMAND make
            DEPENDS ${PROJECT_BINARY_DIR}/CMakeCache.txt ${WT_DIR}
    )
    endif()

    add_custom_target(libwiredtiger ALL DEPENDS ${WT_DIR} ${WT_DIR}/configure)
    message(STATUS "Use bundled WT: ${PROJECT_SOURCE_DIR}/third_party/wiredtiger/")
    add_dependencies(build_bundled_libs libwiredtiger)
    set(wt_lib "${WT_DIR}/.libs/libwiredtiger.a")
    message(STATUS "endmacro wiredtiger_build")
endmacro(wiredtiger_build)

