#
# A macro to build the ws

macro(wiredtiger_build)
    message(STATUS "1111111 ${CMAKE_CXX_FLAGS} ${CMAKE_COMMAND}")
    set(WT_DIR ${PROJECT_BINARY_DIR}/third_party/wiredtiger)
    set(WT_INCLUDE_DIR ${WT_DIR})

    message(STATUS "WT_DIR: ${WT_DIR}")

    if (${PROJECT_BINARY_DIR} STREQUAL ${PROJECT_SOURCE_DIR})
        add_custom_command(OUTPUT ${PROJECT_SOURCE_DIR}/third_party/wiredtiger
            WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/third_party/wiredtiger
            COMMAND pwd
	    #TODO modify this condition
       )
    else()
        add_custom_command(OUTPUT ${WT_DIR}
            COMMAND echo "make dir"
            COMMAND ${CMAKE_COMMAND} -E make_directory ${WT_DIR}
            )
	add_custom_command(OUTPUT ${WT_DIR}/Makefile
            WORKING_DIRECTORY ${WT_DIR}
            COMMAND echo "make file"
            COMMAND ${CMAKE_COMMAND} -E copy_directory ${PROJECT_SOURCE_DIR}/third_party/wiredtiger ${WT_DIR}
            COMMAND ./autogen.sh
            COMMAND ./configure
            #DEPENDS ${PROJECT_BINARY_DIR}/CMakeCache.txt ${WT_DIR}
	    )
        add_custom_command(OUTPUT ${WT_DIR}/.libs/libwiredtiger.a
            WORKING_DIRECTORY ${WT_DIR}
            COMMAND echo "make source"
	    COMMAND make
            #DEPENDS ${PROJECT_BINARY_DIR}/CMakeCache.txt ${WT_DIR}/Makefile
    )
    endif()
	add_custom_target(libwiredtiger ALL DEPENDS ${WT_DIR} ${WT_DIR}/Makefile ${WT_DIR}/.libs/libwiredtiger.a)
    message(STATUS "Use bundled WT: ${PROJECT_SOURCE_DIR}/third_party/wiredtiger/")
    set(wt_lib "${WT_DIR}/.libs/libwiredtiger.a")
    message(STATUS "endmacro wiredtiger_build")
    add_dependencies(build_bundled_libs libwiredtiger)
endmacro(wiredtiger_build)

