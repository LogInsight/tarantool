#
# A macro to build the ws

macro(ws_build)
    message(STATUS "1111111 ${CMAKE_CXX_FLAGS} ${CMAKE_COMMAND}")

    set(WS_DIR ${PROJECT_BINARY_DIR}/third_party/ws)

    message(STATUS "WS_DIR: ${WS_DIR}")

    if (${PROJECT_BINARY_DIR} STREQUAL ${PROJECT_SOURCE_DIR})
        add_custom_command(OUTPUT ${PROJECT_SOURCE_DIR}/third_party/wumpus/bld/libws.a
            WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/third_party/wumpus/bld
            COMMAND $(MAKE) ${SOPHIA_OPTS} clean
            #TODO modify this condition
       )
    else()
        add_custom_command(OUTPUT ${WS_DIR}
            COMMAND ${CMAKE_COMMAND} -E make_directory ${WS_DIR}
            COMMAND ${CMAKE_COMMAND} -E make_directory ${WS_DIR}/bld
            )
        add_custom_command(OUTPUT ${WS_DIR}/bld/libws.a
            WORKING_DIRECTORY ${WS_DIR}/bld
            COMMAND ${CMAKE_COMMAND} -E copy_directory ${PROJECT_SOURCE_DIR}/third_party/wumpus ${WS_DIR}
            COMMAND cmake ..
            COMMAND echo "make"
            COMMAND pwd
            COMMAND make
            DEPENDS ${PROJECT_BINARY_DIR}/CMakeCache.txt ${WS_DIR}
    )
    endif()

    add_custom_target(libws ALL DEPENDS ${WS_DIR} ${WS_DIR}/bld/libws.a)
    message(STATUS "Use bundled WS: ${PROJECT_SOURCE_DIR}/third_party/ws/")
    add_dependencies(build_bundled_libs libws)
    set(ws_lib "${WS_DIR}/bld/libws.a")
    message(STATUS "endmacro ws_build")
endmacro(ws_build)

