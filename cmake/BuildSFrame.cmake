#
# A macro to build the sframe

macro(sframe_build)
    message(STATUS "macro sframe_build")

    set(SFRAME_DIR ${PROJECT_BINARY_DIR}/third_party/sframe)

    message(STATUS "SFRAME: ${SFRAME_DIR}")

    if (${PROJECT_BINARY_DIR} STREQUAL ${PROJECT_SOURCE_DIR})
        add_custom_command(OUTPUT ${PROJECT_SOURCE_DIR}/third_party/wumpus/bld/libws.a
            WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/third_party/wumpus/bld
            COMMAND $(MAKE) ${SOPHIA_OPTS} clean
            #TODO modify this condition
       )
    else()
        add_custom_command(OUTPUT ${SFRAME_DIR}
            COMMAND ${CMAKE_COMMAND} -E make_directory ${SFRAME_DIR})

        add_custom_command(OUTPUT ${SFRAME_DIR}/release/Makefile
            WORKING_DIRECTORY ${SFRAME_DIR}
	    COMMAND echo "copy the sframe dir*************"
            COMMAND ${CMAKE_COMMAND} -E copy_directory ${PROJECT_SOURCE_DIR}/third_party/sframe ${SFRAME_DIR}
            COMMAND ./configure 
            DEPENDS ${PROJECT_BINARY_DIR}/CMakeCache.txt ${SFRAME_DIR})

        add_custom_command(OUTPUT ${SFRAME_DIR}/release/oss_src/sframe/libsframe.a
            WORKING_DIRECTORY ${SFRAME_DIR}/release
            COMMAND make -j4
            DEPENDS ${PROJECT_BINARY_DIR}/CMakeCache.txt ${SFRAME_DIR}/release/oss_src/sframe/libsframe.a)

    endif()

    add_custom_target(libsframe ALL DEPENDS ${SFRAME_DIR} ${SFRAME_DIR}/release/Makefile 
        ${SFRAME_DIR}/release/oss_src/sframe/libsframe.a)
    message(STATUS "Use bundled sframe: ${SFRAME_DIR}")
    #add_dependencies(build_bundled_libs libws)
    #set(ws_lib "${SFRAME}/bld/libws.a")
    message(STATUS "endmacro sframe_build")
endmacro(sframe_build)

