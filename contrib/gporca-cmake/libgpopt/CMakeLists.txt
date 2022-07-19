# Copyright (c) 2015, Pivotal Software, Inc. All Rights Reserved.

include_directories(${GPORCA_DIR}/libgpopt/include)
include_directories(${GPORCA_DIR}/libgpcost/include)
include_directories(${GPORCA_DIR}/libnaucrates/include)
include_directories(${XERCES_BIN_INCLUDE_DIR})

# Generate version-number header.
configure_file(${GPORCA_DIR}/libgpopt/version.h.in
        ${GPORCA_BIN_DIR}/libgpopt/include/libgpopt/version.h)

# for the generated config.h file.
include_directories(${GPORCA_BIN_DIR}/libgpos/include/)

file(GLOB_RECURSE hdrs ${GPORCA_DIR}/libgpopt/include/*.h)
file(GLOB_RECURSE srcs ${GPORCA_DIR}/libgpopt/src/*.cpp)

# Add headers to make them visible in some IDEs (Clion, VS, Xcode)
list(APPEND srcs ${hdrs})

add_library(_gpopt STATIC ${srcs})

target_include_directories(_gpopt SYSTEM BEFORE PUBLIC ${GPORCA_DIR}/libgpopt/include)
target_link_libraries(_gpopt
                      _gpdbcost
                      _naucrates
                      ${CMAKE_THREAD_LIBS_INIT} ${CMAKE_DL_LIBS}
                      _gpos
                      _icuuc
                      _icudata
                      ${XERCES_LIBRARIES}
                      ${GPORCA_SOLARIS_EXTRA_LIBS})

set_target_properties(_gpopt PROPERTIES
                      SOVERSION ${GPORCA_ABI_VERSION}
                      VERSION ${GPORCA_VERSION_STRING})

#install(TARGETS gpopt DESTINATION lib)
#install(DIRECTORY include/gpopt DESTINATION include)
#install(FILES "${PROJECT_BINARY_DIR}/libgpopt/include/libgpopt/version.h"
#        DESTINATION include/gpopt)
