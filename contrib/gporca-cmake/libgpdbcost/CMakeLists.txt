# Copyright (c) 2015, Pivotal Software, Inc. All Rights Reserved.

include_directories(${GPORCA_DIR}/libgpdbcost/include)
include_directories(${GPORCA_DIR}/libgpopt/include)
include_directories(${GPORCA_DIR}/libnaucrates/include)
# for the generated config.h file.
include_directories(${GPORCA_BIN_DIR}/libgpos/include/)

file(GLOB_RECURSE hdrs ${GPORCA_DIR}/libgpdbcost/include/*.h)
file(GLOB_RECURSE srcs ${GPORCA_DIR}/libgpdbcost/src/*.cpp)

# Add headers to make them visible in some IDEs (Clion, VS, Xcode)
list(APPEND srcs ${hdrs})

add_library(_gpdbcost STATIC ${srcs})

target_include_directories(_gpdbcost SYSTEM BEFORE PUBLIC ${GPORCA_DIR}/libgpdbcost/include)
target_link_libraries(_gpdbcost  
                      ${CMAKE_THREAD_LIBS_INIT} ${CMAKE_DL_LIBS}
                      _gpos
                      _icuuc
                      _icudata
                      ${XERCES_LIBRARIES}
                      ${GPORCA_SOLARIS_EXTRA_LIBS})

# Prevent linker errors from undefined symbols on OSX.
if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  set_target_properties(_gpdbcost PROPERTIES
                        LINK_FLAGS "-undefined dynamic_lookup")
endif()

set_target_properties(_gpdbcost PROPERTIES
                      SOVERSION ${GPORCA_ABI_VERSION}
                      VERSION ${GPORCA_VERSION_STRING})

#install(TARGETS gpdbcost DESTINATION lib)
#install(DIRECTORY include/gpdbcost DESTINATION include)
