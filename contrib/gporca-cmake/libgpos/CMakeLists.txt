# Need pthreads.
find_package(Threads REQUIRED)
if (NOT CMAKE_USE_PTHREADS_INIT)
  message(FATAL_ERROR "Found a threading library, but it is not pthreads.")
endif()

# Try to find atomic operations.
include(FindAtomics)

if ((NOT (GPOS_GCC_FETCH_ADD_32 AND GPOS_GCC_FETCH_ADD_64
          AND GPOS_GCC_CAS_32 AND GPOS_GCC_CAS_64))
    AND (NOT (${CMAKE_SYSTEM_NAME} MATCHES "SunOS")))
  message(FATAL_ERROR "Could not find GCC-style atomic built-ins or Solaris "
                      "atomic headers. GPOS will fail to build. Please try "
                      "using a recent g++ or clang++ compiler.")
endif()
include_directories(${GPORCA_DIR}/libgpos/include)

include_directories(${GPORCA_DIR}/libgpopt/include/)
include_directories(${GPORCA_DIR}/libnaucrates/include/)

configure_file(${GPORCA_DIR}/libgpos/config.h.in
        ${GPORCA_DIR}/libgpos/include/gpos/config.h)

# for the generated config.h file.
include_directories(${GPORCA_BIN_DIR}/libgpos/include/)

file(GLOB_RECURSE hdrs ${GPORCA_DIR}/libgpos/include/*.h)
file(GLOB_RECURSE srcs ${GPORCA_DIR}/libgpos/src/*.cpp)

# Add headers to make them visible in some IDEs (Clion, VS, Xcode)
list(APPEND srcs ${hdrs})

add_library(_gpos STATIC ${srcs})

target_link_libraries(_gpos ${CMAKE_THREAD_LIBS_INIT} ${CMAKE_DL_LIBS})
target_include_directories(_gpos SYSTEM BEFORE INTERFACE ${GPORCA_DIR}/libgpos/include)

# Extra system libs needed on Solaris.
if (${CMAKE_SYSTEM_NAME} MATCHES "SunOS")
  find_library(RT_LIBRARY NAMES rt librt)
  if (${RT_LIBRARY-NOTFOUND})
    message(WARNING "rt library not found. Linking may fail.")
  else()
    target_link_libraries(gpos ${RT_LIBRARY})
  endif()

  find_library(SOCKET_LIBRARY NAMES socket libsocket)
  if (${SOCKET_LIBRARY-NOTFOUND})
    message(WARNING "socket library not found. Linking may fail.")
  else()
    target_link_libraries(gpos ${SOCKET_LIBRARY})
  endif()
endif()

set_target_properties(_gpos PROPERTIES
                      SOVERSION ${GPORCA_ABI_VERSION}
                      VERSION ${GPORCA_VERSION_STRING})

# Tests reside in the 'server' subdirectory.
#add_subdirectory(server)

# Installation.
#option(VERBOSE_INSTALL_PATH "Install in a subdirectory path that includes GPOS version, CPU architecture, and bit width" OFF)
#if (VERBOSE_INSTALL_PATH)
#  set(installpath "libgpos/${GPORCA_VERSION_VERSION}/${CMAKE_SYSTEM_NAME}-${CMAKE_SYSTEM_PROCESSOR}/${CMAKE_BUILD_TYPE}")
#  string(TOLOWER ${installpath} installpath)
#  get_filename_component(full_install_name_dir "${installpath}/lib" ABSOLUTE)
#  install(TARGETS gpos DESTINATION "${installpath}/lib")
#  install(DIRECTORY include/gpos DESTINATION "${installpath}/include")
#  install(FILES "${PROJECT_BINARY_DIR}/libgpos/include/gpos/config.h" DESTINATION "${installpath}/include/gpos")
#else()
#  get_filename_component(full_install_name_dir "${CMAKE_INSTALL_PREFIX}/lib" ABSOLUTE)
#  install(TARGETS gpos DESTINATION lib)
#  install(DIRECTORY include/gpos DESTINATION include)
#  install(FILES "${PROJECT_BINARY_DIR}/libgpos/include/gpos/config.h" DESTINATION include/gpos)
#endif()

