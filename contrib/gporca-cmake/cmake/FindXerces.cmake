# Module to find Xerces-C.
# We specifically try to find the Greenplum patched version of Xerces.

message(status "XERCES_INCLUDE_DIR" ${XERCES_INCLUDE_DIR})
find_path(XERCES_INCLUDE_DIR xercesc/sax2/DefaultHandler.hpp)

message(status "XERCES_LIBRARY" ${XERCES_LIBRARY})
find_library(XERCES_LIBRARY NAMES xerces-c libxerces-c lib_xerces-c)

set(XERCES_LIBRARIES ${XERCES_LIBRARY})
set(XERCES_INCLUDE_DIRS ${XERCES_INCLUDE_DIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Xerces DEFAULT_MSG
                                  XERCES_LIBRARY XERCES_INCLUDE_DIR)

if (XERCES_FOUND)
  # Check for patched Xerces. DOMImplementationList is a subclass of XMemory in
  # the Greenplum version of Xerces, but not (yet) in upstream.
  set(CMAKE_REQUIRED_INCLUDES
      "${XERCES_INCLUDE_DIRS} ${CMAKE_REQUIRED_INCLUDES}")
  include(CheckCXXSourceCompiles)
endif()

mark_as_advanced(XERCES_INCLUDE_DIR XERCES_LIBRARY)
