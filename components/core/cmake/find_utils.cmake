# This file contains utility functions for finding dependencies.

# @param {string} LIBRARY
# @return {bool} Whether we have already run the find function for LIBRARY in the CLP_CHECKED_FIND
# variable.
function(get_clp_checked_find LIBRARY)
    get_property(CLP_CHECKED_FIND 
        DIRECTORY "${PROJECT_SOURCE_DIR}"
        PROPERTY "clp_checked_find_${LIBRARY}"
    )
    set(CLP_CHECKED_FIND "${CLP_CHECKED_FIND}" PARENT_SCOPE)
endfunction()

# Sets a flag indicating that we have run the find function for LIBRARY.
# @param {string} LIBRARY
function(set_clp_checked_find LIBRARY)
    set_property(DIRECTORY "${PROJECT_SOURCE_DIR}" PROPERTY "clp_checked_find_${LIBRARY}" TRUE)
endfunction()

# Find and setup Boost library.
# @return Forwards any variables from the `find_package` call.
macro(clp_find_boost)
    if(CLP_USE_STATIC_LIBS)
        set(Boost_USE_STATIC_LIBS ON)
    endif()
    find_package(Boost 1.81 REQUIRED iostreams program_options filesystem system regex url)
endmacro()

# Find and setup libcurl.
# By default, CURL does not provide static libraries.
# @return Forwards any variables from the `find_package` call.
macro(clp_find_curl)
    find_package(CURL 7.61.1 REQUIRED)
endmacro()

# Find and setup libarchive.
# @return Forwards any variables from the `find_package` call.
macro(clp_find_libarchive)
    if(CLP_USE_STATIC_LIBS)
        set(LibArchive_USE_STATIC_LIBS ON)
    endif()
    find_package(LibArchive REQUIRED)
endmacro()

# Find and setup LZMA library.
# TODO: Add a script in ./cmake/Modules to properly import LZMA in find_package()'s module mode.
# @return Forwards any variables from the `find_package` call.
macro(clp_find_lzma)
    if(CLP_USE_STATIC_LIBS)
        set(LIBLZMA_USE_STATIC_LIBS ON)
    endif()
    find_package(LibLZMA REQUIRED)
    if(LIBLZMA_FOUND)
        # Version 5.8.1 and above address CVE-2024-3094 and CVE-2025-31115.
        set(REQUIRED_LIBLZMA_VERSION "5.8.1")
        if(LIBLZMA_VERSION_STRING VERSION_LESS ${REQUIRED_LIBLZMA_VERSION})
            message(
                FATAL_ERROR
                "Detected LibLZMA version ${LIBLZMA_VERSION_STRING} is older than required"
                " ${REQUIRED_LIBLZMA_VERSION}"
            )
        endif()
        include_directories(${LIBLZMA_INCLUDE_DIRS})
    endif()
endmacro()


# Find and setup MariaDBClient library.
# @return Forwards any variables from the `find_package` call.
macro(clp_find_mariadb_client)
    # Only log static linking warning one time
    get_clp_checked_find(mariadb_client)
    if(CLP_USE_STATIC_LIBS AND NOT CLP_CHECKED_FIND)
        # NOTE: We can't statically link to MariaDBClient since it's GPL
        message(AUTHOR_WARNING "MariaDBClient cannot be statically linked due to its license.")
    endif()
    find_package(MariaDBClient 3.1.0 REQUIRED)
    set_clp_checked_find(mariadb_client)
endmacro()

# Find and setup mongocxx library.
# @return The name of the mongocxx target in the `MONGOCXX_TARGET` variable, and
# forwards any variables from the `find_package` call.
macro(clp_find_mongocxx)
    find_package(mongocxx REQUIRED)
    if(CLP_USE_STATIC_LIBS)
        set(MONGOCXX_TARGET mongo::mongocxx_static)
    else()
        set(MONGOCXX_TARGET mongo::mongocxx_shared)
    endif()
endmacro()

# Find and setup msgpack library.
# @return Forwards any variables from the `find_package` call.
macro(clp_find_msgpack)
    find_package(msgpack-cxx 7.0.0 REQUIRED)
endmacro()

# Find and setup sqlite library.
# @return Forwards any variables from the `FindDynamicLibraryDependencies` call.
macro(clp_find_sqlite)
    set(sqlite_DYNAMIC_LIBS "dl;m;pthread")
    include("${PROJECT_SOURCE_DIR}/cmake/Modules/FindLibraryDependencies.cmake")
    FindDynamicLibraryDependencies(sqlite "${sqlite_DYNAMIC_LIBS}")
endmacro()

# Find and setup ystdlib.
function(clp_find_ystdlib)
    # We can not call `add_subdirectory` for the same directory multiple times.
    get_clp_checked_find(ystdlib)
    if (CLP_CHECKED_FIND)
        return()
    endif()

    set(YSTDLIB_CPP_BUILD_TESTING OFF)
    add_subdirectory("${CLP_YSTDLIB_SOURCE_DIRECTORY}" "${CMAKE_BINARY_DIR}/ystdlib" EXCLUDE_FROM_ALL)
    set_clp_checked_find(ystdlib)
endfunction()

# Find and setup ZStd library.
# @return Forwards any variables from the `find_package` call.
macro(clp_find_zstd)
    if(CLP_USE_STATIC_LIBS)
        set(ZStd_USE_STATIC_LIBS ON)
    endif()
    find_package(ZStd 1.4.4 REQUIRED)
endmacro()
