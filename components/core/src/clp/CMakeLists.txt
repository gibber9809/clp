add_subdirectory(make_dictionaries_readable)
add_subdirectory(regex_utils)
add_subdirectory(string_utils)

add_subdirectory(clg)
add_subdirectory(clo)
add_subdirectory(clp)

set(
        CLP_FFI_ENCODING_SOURCES
        Defs.h
        ErrorCode.hpp
        ffi/encoding_methods.cpp
        ffi/encoding_methods.hpp
        ffi/encoding_methods.inc
        ir/types.hpp
        ir/parsing.cpp
        ir/parsing.hpp
        ir/parsing.inc
        time_types.hpp
        TraceableException.hpp
        type_utils.hpp
)

add_library(
        clp_ffi_encoding
        ${CLP_FFI_ENCODING_SOURCES}
)
add_library(clp::ffi::encoding ALIAS clp_ffi_encoding)
target_compile_features(clp_ffi_encoding PRIVATE cxx_std_20)
target_include_directories(clp_ffi_encoding PUBLIC ../)
target_link_libraries(
        clp_ffi_encoding
        PUBLIC
        clp::string_utils
)

set(
        CLP_FFI_IR_STREAM_DECODING_SOURCES
        Defs.h
        ErrorCode.hpp
        ffi/ir_stream/byteswap.hpp
        ffi/ir_stream/decoding_methods.cpp
        ffi/ir_stream/decoding_methods.hpp
        ffi/ir_stream/decoding_methods.inc
        ffi/ir_stream/protocol_constants.hpp
        ffi/ir_stream/utils.cpp
        ffi/ir_stream/utils.hpp
        ffi/SchemaTree.cpp
        ffi/SchemaTree.hpp # Transitive dependency from ir_stream/utils
        ir/types.hpp
        ReaderInterface.hpp
        time_types.hpp
        TraceableException.hpp
        type_utils.hpp
)

add_library(
        clp_ffi_ir_stream_decoding
        ${CLP_FFI_IR_STREAM_DECODING_SOURCES}
)
add_library(clp::ffi::ir_stream::decoding ALIAS clp_ffi_ir_stream_decoding)
target_compile_features(clp_ffi_ir_stream_decoding PRIVATE cxx_std_20)
target_include_directories(clp_ffi_ir_stream_decoding PUBLIC ../)
target_link_libraries(
        clp_ffi_ir_stream_decoding
        PUBLIC
        clp::ffi::encoding
        PRIVATE
        nlohmann_json
        ystdlib::error_handling
)

set(
        CLP_FFI_SEARCH_SOURCES
        ffi/search/CompositeWildcardToken.cpp
        ffi/search/CompositeWildcardToken.hpp
        ffi/search/ExactVariableToken.cpp
        ffi/search/ExactVariableToken.hpp
        ffi/search/query_methods.cpp
        ffi/search/query_methods.hpp
        ffi/search/QueryMethodFailed.hpp
        ffi/search/QueryToken.hpp
        ffi/search/QueryWildcard.cpp
        ffi/search/QueryWildcard.hpp
        ffi/search/Subquery.cpp
        ffi/search/Subquery.hpp
        ffi/search/WildcardToken.cpp
        ffi/search/WildcardToken.hpp
)

add_library(
        clp_ffi_search
        ${CLP_FFI_SEARCH_SOURCES}
)
add_library(clp::ffi::search ALIAS clp_ffi_search)
target_compile_features(clp_ffi_search PRIVATE cxx_std_20)
target_include_directories(clp_ffi_search PUBLIC ../)
target_link_libraries(
        clp_ffi_search
        PUBLIC
        clp::ffi::encoding
        PRIVATE
        clp::string_utils
)
