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

if(CLP_BUILD_CLP_FFI_ENCODING)
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
endif()

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

if(CLP_BUILD_CLP_FFI_IR_STREAM_DECODING)
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
endif()

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

if(CLP_BUILD_CLP_FFI_SEARCH)
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
endif()

set(
    CLP_ENCODED_VARIABLE_INTERPRETER_SOURCES
    Defs.h
    DictionaryConcepts.hpp
    EncodedVariableInterpreter.cpp
    EncodedVariableInterpreter.hpp
    ErrorCode.hpp
    ir/EncodedTextAst.cpp
    ir/EncodedTextAst.hpp
    ir/LogEvent.hpp
    Query.cpp
    Query.hpp
    spdlog_with_specializations.hpp
    TraceableException.hpp
)

if(CLP_BUILD_CLP_ENCODED_VARIABLE_INTERPRETER)
    add_library(
        clp_encoded_variable_interpreter
        ${CLP_ENCODED_VARIABLE_INTERPRETER_SOURCES}
    )
    add_library(clp::encoded_variable_interpreter ALIAS clp_encoded_variable_interpreter)
    target_compile_features(clp_encoded_variable_interpreter PRIVATE cxx_std_20)
    target_include_directories(clp_encoded_variable_interpreter PUBLIC ../)
    target_link_libraries(
        clp_encoded_variable_interpreter
        PUBLIC
        clp::ffi::encoding
        clp::ffi::ir_stream::decoding
        spdlog::spdlog
        PRIVATE
        clp::string_utils
    )
endif()

set(
    CLP_GREP_CORE_SOURCES
    Defs.h
    DictionaryConcepts.hpp
    GrepCore.cpp
    GrepCore.hpp
    LogSurgeonReader.cpp
    LogSurgeonReader.hpp
    QueryToken.cpp
    QueryToken.hpp
    ReaderInterface.hpp
    StringReader.cpp
    StringReader.hpp
)

if(CLP_BUILD_CLP_GREP_CORE)
    add_library(
        clp_grep_core
        ${CLP_GREP_CORE_SOURCES}
    )
    add_library(clp::grep_core ALIAS clp_grep_core)
    target_compile_features(clp_grep_core PRIVATE cxx_std_20)
    target_include_directories(clp_grep_core PUBLIC ../)
    target_link_libraries(
        clp_grep_core
        PUBLIC
        clp::encoded_variable_interpreter
        clp::ffi::encoding
        clp::string_utils
        log_surgeon::log_surgeon
    )
endif()
