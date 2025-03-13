// Code from CLP

#ifndef CLP_S_DEFS_HPP
#define CLP_S_DEFS_HPP

// C++ libraries
#include <cstdint>
#include <limits>

namespace clp_s {
// Types
using epochtime_t = int64_t;
static epochtime_t const cEpochTimeMin = INT64_MIN;
static epochtime_t const cEpochTimeMax = INT64_MAX;
static double const cDoubleEpochTimeMin = std::numeric_limits<double>::lowest();
static double const cDoubleEpochTimeMax = std::numeric_limits<double>::max();

using variable_dictionary_id_t = uint64_t;
static variable_dictionary_id_t const cVariableDictionaryIdMax = UINT64_MAX;
using logtype_dictionary_id_t = int64_t;
static logtype_dictionary_id_t const cLogtypeDictionaryIdMax = INT64_MAX;

using archive_format_version_t = uint16_t;
// This flag is used to maintain two separate streams of archive format versions:
// - Development versions (which can change frequently as necessary) which should have the flag
// - Production versions (which should be changed with care and as infrequently as possible)
// which should not have the flag
constexpr archive_format_version_t cArchiveFormatDevelopmentVersionFlag = 0x8000;

using file_id_t = uint64_t;
using segment_id_t = uint64_t;
using encoded_variable_t = int64_t;
}  // namespace clp_s
#endif  // CLP_S_DEFS_HPP
