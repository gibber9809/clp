#ifndef CLP_DICTIONARYCONCEPTS_HPP
#define CLP_DICTIONARYCONCEPTS_HPP

#include <concepts>
#include <cstddef>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "Defs.h"
#include "ir/types.hpp"

namespace clp {
template <typename LogTypeDictionaryEntryType>
concept LogTypeDictionaryEntryReq = requires(
        LogTypeDictionaryEntryType entry,
        size_t length,
        std::string_view msg,
        size_t& begin_pos_ref,
        size_t& end_pos_ref,
        std::string_view& parsed_var_ref,
        size_t begin_pos,
        size_t placeholder_ix,
        ir::VariablePlaceholder& placeholder_ref,
        std::string& logtype
) {
    {
        entry.clear()
    } -> std::same_as<void>;

    {
        entry.reserve_constant_length(length)
    } -> std::same_as<void>;

    {
        entry.parse_next_var(msg, begin_pos_ref, end_pos_ref, parsed_var_ref)
    } -> std::same_as<bool>;

    {
        entry.add_constant(msg, begin_pos, length)
    } -> std::same_as<void>;

    {
        entry.add_int_var()
    } -> std::same_as<void>;

    {
        entry.add_float_var()
    } -> std::same_as<void>;

    {
        entry.add_dictionary_var()
    } -> std::same_as<void>;

    {
        entry.get_value()
    } -> std::same_as<std::string const&>;

    {
        entry.get_num_variables()
    } -> std::same_as<size_t>;

    {
        entry.get_num_placeholders()
    } -> std::same_as<size_t>;

    {
        entry.get_placeholder_info(placeholder_ix, placeholder_ref)
    } -> std::same_as<size_t>;

    {
        entry.get_id()
    } -> std::same_as<logtype_dictionary_id_t>;

    {
        LogTypeDictionaryEntryType::add_int_var(logtype)
    } -> std::same_as<void>;

    {
        LogTypeDictionaryEntryType::add_float_var(logtype)
    } -> std::same_as<void>;

    {
        LogTypeDictionaryEntryType::add_dict_var(logtype)
    } -> std::same_as<void>;
};

template <typename VariableDictionaryEntryType>
concept VariableDictionaryEntryReq = requires(VariableDictionaryEntryType entry) {
    {
        entry.get_id()
    } -> std::same_as<variable_dictionary_id_t>;
};

template <typename LogTypeDictionaryReaderType, typename LogTypeDictionaryEntryType>
concept LogTypeDictionaryReaderReq = requires(
        LogTypeDictionaryReaderType reader,
        std::string_view logtype,
        bool ignore_case,
        std::unordered_set<LogTypeDictionaryEntryType const*>& entries
) {
    {
        reader.get_entry_matching_value(logtype, ignore_case)
    } -> std::same_as<std::vector<LogTypeDictionaryEntryType const*>>;

    {
        reader.get_entries_matching_wildcard_string(logtype, ignore_case, entries)
    } -> std::same_as<void>;
};

template <typename VariableDictionaryWriterType>
concept VariableDictionaryWriterReq = requires(
        VariableDictionaryWriterType writer,
        std::string_view value,
        variable_dictionary_id_t id
) {
    {
        writer.add_entry(value, id)
    } -> std::same_as<bool>;
};

template <typename VariableDictionaryReaderType, typename VariableDictionaryEntryType>
concept VariableDictionaryReaderReq = requires(
        VariableDictionaryReaderType reader,
        variable_dictionary_id_t id,
        std::string_view variable,
        bool ignore_case,
        std::unordered_set<VariableDictionaryEntryType const*>& entries
) {
    {
        reader.get_value(id)
    } -> std::same_as<std::string const&>;

    {
        reader.get_entry_matching_value(variable, ignore_case)
    } -> std::same_as<std::vector<VariableDictionaryEntryType const*>>;

    {
        reader.get_entries_matching_wildcard_string(variable, ignore_case, entries)
    } -> std::same_as<void>;

    std::same_as<typename VariableDictionaryReaderType::dictionary_id_t, variable_dictionary_id_t>;

    std::same_as<typename VariableDictionaryReaderType::entry_t, VariableDictionaryEntryType>;
};
}  // namespace clp

#endif  // CLP_DICTIONARYCONCEPTS_HPP
