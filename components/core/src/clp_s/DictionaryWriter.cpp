#include "DictionaryWriter.hpp"

#include <spdlog/spdlog.h>

#include "../clp/LogTypeDictionaryEntry.hpp"
#include "../clp/VariableDictionaryEntry.hpp"

namespace clp_s {
bool VariableDictionaryWriter::add_entry(std::string const& value, uint64_t& id) {
    bool new_entry = false;

    auto const ix = m_value_to_id.find(value);
    if (m_value_to_id.end() != ix) {
        id = ix->second;
    } else {
        // Entry doesn't exist so create it

        if (m_next_id > m_max_id) {
            SPDLOG_ERROR("VariableDictionaryWriter ran out of IDs.");
            throw OperationFailed(ErrorCodeOutOfBounds, __FILENAME__, __LINE__);
        }

        // Assign ID
        id = m_next_id;
        ++m_next_id;

        // Insert the ID obtained from the database into the dictionary
        clp::VariableDictionaryEntry entry{value, id};
        m_value_to_id.emplace(value, id);

        new_entry = true;

        m_data_size += entry.get_data_size();

        entry.write_to_file(m_dictionary_compressor);
    }
    return new_entry;
}

bool LogTypeDictionaryWriter::add_entry(
        clp::LogTypeDictionaryEntry& logtype_entry,
        uint64_t& logtype_id
) {
    bool is_new_entry = false;

    std::string const& value = logtype_entry.get_value();
    auto const ix = m_value_to_id.find(value);
    if (m_value_to_id.end() != ix) {
        // Entry exists so get its ID
        logtype_id = ix->second;
    } else {
        // Assign ID
        logtype_id = m_next_id;
        ++m_next_id;
        logtype_entry.set_id(logtype_id);

        // Insert new entry into dictionary
        m_value_to_id.emplace(value, logtype_id);

        is_new_entry = true;

        m_data_size += logtype_entry.get_data_size();

        logtype_entry.write_to_file(m_dictionary_compressor);
    }
    return is_new_entry;
}
}  // namespace clp_s
