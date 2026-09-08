#ifndef CLP_S_INDEXER_COMMANDLINEARGUMENTS_HPP
#define CLP_S_INDEXER_COMMANDLINEARGUMENTS_HPP

#include <cstdint>
#include <optional>
#include <string>

#include "../../clp/GlobalMetadataDBConfig.hpp"
#include "../InputConfig.hpp"

namespace clp_s::indexer {
/**
 * Class to parse command line arguments
 */
class CommandLineArguments {
public:
    // Types
    enum class ParsingResult {
        Success = 0,
        InfoCommand,
        Failure
    };

    // Constructors
    explicit CommandLineArguments(std::string const& program_name) : m_program_name(program_name) {}

    // Methods
    ParsingResult parse_arguments(int argc, char const* argv[]);

    std::string const& get_program_name() const { return m_program_name; }

    uint16_t get_dataset_id() const { return m_dataset_id; }

    Path const& get_archive_path() const { return m_archive_path; }

    std::optional<clp::GlobalMetadataDBConfig> const& get_db_config() const {
        return m_metadata_db_config;
    }

private:
    // Methods
    void print_basic_usage() const;

    // Variables
    std::string m_program_name;
    uint16_t m_dataset_id{0};
    Path m_archive_path;

    std::optional<clp::GlobalMetadataDBConfig> m_metadata_db_config;
};
}  // namespace clp_s::indexer

#endif  // CLP_S_INDEXER_COMMANDLINEARGUMENTS_HPP
