#include "CommandLineArguments.hpp"

#include <iostream>

#include <boost/program_options.hpp>

#include "../clp/spdlog_with_specializations.hpp"

namespace po = boost::program_options;

namespace reducer {
clp::CommandLineArgumentsBase::ParsingResult
CommandLineArguments::parse_arguments(int argc, char const* argv[]) {
    try {
        po::options_description options_general("General Options");
        options_general.add_options()("help,h", "Print help");

        po::options_description options_reducer("Reducer Options");
        options_reducer.add_options()(
            "reducer-host",
            po::value<std::string>(&m_reducer_host)
                ->default_value(m_reducer_host),
            "Host that this reducer should bind to"
        )(
            "reducer-port",
            po::value<int>(&m_reducer_port)
                ->default_value(m_reducer_port),
            "Port this reducer should listen on for connections"
        )(
            "db-host",
            po::value<std::string>(&m_db_host)
                ->default_value(m_db_host),
            "Host the jobs database is running on"
        )(
            "db-port",
            po::value<int>(&m_db_port)
                ->default_value(m_db_port),
            "Port the jobs database is listening on"
        )(
            "db-user",
            po::value<std::string>(&m_db_user)
                ->default_value(m_db_user),
            "User for the jobs database"
        )(
            "db-password",
            po::value<std::string>(&m_db_password)
                ->default_value(m_db_password),
            "Password for the jobs database"
        )(
            "db-database",
            po::value<std::string>(&m_db_database)
                ->default_value(m_db_database),
            "Database containing the jobs table"
        )(
            "db-jobs-table",
            po::value<std::string>(&m_db_jobs_table)
                ->default_value(m_db_jobs_table),
            "Name of the table containing jobs"
        )(
            "mongodb-database",
            po::value<std::string>(&m_mongodb_database)
                ->default_value(m_mongodb_database),
            "MongoDB database for results"
        )(
            "mongodb-uri",
            po::value<std::string>(&m_mongodb_uri)
                ->default_value(m_mongodb_uri),
            "URI pointing to MongoDB database"
        )(
            "polling-interval-ms",
            po::value<int>(&m_polling_interval_ms)
                ->default_value(m_polling_interval_ms),
            "Polling interval for the jobs table in milliseconds"
        );

        po::options_description all_options;
        all_options.add(options_general);
        all_options.add(options_reducer);

        po::variables_map parsed_command_line_options;
        po::store(po::parse_command_line(argc, argv, all_options), parsed_command_line_options);
        po::notify(parsed_command_line_options);

        if (parsed_command_line_options.count("help")) {
            if (argc > 2) {
                SPDLOG_WARN("Ignoring all options besides --help.");
            }

            print_basic_usage();
            std::cerr << std::endl;
            std::cerr << "Options can be specified on the command line or through a configuration "
                      << "file." << std::endl;
            std::cerr << all_options << std::endl;
            return clp::CommandLineArgumentsBase::ParsingResult::InfoCommand;
        }
    } catch (std::exception& e) {
        SPDLOG_ERROR("Failed to parse command line arguments - {}", e.what());
        return clp::CommandLineArgumentsBase::ParsingResult::Failure;
    }

    // Validate arguments. Note: mysql username and password are allowed to be the empty string.
    bool valid_arguments = true;
    if (m_reducer_host.empty()) {
        SPDLOG_ERROR("Empty reducer-host argument");
        valid_arguments = false;
    }

    if (m_reducer_port <= 0) {
        SPDLOG_ERROR("Invalid argument for reducer-port {}", m_reducer_port);
        valid_arguments = false;
    }

    if (m_db_host.empty()) {
        SPDLOG_ERROR("Empty db-host argument");
        valid_arguments = false;
    }

    if (m_db_port <= 0) {
        SPDLOG_ERROR("Invalid argument for db-port {}", m_db_port);
        valid_arguments = false;
    }

    if (m_db_database.empty()) {
        SPDLOG_ERROR("Empty db-database argument");
        valid_arguments = false;
    }

    if (m_db_jobs_table.empty()) {
        SPDLOG_ERROR("Empty db-jobs-table argument");
        valid_arguments = false;
    }

    if (m_mongodb_database.empty()) {
        SPDLOG_ERROR("Empty mongodb-database argument");
        valid_arguments = false;
    }

    if (m_mongodb_uri.empty()) {
        SPDLOG_ERROR("Empty mongodb-uri argument");
        valid_arguments = false;
    }

    if (m_polling_interval_ms <= 0) {
        SPDLOG_ERROR("Invalid argument for polling-interval-ms {}", m_polling_interval_ms);
        valid_arguments = false;
    }

    if (!valid_arguments) {
        return clp::CommandLineArgumentsBase::ParsingResult::Failure;
    }

    return clp::CommandLineArgumentsBase::ParsingResult::Success;
}

void CommandLineArguments::print_basic_usage() const {
    std::cerr << "Usage: " << get_program_name() << " [OPTIONS]" << std::endl;
}
}  // namespace reducer
