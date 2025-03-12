#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <spider/client/spider.hpp>
#include <string>
#include <type_traits>
#include <utility>

#include "spider_tasks.hpp"

class InputFileIterator {
public:
    explicit InputFileIterator(std::string const& path) : m_stream(path) {}

    bool get_next_line(std::string& line) {
        if (false == m_stream.is_open()) {
            return false;
        }

        if (!std::getline(m_stream, line)) {
            m_stream.close();
        }
        if (line.empty()) {
            m_stream.close();
            return false;
        }
        return true;
    }

    bool done() { return false == m_stream.is_open(); }

private:
    std::ifstream m_stream;
};

std::vector<std::string> get_ingestion_urls(std::string const& input_path) {
    std::vector<std::string> ingestion_urls;
    InputFileIterator it{input_path};
    std::string url;
    while (it.get_next_line(url)) {
        ingestion_urls.emplace_back(std::move(url));
    }
    return ingestion_urls;
}

// NOLINTBEGIN(bugprone-exception-escape)
auto main(int argc, char const* argv[]) -> int {
    // Parse the storage backend URL from the command line arguments
    if (argc != 6) {
        std::cerr << "Usage: ./client <storage-backend-url> <paths-file> <destination-url> "
                     "<timestamp-key> <compression-batch-size>\n";
        return 1;
    }
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    std::string const storage_url{argv[1]};
    if (storage_url.empty()) {
        std::cerr << "storage-backend-url cannot be empty.\n";
        return 1;
    }

    std::string const paths_file{argv[2]};
    if (paths_file.empty()) {
        std::cerr << "paths-file cannot be empty.\n";
        return 1;
    }
    if (false == std::filesystem::exists(std::filesystem::path(paths_file))) {
        std::cerr << "paths-file " << paths_file << " does not exist.\n";
        return 1;
    }

    std::string const destination_url{argv[3]};
    if (destination_url.empty()) {
        std::cerr << "destination-url cannot be empty.\n";
        return 1;
    }

    std::string const timestamp_key{argv[4]};
    if (timestamp_key.empty()) {
        std::cerr << "timestamp-key cannot be empty.\n";
        return 1;
    }

    int const batch_size = std::atoi(argv[5]);
    if (batch_size <= 0) {
        std::cerr << "batch-size must be > 0\n";
        return 1;
    }

    // Create a driver that connects to the Spider cluster
    spider::Driver driver{storage_url};

    // Submit tasks for execution
    std::vector<spider::Job<std::vector<std::string>>> jobs;

    auto const urls = get_ingestion_urls(paths_file);
    auto it = urls.begin();
    while (it != urls.end()) {
        std::vector<std::string> batched_urls;
        while (batched_urls.size() < batch_size && it != urls.end()) {
            batched_urls.emplace_back(std::move(*it));
            ++it;
        }
        jobs.emplace_back(driver.start(&compress, batched_urls, destination_url, timestamp_key));
    }

    // Wait for the jobs to complete
    bool failed = false;
    for (auto& job : jobs) {
        job.wait_complete();
        // Handle the job's success/failure
        switch (auto job_status = job.get_status()) {
            case spider::JobStatus::Succeeded: {
                break;
            }
            case spider::JobStatus::Failed: {
                std::pair<std::string, std::string> const error_and_fn_name = job.get_error();
                std::cerr << "Job failed in function " << error_and_fn_name.second << " - "
                          << error_and_fn_name.first << '\n';
                failed = true;
                break;
            }
            default: {
                std::cerr << "Job is in unexpected state - "
                          << static_cast<std::underlying_type_t<decltype(job_status)>>(job_status)
                          << '\n';
                failed = true;
                break;
            }
        }
    }
    if (failed) {
        return 1;
    }
    return 0;
}

// NOLINTEND(bugprone-exception-escape)
