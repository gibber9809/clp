#ifndef TASKS_HPP
#define TASKS_HPP

#include <spider/client/spider.hpp>
#include <string>
#include <vector>

// Task function prototype
/**
 * @param context
 * @param s3_paths vector of s3 object URLs
 * @param destination
 * @param timestamp_key
 * @return The list of paths that were ingested succesfully.
 */
std::vector<std::string> compress(
        spider::TaskContext& context,
        std::vector<std::string> s3_paths,
        std::string destination,
        std::string timestamp_key
);

#endif  // TASKS_HPP
