#ifndef CLP_S_SCHEMAMAP_HPP
#define CLP_S_SCHEMAMAP_HPP

#include <map>
#include <string>

#include "Schema.hpp"

namespace clp_s {
class SchemaMap {
public:
    using schema_map_t = std::map<Schema, int32_t>;

    // Constructor
    explicit SchemaMap(std::string const& archives_dir, int compression_level)
            : m_archives_dir(archives_dir),
              m_compression_level(compression_level),
              m_current_schema_id(0) {}

    /**
     * Return a schema's Id and add the schema to the
     * schema map if it does not already exist.
     * @param schema
     * @return the Id of the schema
     */
    int32_t add_schema(Schema const& schema);

    /**
     * Write the contents of the SchemaMap to the schema map file
     * @return the compressed size of the SchemaMap in bytes
     */
    [[nodiscard]] size_t store();

    /**
     * Get const iterators into the schema map
     * @return const it to the schema map
     */
    schema_map_t::const_iterator schema_map_begin() const { return m_schema_map.cbegin(); }

    schema_map_t::const_iterator schema_map_end() const { return m_schema_map.cend(); }

    size_t get_num_nodes() const { return m_schema_map.size(); }

    std::pair<double, size_t> get_avg_and_max_nodes_per_schema() {
        size_t max = 0;
        size_t tot = 0;
        double n = m_schema_map.size();
        for (auto it = m_schema_map.begin(); it != m_schema_map.end(); ++it) {
            size_t size_i = it->first.size();

            if (size_i > max) {
                max = size_i;
            }
            tot += size_i;
        }

        double avg = tot / n;
        return {avg, max};
    }

private:
    std::string m_archives_dir;
    int m_compression_level;
    int32_t m_current_schema_id;
    schema_map_t m_schema_map;
};
}  // namespace clp_s

#endif  // CLP_S_SCHEMAMAP_HPP
