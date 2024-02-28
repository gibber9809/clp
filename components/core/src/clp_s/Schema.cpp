#include "Schema.hpp"

#include <algorithm>

namespace clp_s {
void Schema::insert_ordered(int32_t mst_node_id) {
    m_schema.insert(
            std::upper_bound(
                    m_schema.begin(),
                    m_schema.begin()
                            + static_cast<decltype(m_schema)::difference_type>(m_num_ordered),
                    mst_node_id
            ),
            mst_node_id
    );
    ++m_num_ordered;
}

bool dedup_obj(std::vector<int32_t> const& v, int obj_start, int obj_end) {
    int prev_obj = obj_start - 1;
    if (obj_start == 0 || v[prev_obj] != -2) {
        return false;
    }

    for (; prev_obj >= 0 && obj_end >= obj_start; --prev_obj, --obj_end) {
        if (v[prev_obj] != v[obj_end]) {
            return false;
        }
    }

    return obj_end < obj_start;
}

void Schema::insert_unordered(int32_t mst_node_id) {
    if (false == m_test) {
        m_schema.push_back(mst_node_id);
        return;
    }

    // Hacky test code for array optimization
    if (mst_node_id == -1) {  // array boundary
        m_schema.push_back(mst_node_id);
        if (!m_obj_stack.empty() && m_obj_stack.top().first == -1) {  // array end
            m_obj_stack.pop();
        } else {  // array start
            m_obj_stack.push({mst_node_id, m_schema.size() - 1});
        }
    } else if (mst_node_id == -2) {  // obj element boundary
        m_schema.push_back(mst_node_id);
        if (!m_obj_stack.empty() && m_obj_stack.top().first == -2) {  // obj end
            if (dedup_obj(m_schema, m_obj_stack.top().second, m_obj_stack.size() - 1)) {
                m_schema.erase(m_schema.begin() + m_obj_stack.top().second, m_schema.end());
            }
            m_obj_stack.pop();
        } else if (m_obj_stack.top().first == -1) {  // obj start
            m_obj_stack.push({mst_node_id, m_schema.size() - 1});
        }
    } else {
        if (!m_obj_stack.empty() && m_obj_stack.top().first == -1) {
            if (m_schema.back() != mst_node_id) {
                m_schema.push_back(mst_node_id);
            }
        } else {
            // trying unordered obj first
            m_schema.push_back(mst_node_id);
        }
    }
}

void Schema::insert_unordered(Schema const& schema) {
    m_schema.insert(m_schema.end(), schema.begin(), schema.end());
}
}  // namespace clp_s
