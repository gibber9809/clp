#ifndef CLP_AGGREGATION_RECORD_GROUP_SERDES_HPP
#define CLP_AGGREGATION_RECORD_GROUP_SERDES_HPP

#include <iostream>
#include <utility>
#include <vector>

#include <json/single_include/nlohmann/json.hpp>

#include "Record.hpp"
#include "RecordGroup.hpp"
#include "RecordIterator.hpp"
#include "RecordValueIterator.hpp"

namespace reducer {
/**
 * Class which converts serialized data into a RecordGroup and exposes iterators to the underlying
 * data.
 *
 * The serialized data comes from the "serialize" function declared in this file.
 */
class DeserializedRecordGroup : public RecordGroup {
public:
    explicit DeserializedRecordGroup(std::vector<uint8_t>& serialized_data);
    DeserializedRecordGroup(char* buf, size_t len);

    [[nodiscard]] std::unique_ptr<RecordIterator> record_it() const override;

    [[nodiscard]] GroupTags const& get_tags() const override;

private:
    void init_tags_from_json();
    GroupTags m_tags;
    nlohmann::json m_record_group;
};

/**
 * Class which exposes the Record interface on data which had been serialized by
 * the "serialize" function declared in this file.
 */
class DeserializedRecord : public Record {
public:
    void set_record(nlohmann::json const* record) { m_record = record; }

    [[nodiscard]] std::string_view get_string_view(std::string_view key) const override {
        return (*m_record)[key].template get<std::string_view>();
    }

    [[nodiscard]] int64_t get_int64_value(std::string_view key) const override {
        return (*m_record)[key].template get<int64_t>();
    }

    [[nodiscard]] double get_double_value(std::string_view key) const override {
        return (*m_record)[key].template get<double>();
    }

    // FIXME: provide a real record value iterator
    // fine to omit for now since it isn't used by any existing code
    [[nodiscard]] std::unique_ptr<RecordValueIterator> value_iter() const override {
        return std::make_unique<EmptyRecordValueIterator>();
    }

private:
    nlohmann::json const* m_record{nullptr};
};

/**
 * Class which provides a RecordIterator over data serialized by the "serialize" function declared
 * in this file.
 */
class DeserializedRecordIterator : public RecordIterator {
public:
    explicit DeserializedRecordIterator(nlohmann::json::array_t jarray)
            : m_jarray(std::move(jarray)) {
        m_it = m_jarray.begin();
        m_cur_json_record = *m_it;
        m_record.set_record(&m_cur_json_record);
    }

    Record const* get() override { return &m_record; }

    void next() override { ++m_it; }

    bool done() override { return m_it == m_jarray.end(); }

private:
    nlohmann::json m_cur_json_record;
    DeserializedRecord m_record;
    nlohmann::json::array_t m_jarray;
    nlohmann::json::array_t::iterator m_it;
};

std::vector<uint8_t> serialize(
        RecordGroup const& group,
        std::vector<uint8_t>(ser)(nlohmann::json const& j) = nlohmann::json::to_msgpack
);

std::vector<uint8_t> serialize_timeline(RecordGroup const& group);

DeserializedRecordGroup deserialize(std::vector<uint8_t>& data);

DeserializedRecordGroup deserialize(char* buf, size_t len);
}  // namespace reducer

#endif  // CLP_AGGREGATION_RECORD_GROUP_SERDES_HPP
