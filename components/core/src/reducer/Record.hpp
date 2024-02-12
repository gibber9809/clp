#ifndef CLP_AGGREGATION_RECORD_HPP
#define CLP_AGGREGATION_RECORD_HPP

#include <cstdint>
#include <memory>
#include <string>
#include <variant>

#include "RecordValueIterator.hpp"

namespace reducer {
/**
 * Class which describes a single Record containing data which can be accessed via typed key-value
 * pairs.
 */
class Record {
public:
    virtual ~Record() = default;

    virtual std::string_view get_string_view(std::string_view key) const {
        return std::string_view();
    }

    virtual int64_t get_int64_value(std::string_view key) const { return 0.0; }

    virtual double get_double_value(std::string_view key) const { return 0; }

    virtual std::unique_ptr<RecordValueIterator> value_iter() const = 0;
};

/**
 * Record implementation which exposes a single string key-value pair.
 *
 * The value associated with the key can be updated allowing this class to act as an adapter for a
 * larger set of data.
 */
class StringRecordAdapter : public Record {
public:
    StringRecordAdapter(std::string key_name) : m_key_name(key_name) {}

    void set_record_value(std::string_view value) { m_value = value; }

    virtual std::string_view get_string_view(std::string_view key) const {
        if (key == m_key_name) {
            return m_value;
        }
        return std::string_view();
    }

    virtual std::unique_ptr<RecordValueIterator> value_iter() const {
        return std::make_unique<SingleValueIterator>(m_key_name, ValueType::STRING);
    }

private:
    std::string m_key_name;
    std::string_view m_value;
};

/**
 * Record implementation which exposes a single integer key-value pair.
 *
 * The value associated with the key can be updated allowing this class to act as an adapter for a
 * larger set of data.
 */
class Int64RecordAdapter : public Record {
public:
    Int64RecordAdapter(std::string key_name) : m_key_name(key_name) {}

    void set_record_value(int64_t value) { m_value = value; }

    virtual int64_t get_int64_value(std::string_view key) const {
        if (key == m_key_name) {
            return m_value;
        }
        return 0;
    }

    virtual std::unique_ptr<RecordValueIterator> value_iter() const {
        return std::make_unique<SingleValueIterator>(m_key_name, ValueType::INT64);
    }

private:
    std::string m_key_name;
    int64_t m_value;
};

/**
 * Record implementation for an empty key-value pair.
 */
class EmptyRecord : public Record {
public:
    EmptyRecord() {}

    virtual std::unique_ptr<RecordValueIterator> value_iter() const {
        return std::make_unique<EmptyRecordValueIterator>();
    }
};
}  // namespace reducer

#endif  // CLP_AGGREGATION_RECORD_HPP