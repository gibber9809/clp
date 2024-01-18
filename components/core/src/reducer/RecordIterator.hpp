#ifndef CLP_AGGREGATION_RECORDIT_HPP
#define CLP_AGGREGATION_RECORDIT_HPP

#include <map>
#include <utility>
#include <vector>

#include "Record.hpp"

namespace reducer {
class RecordIterator {
public:
    virtual ~RecordIterator() = default;
    virtual Record const* get() = 0;
    virtual void next() = 0;
    virtual bool done() = 0;
};

class SingleRecordIterator : public RecordIterator {
public:
    SingleRecordIterator(Record const* record) : m_record(record) {}

    virtual Record const* get() { return m_record; }

    virtual void next() {
        m_done = true;
        m_record = nullptr;
    }

    virtual bool done() { return m_done; }

private:
    Record const* m_record;
    bool m_done{false};
};

class VectorRecordIterator : public RecordIterator {
public:
    VectorRecordIterator(std::vector<Record> const* records)
            : m_cur(records->cbegin()),
              m_end(records->cend()) {}

    virtual Record const* get() { return m_cur != m_end ? &*m_cur : nullptr; }

    virtual void next() { ++m_cur; }

    virtual bool done() { return m_cur == m_end; }

private:
    std::vector<Record>::const_iterator m_cur;
    std::vector<Record>::const_iterator m_end;
};
}  // namespace reducer

#endif  // CLP_AGGREGATION_RECORDIT_HPP
