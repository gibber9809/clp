#include "BufferedSocketWriter.hpp"

#include "../clp/networking/socket_utils.hpp"

namespace reducer {
bool BufferedSocketWriter::write(char const* data, size_t size) {
    if ((m_buffer.size() + size) <= m_buffer.capacity()) {
        m_buffer.insert(m_buffer.end(), data, data + size);
        return true;
    }

    do {
        size_t space_left = m_buffer.capacity() - m_buffer.size();
        if (space_left > 0) {
            m_buffer.insert(m_buffer.end(), data, data + space_left);
            data += space_left;
            size -= space_left;
        }
        auto ecode = clp::networking::try_send(
                m_socket_fd,
                reinterpret_cast<char const*>(m_buffer.data()),
                m_buffer.size()
        );
        if (clp::ErrorCode::ErrorCode_Success != ecode) {
            return false;
        }
        m_buffer.clear();
    } while (size > m_buffer.capacity());

    if (size > 0) {
        m_buffer.insert(m_buffer.end(), data, data + size);
    }
    return true;
}

bool BufferedSocketWriter::flush() {
    if (m_buffer.empty()) {
        return true;
    }

    auto ecode = clp::networking::try_send(
            m_socket_fd,
            reinterpret_cast<char const*>(m_buffer.data()),
            m_buffer.size()
    );
    m_buffer.clear();
    return clp::ErrorCode::ErrorCode_Success == ecode;
}
}  // namespace reducer
