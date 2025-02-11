#ifndef TASKS_HPP
#define TASKS_HPP

#include <spider/client/spider.hpp>
#include <vector>
#include <string>
#include <cstring>

#include <msgpack.hpp>
#include <msgpack/type.hpp>

/*
template <>
struct msgpack::adaptor::convert<std::string> {
    auto operator()(msgpack::object const& object, std::string &s) const -> msgpack::object const& {
        if (object.type != type::BIN) {
            throw type_error();
        }
        s.resize(object.via.bin.size);
        std::memcpy(s.data(), object.via.bin.ptr, s.size());
        return object;
    }
};

template <>
struct msgpack::adaptor::pack<std::string> {
    template <class Stream>
    auto operator()(msgpack::packer<Stream>& packer, std::string const& s) const
            -> msgpack::packer<Stream>& {
        packer.pack_bin(s.size());
        // NOLINTBEGIN(cppcoreguidelines-pro-type-cstyle-cast)
        packer.pack_bin_body((char const*)s.data(), s.size());
        // NOLINTEND(cppcoreguidelines-pro-type-cstyle-cast)
        return packer;
    }
};*/

/*
template <typename T>
template<>
struct msgpack::adaptor::convert<std::vector<T>> {
    auto operator()(msgpack::object const& object, std::vector<T> &v) -> msgpack::object const& {
        if (object.type != type::BIN) {
            throw type_error();
        }
    }
}*/

// Task function prototype
/**
 * @param context
 * @param s3_paths vector of s3 object URLs
 * @param destination 
 * @return The sum of x and y.
 */
int compress(spider::TaskContext& context, std::vector<std::string> s3_paths, std::string destination);

#endif  // TASKS_HPP
