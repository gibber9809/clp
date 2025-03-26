#ifndef CLP_FFI_IR_STREAM_SEARCH_METHODS_HPP
#define CLP_FFI_IR_STREAM_SEARCH_METHODS_HPP

#include <cstdint>
#include <map>
#include <memory>
#include <tuple>

#include "../../../clp_s/search/Expression.hpp"
#include "../../../clp_s/search/Literal.hpp"
#include "../KeyValuePairLogEvent.hpp"
#include "../SchemaTree.hpp"

namespace clp::ffi::ir_stream {
enum class EvaluatedValue : uint8_t {
    True,
    False,
    Unknown,
    Prune
};

auto preprocess_query(std::shared_ptr<clp_s::search::Expression> expr)
        -> std::shared_ptr<clp_s::search::Expression>;

auto node_to_literal_type(SchemaTree::Node::Type node_type) -> clp_s::search::LiteralTypeBitmask;
}  // namespace clp::ffi::ir_stream

#endif  // CLP_FFI_IR_STREAM_SEARCH_METHODS_HPP
