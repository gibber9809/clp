#ifndef CLP_FFI_IR_STREAM_SEARCH_METHODS_HPP
#define CLP_FFI_IR_STREAM_SEARCH_METHODS_HPP

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <tuple>

#include "../../../clp_s/search/Expression.hpp"
#include "../../../clp_s/search/FilterExpr.hpp"
#include "../../../clp_s/search/Literal.hpp"
#include "../KeyValuePairLogEvent.hpp"
#include "../SchemaTree.hpp"
#include "../Value.hpp"

namespace clp::ffi::ir_stream {
namespace {
}

enum class EvaluatedValue : uint8_t {
    True,
    False,
    Prune
};

auto preprocess_query(std::shared_ptr<clp_s::search::Expression> expr)
        -> std::shared_ptr<clp_s::search::Expression>;

auto node_to_literal_types(SchemaTree::Node::Type node_type) -> clp_s::search::LiteralTypeBitmask;

auto
node_and_value_to_literal_type(SchemaTree::Node::Type node_type, std::optional<Value> const& value)
        -> clp_s::search::LiteralType;

auto evaluate(
        clp_s::search::FilterExpr* expr,
        clp_s::search::LiteralType literal_type,
        std::optional<Value> const& value
) -> EvaluatedValue;
}  // namespace clp::ffi::ir_stream

#endif  // CLP_FFI_IR_STREAM_SEARCH_METHODS_HPP
