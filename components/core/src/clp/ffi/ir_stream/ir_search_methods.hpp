#ifndef CLP_FFI_IR_STREAM_SEARCH_METHODS_HPP
#define CLP_FFI_IR_STREAM_SEARCH_METHODS_HPP

#include <cstdint>
#include <memory>
#include <optional>

#include "../../../clp_s/search/ast/Expression.hpp"
#include "../../../clp_s/search/ast/FilterExpr.hpp"
#include "../../../clp_s/search/ast/Literal.hpp"
#include "../SchemaTree.hpp"
#include "../Value.hpp"

namespace clp::ffi::ir_stream {
enum class EvaluatedValue : uint8_t {
    True,
    False,
    Prune
};

auto preprocess_query(std::shared_ptr<clp_s::search::ast::Expression> expr)
        -> std::shared_ptr<clp_s::search::ast::Expression>;

auto node_to_literal_types(SchemaTree::Node::Type node_type)
        -> clp_s::search::ast::LiteralTypeBitmask;

auto
node_and_value_to_literal_type(SchemaTree::Node::Type node_type, std::optional<Value> const& value)
        -> clp_s::search::ast::LiteralType;

auto evaluate(
        clp_s::search::ast::FilterExpr* expr,
        clp_s::search::ast::LiteralType literal_type,
        std::optional<Value> const& value
) -> EvaluatedValue;
}  // namespace clp::ffi::ir_stream

#endif  // CLP_FFI_IR_STREAM_SEARCH_METHODS_HPP
