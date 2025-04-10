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

/**
 * Preprocesses and simplifies a search query by running several transformation passes.
 * @param expr
 * @return The preprocessed query or nullptr if `expr` is nullptr.
 */
auto preprocess_query(std::shared_ptr<clp_s::search::ast::Expression> expr)
        -> std::shared_ptr<clp_s::search::ast::Expression>;

/**
 * Gets all possible matching literal types for a schema tree node type.
 * @param node_type
 * @return A bitmask representing all possible matching literal types.
 */
auto node_to_literal_types(SchemaTree::Node::Type node_type)
        -> clp_s::search::ast::LiteralTypeBitmask;

/**
 * Gets the matching literal type for a given node type and Value combination.
 * @param node_type
 * @param value
 * @return The matching literal type.
 */
auto
node_and_value_to_literal_type(SchemaTree::Node::Type node_type, std::optional<Value> const& value)
        -> clp_s::search::ast::LiteralType;

/**
 * Evaluates a Filter Expression against a Value.
 * @param expr
 * @param literal_type
 * @param value
 * @param case_sensitive_match
 * @return The result of the expression evalution -- either EvaluatedValue::True or
 * EvaluatedValue::False.
 */
auto evaluate(
        clp_s::search::ast::FilterExpr* expr,
        clp_s::search::ast::LiteralType literal_type,
        std::optional<Value> const& value,
        bool case_sensitive_match
) -> EvaluatedValue;
}  // namespace clp::ffi::ir_stream

#endif  // CLP_FFI_IR_STREAM_SEARCH_METHODS_HPP
