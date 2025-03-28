#include "ir_search_methods.hpp"

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <tuple>

#include <string_utils/string_utils.hpp>

#include "../../../clp_s/search/ast/ConvertToExists.hpp"
#include "../../../clp_s/search/ast/EmptyExpr.hpp"
#include "../../../clp_s/search/ast/Expression.hpp"
#include "../../../clp_s/search/ast/FilterExpr.hpp"
#include "../../../clp_s/search/ast/FilterOperation.hpp"
#include "../../../clp_s/search/ast/Literal.hpp"
#include "../../../clp_s/search/ast/NarrowTypes.hpp"
#include "../../../clp_s/search/ast/OrOfAndForm.hpp"
#include "../SchemaTree.hpp"
#include "../Value.hpp"

using clp_s::search::ast::ConvertToExists;
using clp_s::search::ast::EmptyExpr;
using clp_s::search::ast::Expression;
using clp_s::search::ast::FilterExpr;
using clp_s::search::ast::FilterOperation;
using clp_s::search::ast::Literal;
using clp_s::search::ast::LiteralType;
using clp_s::search::ast::LiteralTypeBitmask;
using clp_s::search::ast::NarrowTypes;
using clp_s::search::ast::OrOfAndForm;

namespace clp::ffi::ir_stream {
namespace {
auto evaluate_int_filter(
        FilterOperation op,
        std::shared_ptr<Literal> const& operand,
        std::optional<Value> const& value
) -> bool;
auto evaluate_float_filter(
        FilterOperation op,
        std::shared_ptr<Literal> const& operand,
        std::optional<Value> const& value
) -> bool;
auto evaluate_bool_filter(
        FilterOperation op,
        std::shared_ptr<Literal> const& operand,
        std::optional<Value> const& value
) -> bool;
auto evaluate_var_string_filter(
        FilterOperation op,
        std::shared_ptr<Literal> const& operand,
        std::optional<Value> const& value
) -> bool;
auto evaluate_clp_string_filter(
        FilterOperation op,
        std::shared_ptr<Literal> const& operand,
        std::optional<Value> const& value
) -> bool;
}  // namespace

auto preprocess_query(std::shared_ptr<Expression> expr) -> std::shared_ptr<Expression> {
    if (nullptr == expr) {
        return expr;
    }

    OrOfAndForm standardize_pass;
    if (expr = standardize_pass.run(expr); nullptr != std::dynamic_pointer_cast<EmptyExpr>(expr)) {
        return expr;
    }

    NarrowTypes narrow_pass;
    if (expr = narrow_pass.run(expr); nullptr != std::dynamic_pointer_cast<EmptyExpr>(expr)) {
        return expr;
    }

    ConvertToExists convert_pass;
    return convert_pass.run(expr);
}

auto node_to_literal_types(SchemaTree::Node::Type node_type) -> LiteralTypeBitmask {
    switch (node_type) {
        case SchemaTree::Node::Type::Int:
        case SchemaTree::Node::Type::Float:
            return LiteralType::IntegerT | LiteralType::FloatT;
        case SchemaTree::Node::Type::Bool:
            return LiteralType::BooleanT;
        case SchemaTree::Node::Type::Str:
            return LiteralType::ClpStringT | LiteralType::VarStringT;
        case SchemaTree::Node::Type::UnstructuredArray:
            return LiteralType::ArrayT;
        case SchemaTree::Node::Type::Obj:
            // FIXME: add LiteralType::ObjectT once supported
            return LiteralType::NullT;
        default:
            return LiteralType::UnknownT;
    }
}

auto
node_and_value_to_literal_type(SchemaTree::Node::Type node_type, std::optional<Value> const& value)
        -> LiteralType {
    switch (node_type) {
        case clp::ffi::SchemaTree::Node::Type::Int:
            return LiteralType::IntegerT;
        case clp::ffi::SchemaTree::Node::Type::Float:
            return LiteralType::FloatT;
        case clp::ffi::SchemaTree::Node::Type::Bool:
            return LiteralType::BooleanT;
        case clp::ffi::SchemaTree::Node::Type::UnstructuredArray:
            return LiteralType::ArrayT;
        case clp::ffi::SchemaTree::Node::Type::Str:
            if (value.value().is<std::string>()) {
                return LiteralType::VarStringT;
            }
            return LiteralType::ClpStringT;
        case clp::ffi::SchemaTree::Node::Type::Obj:
            if (value.has_value() && value.value().is_null()) {
                return LiteralType::NullT;
            }
            // FIXME: return LiteralType::ObjectT once supported
            return LiteralType::UnknownT;
        default:
            return LiteralType::UnknownT;
    }
}

auto evaluate(FilterExpr* expr, LiteralType literal_type, std::optional<Value> const& value)
        -> EvaluatedValue {
    auto const op = expr->get_operation();
    if (FilterOperation::EXISTS == op) {
        return EvaluatedValue::True;
    } else if (FilterOperation::NEXISTS == op) {
        return EvaluatedValue::False;
    }

    bool rval{false};
    switch (literal_type) {
        case LiteralType::IntegerT:
            rval = evaluate_int_filter(op, expr->get_operand(), value);
            break;
        case LiteralType::FloatT:
            rval = evaluate_float_filter(op, expr->get_operand(), value);
            break;
        case LiteralType::BooleanT:
            rval = evaluate_bool_filter(op, expr->get_operand(), value);
            break;
        case LiteralType::VarStringT:
            rval = evaluate_var_string_filter(op, expr->get_operand(), value);
            break;
        case LiteralType::ClpStringT:
            rval = evaluate_clp_string_filter(op, expr->get_operand(), value);
            break;
        case LiteralType::ArrayT:
        case LiteralType::NullT:
        case LiteralType::EpochDateT:
        case LiteralType::UnknownT:
        default:
            rval = false;
            break;
    }
    return rval ? EvaluatedValue::True : EvaluatedValue::False;
}

namespace {
auto evaluate_int_filter(
        FilterOperation op,
        std::shared_ptr<Literal> const& operand,
        std::optional<Value> const& value
) -> bool {
    int64_t op_value;
    if (false == operand->as_int(op_value, op)) {
        return false;
    }
    auto const extracted_value = value.value().get_immutable_view<clp::ffi::value_int_t>();

    switch (op) {
        case FilterOperation::EQ:
            return extracted_value == op_value;
        case FilterOperation::NEQ:
            return extracted_value != op_value;
        case FilterOperation::LT:
            return extracted_value < op_value;
        case FilterOperation::GT:
            return extracted_value > op_value;
        case FilterOperation::LTE:
            return extracted_value <= op_value;
        case FilterOperation::GTE:
            return extracted_value >= op_value;
        default:
            return false;
    }
}

auto evaluate_float_filter(
        FilterOperation op,
        std::shared_ptr<Literal> const& operand,
        std::optional<Value> const& value
) -> bool {
    double op_value;
    if (false == operand->as_float(op_value, op)) {
        return false;
    }
    auto const extracted_value = value.value().get_immutable_view<clp::ffi::value_float_t>();

    switch (op) {
        case FilterOperation::EQ:
            return extracted_value == op_value;
        case FilterOperation::NEQ:
            return extracted_value != op_value;
        case FilterOperation::LT:
            return extracted_value < op_value;
        case FilterOperation::GT:
            return extracted_value > op_value;
        case FilterOperation::LTE:
            return extracted_value <= op_value;
        case FilterOperation::GTE:
            return extracted_value >= op_value;
        default:
            return false;
    }
}

auto evaluate_bool_filter(
        FilterOperation op,
        std::shared_ptr<Literal> const& operand,
        std::optional<Value> const& value
) -> bool {
    bool op_value;
    if (false == operand->as_bool(op_value, op)) {
        return false;
    }
    auto const extracted_value = value.value().get_immutable_view<clp::ffi::value_bool_t>();

    switch (op) {
        case FilterOperation::EQ:
            return extracted_value == op_value;
        case FilterOperation::NEQ:
            return extracted_value != op_value;
        default:
            return false;
    }
}

auto evaluate_var_string_filter(
        FilterOperation op,
        std::shared_ptr<Literal> const& operand,
        std::optional<Value> const& value
) -> bool {
    std::string op_value;
    if (false == operand->as_var_string(op_value, op)) {
        return false;
    }
    auto const extracted_value = value.value().get_immutable_view<std::string>();

    switch (op) {
        case FilterOperation::EQ:
            return clp::string_utils::wildcard_match_unsafe(extracted_value, op_value, false);
        case FilterOperation::NEQ:
            return false
                   == clp::string_utils::wildcard_match_unsafe(extracted_value, op_value, false);
        default:
            return false;
    }
}

auto evaluate_clp_string_filter(
        FilterOperation op,
        std::shared_ptr<Literal> const& operand,
        std::optional<Value> const& value
) -> bool {
    std::string op_value;
    if (false == operand->as_clp_string(op_value, op)) {
        return false;
    }
    std::string extracted_value;
    if (value.value().is<clp::ir::EightByteEncodedTextAst>()) {
        extracted_value = value.value()
                                  .get_immutable_view<clp::ir::EightByteEncodedTextAst>()
                                  .decode_and_unparse()
                                  .value();
    } else {
        extracted_value = value.value()
                                  .get_immutable_view<clp::ir::FourByteEncodedTextAst>()
                                  .decode_and_unparse()
                                  .value();
    }

    switch (op) {
        case FilterOperation::EQ:
            return clp::string_utils::wildcard_match_unsafe(extracted_value, op_value, false);
        case FilterOperation::NEQ:
            return false
                   == clp::string_utils::wildcard_match_unsafe(extracted_value, op_value, false);
        default:
            return false;
    }
}
}  // namespace
}  // namespace clp::ffi::ir_stream
