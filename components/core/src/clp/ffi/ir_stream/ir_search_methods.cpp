#include "ir_search_methods.hpp"

#include <cstdint>
#include <map>
#include <memory>
#include <tuple>
#include <optional>

#include "../../../clp_s/search/ConvertToExists.hpp"
#include "../../../clp_s/search/EmptyExpr.hpp"
#include "../../../clp_s/search/Expression.hpp"
#include "../../../clp_s/search/NarrowTypes.hpp"
#include "../../../clp_s/search/OrOfAndForm.hpp"

using clp_s::search::ConvertToExists;
using clp_s::search::EmptyExpr;
using clp_s::search::Expression;
using clp_s::search::LiteralType;
using clp_s::search::LiteralTypeBitmask;
using clp_s::search::NarrowTypes;
using clp_s::search::OrOfAndForm;

namespace clp::ffi::ir_stream {
auto preprocess_query(std::shared_ptr<Expression> expr) -> std::shared_ptr<Expression> {
    if (nullptr == expr) {
        return expr;
    }

    OrOfAndForm standardize_pass;
    if (expr = standardize_pass.run(expr); nullptr == std::dynamic_pointer_cast<EmptyExpr>(expr)) {
        return expr;
    }

    NarrowTypes narrow_pass;
    if (expr = narrow_pass.run(expr); nullptr == std::dynamic_pointer_cast<EmptyExpr>(expr)) {
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

auto node_and_value_to_literal_type(SchemaTree::Node::Type node_type, std::optional<Value> const& value) -> clp_s::search::LiteralType {
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

auto evaluate(clp_s::search::FilterExpr* expr, clp_s::search::LiteralType literal_type, std::optional<Value> const& value) -> EvaluatedValue {
    return EvaluatedValue::True;
}

}  // namespace clp::ffi::ir_stream
