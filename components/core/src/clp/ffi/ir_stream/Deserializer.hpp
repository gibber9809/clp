#ifndef CLP_FFI_IR_STREAM_DESERIALIZER_HPP
#define CLP_FFI_IR_STREAM_DESERIALIZER_HPP

#include <concepts>
#include <cstdint>
#include <exception>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <system_error>
#include <tuple>
#include <utility>
#include <vector>

#include <json/single_include/nlohmann/json.hpp>
#include <outcome/single-header/outcome.hpp>

#include "../../../clp_s/archive_constants.hpp"
#include "../../../clp_s/search/ast/AndExpr.hpp"
#include "../../../clp_s/search/ast/ColumnDescriptor.hpp"
#include "../../../clp_s/search/ast/EmptyExpr.hpp"
#include "../../../clp_s/search/ast/Expression.hpp"
#include "../../../clp_s/search/ast/FilterExpr.hpp"
#include "../../../clp_s/search/ast/FilterOperation.hpp"
#include "../../../clp_s/search/ast/OrExpr.hpp"
#include "../../../clp_s/search/ast/SearchUtils.hpp"
#include "../../ReaderInterface.hpp"
#include "../../time_types.hpp"
#include "../SchemaTree.hpp"
#include "decoding_methods.hpp"
#include "ir_search_methods.hpp"
#include "ir_unit_deserialization_methods.hpp"
#include "IrUnitHandlerInterface.hpp"
#include "IrUnitType.hpp"
#include "protocol_constants.hpp"
#include "utils.hpp"

namespace clp::ffi::ir_stream {
/**
 * A deserializer for reading IR units from a CLP kv-pair IR stream. An IR unit handler should be
 * provided to perform user-defined operations on each deserialized IR unit.
 *
 * NOTE: This class is designed only to provide deserialization functionalities. Callers are
 * responsible for maintaining a `ReaderInterface` to input IR bytes from an I/O stream.
 *
 * @tparam IrUnitHandler
 */
template <IrUnitHandlerInterface IrUnitHandler>
requires(std::move_constructible<IrUnitHandler>)
class Deserializer {
public:
    // Factory function
    /**
     * Creates a deserializer by reading the stream's preamble from the given reader.
     * @param reader
     * @param ir_unit_handler
     * @return A result containing the deserializer or an error code indicating the failure:
     * - std::errc::result_out_of_range if the IR stream is truncated
     * - std::errc::protocol_error if the IR stream is corrupted
     * - std::errc::protocol_not_supported if either:
     *   - the IR stream contains an unsupported metadata format;
     *   - the IR stream's version is unsupported;
     *   - or the IR stream's user-defined metadata is not a JSON object.
     */
    [[nodiscard]] static auto create(
            ReaderInterface& reader,
            IrUnitHandler ir_unit_handler,
            std::shared_ptr<clp_s::search::ast::Expression> query,
            std::vector<std::string> projection
    ) -> OUTCOME_V2_NAMESPACE::std_result<Deserializer>;

    // Delete copy constructor and assignment
    Deserializer(Deserializer const&) = delete;
    auto operator=(Deserializer const&) -> Deserializer& = delete;

    // Define default move constructor and assignment
    Deserializer(Deserializer&&) = default;
    auto operator=(Deserializer&&) -> Deserializer& = default;

    // Destructor
    ~Deserializer() = default;

    // Methods
    /**
     * Deserializes the stream from the given reader up to and including the next log event IR unit.
     * @param reader
     * @return Forwards `deserialize_tag`s return values if no tag bytes can be read to determine
     * the next IR unit type.
     * @return std::errc::protocol_not_supported if the IR unit type is not supported.
     * @return std::errc::operation_not_permitted if the deserializer already reached the end of
     * stream by deserializing an end-of-stream IR unit in the previous calls.
     * @return IRUnitType::LogEvent if a log event IR unit is deserialized, or an error code
     * indicating the failure:
     * - Forwards `deserialize_ir_unit_kv_pair_log_event_node_id_value_pairs`'s return values if it
     *   failed to deserialize the log event.
     * - Forwards `KeyValuePairLogEvent::create`'s return values if it failed to construct the log
     *   event.
     * - Forwards `handle_log_event`'s return values from the user-defined IR unit handler on
     *   unit handling failure.
     * @return IRUnitType::SchemaTreeNodeInsertion if a schema tree node insertion IR unit is
     * deserialized, or an error code indicating the failure:
     * - Forwards `deserialize_ir_unit_schema_tree_node_insertion`'s return values if it failed to
     *   deserialize and construct the schema tree node locator.
     * - Forwards `handle_schema_tree_node_insertion`'s return values from the user-defined IR unit
     *   handler on unit handling failure.
     * - Forwards `handle_projection_resolution`'s return values from the user-defined projection
     *   resolution handler on projection resolution handling failure.
     * - std::errc::protocol_error if the deserialized schema tree node already exists in the schema
     *   tree.
     * @return IRUnitType::UtcOffsetChange if a UTC offset change IR unit is deserialized, or an
     * error code indicating the failure:
     * - Forwards `deserialize_ir_unit_utc_offset_change`'s return values if it failed to
     *   deserialize the UTC offset.
     * - Forwards `handle_utc_offset_change`'s return values from the user-defined IR unit handler
     *   on unit handling failure.
     * @return IRUnitType::EndOfStream if an end-of-stream IR unit is deserialized, or an error code
     * indicating the failure:
     * - Forwards `handle_end_of_stream`'s return values from the user-defined IR unit handler on
     *   unit handling failure.
     */
    [[nodiscard]] auto deserialize_next_ir_unit(ReaderInterface& reader)
            -> OUTCOME_V2_NAMESPACE::std_result<IrUnitType>;

    /**
     * @return Whether the stream has completed. A stream is considered completed if an
     * end-of-stream IR unit has already been deserialized.
     */
    [[nodiscard]] auto is_stream_completed() const -> bool { return m_is_complete; }

    [[nodiscard]] auto get_ir_unit_handler() const -> IrUnitHandler const& {
        return m_ir_unit_handler;
    }

    [[nodiscard]] auto get_ir_unit_handler() -> IrUnitHandler& { return m_ir_unit_handler; }

    /**
     * @return The metadata associated with the deserialized stream.
     */
    [[nodiscard]] auto get_metadata() const -> nlohmann::json const& { return m_metadata; }

private:
    // Constructor
    Deserializer(
            IrUnitHandler ir_unit_handler,
            nlohmann::json metadata,
            std::shared_ptr<clp_s::search::ast::Expression> query,
            std::map<std::shared_ptr<clp_s::search::ast::ColumnDescriptor>, std::string>
                    projected_column_to_original_key
    )
            : m_ir_unit_handler{std::move(ir_unit_handler)},
              m_metadata(std::move(metadata)),
              m_query(std::move(query)),
              m_projected_column_to_original_key(std::move(projected_column_to_original_key)) {
        initialize_partial_resolutions();
    }

    /**
     * Initializes state necessary for column resolution.
     */
    void initialize_partial_resolutions();

    /**
     * Handles a step of column resolution for a newly added Node to the Schema Tree.
     * @param is_auto_generated
     * @param node_locator
     * @param node_id
     * @return Forwards `handle_projection_resolution`'s return values from the user-defined handler
     * on failure.
     */
    auto handle_resolution_update_step(
            bool is_auto_generated,
            SchemaTree::NodeLocator const& node_locator,
            SchemaTree::Node::id_t node_id
    ) -> IRErrorCode;

    /**
     * Evaluates a given log event against the query stored in `m_query`.
     * @param node_id_value_pairs
     * @return The evaluated result.
     */
    auto evaluate(
            std::pair<
                    KeyValuePairLogEvent::NodeIdValuePairs,
                    KeyValuePairLogEvent::NodeIdValuePairs> const& node_id_value_pairs
    ) -> EvaluatedValue;

    /**
     * Evaluates a given log event against an Expression recursively.
     * @param expr
     * @param node_id_value_pairs
     * @return The evaluated result.
     */
    auto evaluate_recursive(
            clp_s::search::ast::Expression* expr,
            std::pair<
                    KeyValuePairLogEvent::NodeIdValuePairs,
                    KeyValuePairLogEvent::NodeIdValuePairs> const& node_id_value_pairs
    ) -> EvaluatedValue;

    /**
     * Evaluates a given log event against a Filter Expression.
     * @param expr
     * @param node_id_value_pairs
     * @return The evaluated result.
     */
    auto evaluate_filter(
            clp_s::search::ast::FilterExpr* expr,
            std::pair<
                    KeyValuePairLogEvent::NodeIdValuePairs,
                    KeyValuePairLogEvent::NodeIdValuePairs> const& node_id_value_pairs
    ) -> EvaluatedValue;

    // Variables
    std::shared_ptr<SchemaTree> m_auto_gen_keys_schema_tree{std::make_shared<SchemaTree>()};
    std::shared_ptr<SchemaTree> m_user_gen_keys_schema_tree{std::make_shared<SchemaTree>()};
    nlohmann::json m_metadata;
    UtcOffset m_utc_offset{0};
    IrUnitHandler m_ir_unit_handler;
    bool m_is_complete{false};

    // Search variables
    std::shared_ptr<clp_s::search::ast::Expression> m_query;
    std::map<
            std::tuple<SchemaTree::Node::id_t, bool>,
            std::vector<std::tuple<
                    std::shared_ptr<clp_s::search::ast::ColumnDescriptor>,
                    clp_s::search::ast::DescriptorList::iterator>>>
            m_partial_resolutions;
    std::map<
            std::shared_ptr<clp_s::search::ast::ColumnDescriptor>,
            std::vector<SchemaTree::Node::id_t>>
            m_resolutions;
    std::vector<SchemaTree::Node::id_t> m_schema_buffer;
    std::map<std::shared_ptr<clp_s::search::ast::ColumnDescriptor>, std::string>
            m_projected_column_to_original_key;
};

template <IrUnitHandlerInterface IrUnitHandler>
requires(std::move_constructible<IrUnitHandler>)
auto Deserializer<IrUnitHandler>::create(
        ReaderInterface& reader,
        IrUnitHandler ir_unit_handler,
        std::shared_ptr<clp_s::search::ast::Expression> query,
        std::vector<std::string> projection
) -> OUTCOME_V2_NAMESPACE::std_result<Deserializer> {
    bool is_four_byte_encoded{};
    if (auto const err{get_encoding_type(reader, is_four_byte_encoded)};
        IRErrorCode::IRErrorCode_Success != err)
    {
        return ir_error_code_to_errc(err);
    }

    std::vector<int8_t> metadata;
    encoded_tag_t metadata_type{};
    if (auto const err{deserialize_preamble(reader, metadata_type, metadata)};
        IRErrorCode::IRErrorCode_Success != err)
    {
        return ir_error_code_to_errc(err);
    }

    if (cProtocol::Metadata::EncodingJson != metadata_type) {
        return std::errc::protocol_not_supported;
    }

    auto metadata_json = nlohmann::json::parse(metadata, nullptr, false);
    if (metadata_json.is_discarded()) {
        return std::errc::protocol_error;
    }
    auto const version_iter{metadata_json.find(cProtocol::Metadata::VersionKey)};
    if (metadata_json.end() == version_iter || false == version_iter->is_string()) {
        return std::errc::protocol_error;
    }
    auto const version = version_iter->get_ref<nlohmann::json::string_t&>();
    if (ffi::ir_stream::IRProtocolErrorCode::Supported
        != ffi::ir_stream::validate_protocol_version(version))
    {
        return std::errc::protocol_not_supported;
    }

    if (metadata_json.contains(cProtocol::Metadata::UserDefinedMetadataKey)
        && false == metadata_json.at(cProtocol::Metadata::UserDefinedMetadataKey).is_object())
    {
        return std::errc::protocol_not_supported;
    }

    query = preprocess_query(query);

    std::set<std::string> unique_projected_columns;
    std::map<std::shared_ptr<clp_s::search::ast::ColumnDescriptor>, std::string>
            projected_column_to_original_key;
    for (auto& column : projection) {
        if (unique_projected_columns.count(column)) {
            return std::errc::invalid_argument;
        }

        unique_projected_columns.emplace(column);

        std::vector<std::string> descriptor_tokens;
        std::string descriptor_namespace;
        if (false
            == clp_s::search::ast::tokenize_column_descriptor(
                    column,
                    descriptor_tokens,
                    descriptor_namespace
            ))
        {
            return std::errc::invalid_argument;
        }
        try {
            auto column_descriptor
                    = clp_s::search::ast::ColumnDescriptor::create_from_escaped_tokens(
                            descriptor_tokens,
                            descriptor_namespace
                    );
            if (column_descriptor->is_unresolved_descriptor()
                || column_descriptor->get_descriptor_list().empty())
            {
                return std::errc::invalid_argument;
            }
            projected_column_to_original_key.emplace(
                    std::move(column_descriptor),
                    std::move(column)
            );
        } catch (std::exception const& e) {
            return std::errc::invalid_argument;
        }
    }

    return Deserializer{
            std::move(ir_unit_handler),
            std::move(metadata_json),
            std::move(query),
            std::move(projected_column_to_original_key)
    };
}

template <IrUnitHandlerInterface IrUnitHandler>
requires(std::move_constructible<IrUnitHandler>)
auto Deserializer<IrUnitHandler>::deserialize_next_ir_unit(ReaderInterface& reader)
        -> OUTCOME_V2_NAMESPACE::std_result<IrUnitType> {
    if (is_stream_completed()) {
        return std::errc::operation_not_permitted;
    }

    encoded_tag_t tag{};
    if (auto const err{deserialize_tag(reader, tag)}; IRErrorCode::IRErrorCode_Success != err) {
        return ir_error_code_to_errc(err);
    }

    auto const optional_ir_unit_type{get_ir_unit_type_from_tag(tag)};
    if (false == optional_ir_unit_type.has_value()) {
        return std::errc::protocol_not_supported;
    }

    auto const ir_unit_type{optional_ir_unit_type.value()};
    switch (ir_unit_type) {
        case IrUnitType::LogEvent: {
            auto node_id_value_pairs_result{
                    deserialize_ir_unit_kv_pair_log_event_node_id_value_pairs(reader, tag)
            };
            if (node_id_value_pairs_result.has_error()) {
                return node_id_value_pairs_result.error();
            }

            auto& node_id_value_pairs{node_id_value_pairs_result.value()};

            auto evaluated_value = evaluate(node_id_value_pairs);
            if (EvaluatedValue::True != evaluated_value) {
                // TODO: decide what to do
                return std::errc::no_message;
            }

            auto result{KeyValuePairLogEvent::create(
                    m_auto_gen_keys_schema_tree,
                    m_user_gen_keys_schema_tree,
                    std::move(node_id_value_pairs.first),
                    std::move(node_id_value_pairs.second),
                    m_utc_offset
            )};
            if (result.has_error()) {
                return result.error();
            }

            if (auto const err{m_ir_unit_handler.handle_log_event(std::move(result.value()))};
                IRErrorCode::IRErrorCode_Success != err)
            {
                return ir_error_code_to_errc(err);
            }
            break;
        }

        case IrUnitType::SchemaTreeNodeInsertion: {
            std::string key_name;
            auto const result{
                    deserialize_ir_unit_schema_tree_node_insertion(reader, tag, key_name)
            };
            if (result.has_error()) {
                return result.error();
            }

            auto const& [is_auto_generated, node_locator]{result.value()};
            auto& schema_tree_to_insert{
                    is_auto_generated ? m_auto_gen_keys_schema_tree : m_user_gen_keys_schema_tree
            };

            if (schema_tree_to_insert->has_node(node_locator)) {
                return std::errc::protocol_error;
            }

            auto node_id = schema_tree_to_insert->insert_node(node_locator);

            if (auto const err{
                        handle_resolution_update_step(is_auto_generated, node_locator, node_id)
                };
                IRErrorCode::IRErrorCode_Success != err)
            {
                return ir_error_code_to_errc(err);
            }

            if (auto const err{m_ir_unit_handler.handle_schema_tree_node_insertion(
                        is_auto_generated,
                        node_locator,
                        schema_tree_to_insert
                )};
                IRErrorCode::IRErrorCode_Success != err)
            {
                return ir_error_code_to_errc(err);
            }
            break;
        }

        case IrUnitType::UtcOffsetChange: {
            auto const result{deserialize_ir_unit_utc_offset_change(reader)};
            if (result.has_error()) {
                return result.error();
            }

            auto const new_utc_offset{result.value()};
            if (auto const err{
                        m_ir_unit_handler.handle_utc_offset_change(m_utc_offset, new_utc_offset)
                };
                IRErrorCode::IRErrorCode_Success != err)
            {
                return ir_error_code_to_errc(err);
            }

            m_utc_offset = new_utc_offset;
            break;
        }

        case IrUnitType::EndOfStream: {
            if (auto const err{m_ir_unit_handler.handle_end_of_stream()};
                IRErrorCode::IRErrorCode_Success != err)
            {
                return ir_error_code_to_errc(err);
            }
            m_is_complete = true;
            break;
        }

        default:
            return std::errc::protocol_not_supported;
    }

    return ir_unit_type;
}

template <IrUnitHandlerInterface IrUnitHandler>
requires(std::move_constructible<IrUnitHandler>)
void Deserializer<IrUnitHandler>::initialize_partial_resolutions() {
    for (auto it = m_projected_column_to_original_key.begin();
         it != m_projected_column_to_original_key.end();
         ++it)
    {
        auto key = std::make_tuple(
                SchemaTree::cRootId,
                clp_s::constants::cAutogenNamespace == it->first->get_namespace()
        );
        m_partial_resolutions[key].emplace_back(
                std::make_tuple(it->first, it->first->descriptor_begin())
        );
    }

    if (nullptr == m_query) {
        return;
    }

    std::vector<clp_s::search::ast::Expression*> work_list;
    work_list.push_back(m_query.get());
    while (false == work_list.empty()) {
        auto expr = work_list.back();
        work_list.pop_back();
        if (expr->has_only_expression_operands()) {
            for (auto it = expr->op_begin(); it != expr->op_end(); ++it) {
                work_list.push_back(static_cast<clp_s::search::ast::Expression*>(it->get()));
            }
        } else if (auto filter = dynamic_cast<clp_s::search::ast::FilterExpr*>(expr);
                   nullptr != filter)
        {
            auto col = filter->get_column();
            if (false == col->is_pure_wildcard()) {
                auto key = std::make_tuple(
                        SchemaTree::cRootId,
                        clp_s::constants::cAutogenNamespace == col->get_namespace()
                );
                auto value = std::make_tuple(col, col->descriptor_begin());
                if (col->get_descriptor_list().empty()) {
                    continue;
                }
                m_partial_resolutions[key].push_back(value);
                // Handle edgecase where prefix wildcard matches nothing
                if (col->descriptor_begin()->wildcard()) {
                    auto next_value = std::make_tuple(col, ++col->descriptor_begin());
                    m_partial_resolutions.at(key).emplace_back(std::move(next_value));
                }
            }
        }
    }
}

template <IrUnitHandlerInterface IrUnitHandler>
requires(std::move_constructible<IrUnitHandler>)
auto Deserializer<IrUnitHandler>::handle_resolution_update_step(
        bool is_auto_generated,
        SchemaTree::NodeLocator const& node_locator,
        SchemaTree::Node::id_t node_id
) -> IRErrorCode {
    auto it = m_partial_resolutions.find(
            std::make_pair(node_locator.get_parent_id(), is_auto_generated)
    );
    if (m_partial_resolutions.end() == it) {
        return IRErrorCode::IRErrorCode_Success;
    }

    auto next_resolution_key = std::make_tuple(node_id, is_auto_generated);
    for (auto partial_resolution : it->second) {
        auto [col, token_it] = partial_resolution;
        auto cur_token = token_it++;
        bool is_last_token = col->descriptor_end() == token_it;

        if (false == is_last_token && SchemaTree::Node::Type::Obj == node_locator.get_type()) {
            if (cur_token->wildcard()) {
                m_partial_resolutions[next_resolution_key].push_back(
                        std::make_tuple(col, cur_token)
                );
                m_partial_resolutions[next_resolution_key].push_back(
                        std::make_tuple(col, token_it)
                );
            } else if (cur_token->get_token() == node_locator.get_key_name()) {
                m_partial_resolutions[next_resolution_key].push_back(
                        std::make_tuple(col, token_it)
                );
                if (token_it->wildcard() && col->descriptor_end() != ++token_it) {
                    m_partial_resolutions.at(next_resolution_key)
                            .push_back(std::make_tuple(col, token_it));
                }
            }
        } else if (is_last_token
                   || (false == is_last_token && token_it->wildcard()
                       && col->descriptor_end() == ++token_it))
        {
            if (col->matches_any(node_to_literal_types(node_locator.get_type()))
                && (cur_token->wildcard() || cur_token->get_token() == node_locator.get_key_name()))
            {
                if (auto it = m_projected_column_to_original_key.find(col);
                    it != m_projected_column_to_original_key.end())
                {
                    auto const err{m_ir_unit_handler.handle_projection_resolution(
                            is_auto_generated,
                            node_id,
                            it->second
                    )};
                    if (IRErrorCode::IRErrorCode_Success != err) {
                        return err;
                    }
                } else {
                    m_resolutions[col].push_back(node_id);
                }
            }
        }
    }
    return IRErrorCode::IRErrorCode_Success;
}

template <IrUnitHandlerInterface IrUnitHandler>
requires(std::move_constructible<IrUnitHandler>)
auto Deserializer<IrUnitHandler>::evaluate(
        std::pair<
                KeyValuePairLogEvent::NodeIdValuePairs,
                KeyValuePairLogEvent::NodeIdValuePairs> const& node_id_value_pairs
) -> EvaluatedValue {
    if (nullptr == m_query) {
        return EvaluatedValue::True;
    }

    return evaluate_recursive(m_query.get(), node_id_value_pairs);
}

template <IrUnitHandlerInterface IrUnitHandler>
requires(std::move_constructible<IrUnitHandler>)
auto Deserializer<IrUnitHandler>::evaluate_recursive(
        clp_s::search::ast::Expression* expr,
        std::pair<
                KeyValuePairLogEvent::NodeIdValuePairs,
                KeyValuePairLogEvent::NodeIdValuePairs> const& node_id_value_pairs
) -> EvaluatedValue {
    // TODO: EmptyExpr?
    if (auto and_expr = dynamic_cast<clp_s::search::ast::AndExpr*>(expr); nullptr != and_expr) {
        bool encountered_unknown{false};
        for (auto it = and_expr->op_begin(); it != and_expr->op_end(); ++it) {
            auto nested_expr = static_cast<clp_s::search::ast::Expression*>(it->get());
            auto result = evaluate_recursive(nested_expr, node_id_value_pairs);
            if (EvaluatedValue::Prune == result) {
                return result;
            } else if (EvaluatedValue::False == result) {
                return and_expr->is_inverted() ? EvaluatedValue::True : EvaluatedValue::False;
            }
        }
        return and_expr->is_inverted() ? EvaluatedValue::False : EvaluatedValue::True;
    } else if (auto or_expr = dynamic_cast<clp_s::search::ast::OrExpr*>(expr); nullptr != or_expr) {
        bool all_prune = true;
        for (auto it = or_expr->op_begin(); it != or_expr->op_end(); ++it) {
            auto nested_expr = static_cast<clp_s::search::ast::Expression*>(it->get());
            auto result = evaluate_recursive(nested_expr, node_id_value_pairs);
            if (EvaluatedValue::True == result) {
                return or_expr->is_inverted() ? EvaluatedValue::False : EvaluatedValue::True;
            } else if (EvaluatedValue::False == result) {
                all_prune = false;
            }
        }
        if (all_prune) {
            return EvaluatedValue::Prune;
        }
        return or_expr->is_inverted() ? EvaluatedValue::True : EvaluatedValue::False;
    } else {
        auto const filter_expr = static_cast<clp_s::search::ast::FilterExpr*>(expr);
        auto const result = evaluate_filter(filter_expr, node_id_value_pairs);
        if (EvaluatedValue::Prune == result) {
            return EvaluatedValue::Prune;
        } else if (false == filter_expr->is_inverted()) {
            return result;
        } else {
            return EvaluatedValue::True == result ? EvaluatedValue::False : EvaluatedValue::True;
        }
    }
}

template <IrUnitHandlerInterface IrUnitHandler>
requires(std::move_constructible<IrUnitHandler>)
auto Deserializer<IrUnitHandler>::evaluate_filter(
        clp_s::search::ast::FilterExpr* expr,
        std::pair<
                KeyValuePairLogEvent::NodeIdValuePairs,
                KeyValuePairLogEvent::NodeIdValuePairs> const& node_id_value_pairs
) -> EvaluatedValue {
    auto const col = expr->get_column();

    // Mimic clp-s behaviour of ignoring namespace on pure wildcard columns
    if (col->is_pure_wildcard()) {
        bool matched_any = false;
        for (auto const& pair : node_id_value_pairs.first) {
            auto const node_type = m_auto_gen_keys_schema_tree->get_node(pair.first).get_type();
            auto const literal_type = node_and_value_to_literal_type(node_type, pair.second);
            if (col->matches_type(literal_type)) {
                matched_any = true;
                if (EvaluatedValue::True
                    == clp::ffi::ir_stream::evaluate(expr, literal_type, pair.second))
                {
                    return EvaluatedValue::True;
                }
            }
        }

        for (auto const& pair : node_id_value_pairs.second) {
            auto const node_type = m_user_gen_keys_schema_tree->get_node(pair.first).get_type();
            auto const literal_type = node_and_value_to_literal_type(node_type, pair.second);
            if (col->matches_type(literal_type)) {
                matched_any = true;
                if (EvaluatedValue::True
                    == clp::ffi::ir_stream::evaluate(expr, literal_type, pair.second))
                {
                    return EvaluatedValue::True;
                }
            }
        }
        if (false == matched_any) {
            return EvaluatedValue::Prune;
        }
        return EvaluatedValue::False;
    }

    std::optional<SchemaTree::Node::id_t> matched_node_id{std::nullopt};
    auto matching_nodes_it = m_resolutions.find(col);
    if (m_resolutions.end() == matching_nodes_it) {
        return EvaluatedValue::Prune;
    }

    bool autogen{clp_s::constants::cAutogenNamespace == col->get_namespace()};
    KeyValuePairLogEvent::NodeIdValuePairs const& relevant_field_pairs
            = autogen ? node_id_value_pairs.first : node_id_value_pairs.second;
    for (SchemaTree::Node::id_t id : matching_nodes_it->second) {
        auto it = relevant_field_pairs.find(id);
        if (relevant_field_pairs.end() != it) {
            matched_node_id = id;
            break;
        }
    }

    if (false == matched_node_id.has_value()) {
        return EvaluatedValue::Prune;
    }

    std::shared_ptr<SchemaTree> const& relevant_schema_tree
            = autogen ? m_auto_gen_keys_schema_tree : m_user_gen_keys_schema_tree;
    auto const node_type = relevant_schema_tree->get_node(matched_node_id.value()).get_type();
    auto const& value = relevant_field_pairs.at(matched_node_id.value());
    auto const literal_type = node_and_value_to_literal_type(node_type, value);
    if (false == col->matches_type(literal_type)) {
        return EvaluatedValue::Prune;
    }

    return clp::ffi::ir_stream::evaluate(expr, literal_type, value);
}
}  // namespace clp::ffi::ir_stream

#endif  // CLP_FFI_IR_STREAM_DESERIALIZER_HPP
