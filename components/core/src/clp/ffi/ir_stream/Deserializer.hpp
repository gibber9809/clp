#ifndef CLP_FFI_IR_STREAM_DESERIALIZER_HPP
#define CLP_FFI_IR_STREAM_DESERIALIZER_HPP

#include <algorithm>
#include <concepts>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <system_error>
#include <tuple>
#include <vector>

#include <json/single_include/nlohmann/json.hpp>
#include <outcome/single-header/outcome.hpp>

#include "../../../clp_s/archive_constants.hpp"
#include "../../../clp_s/search/AndExpr.hpp"
#include "../../../clp_s/search/Expression.hpp"
#include "../../../clp_s/search/FilterExpr.hpp"
#include "../../../clp_s/search/FilterOperation.hpp"
#include "../../../clp_s/search/OrExpr.hpp"
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
            std::shared_ptr<clp_s::search::Expression> query,
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
            std::shared_ptr<clp_s::search::Expression> query
    )
            : m_ir_unit_handler{std::move(ir_unit_handler)},
              m_metadata(std::move(metadata)),
              m_query(std::move(query)) {
        initialize_partial_resolutions();
    }

    // TODO
    void initialize_partial_resolutions();

    // TODO
    void handle_resolution_update_step(
            bool is_auto_generated,
            SchemaTree::NodeLocator const& node_locator,
            SchemaTree::Node::id_t node_id
    );

    auto evaluate(
            std::pair<
                    KeyValuePairLogEvent::NodeIdValuePairs,
                    KeyValuePairLogEvent::NodeIdValuePairs> const& node_id_value_pairs
    ) -> EvaluatedValue;

    auto evaluate_recursive(
            clp_s::search::Expression* expr,
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
    std::shared_ptr<clp_s::search::Expression> m_query;
    std::map<
            std::tuple<SchemaTree::Node::id_t, bool>,
            std::vector<std::tuple<
                    clp_s::search::ColumnDescriptor*,
                    clp_s::search::DescriptorList::iterator>>>
            m_partial_resolutions;
    std::map<clp_s::search::ColumnDescriptor*, std::vector<SchemaTree::Node::id_t>> m_resolutions;
    std::vector<SchemaTree::Node::id_t> m_schema_buffer;
};

template <IrUnitHandlerInterface IrUnitHandler>
requires(std::move_constructible<IrUnitHandler>)
auto Deserializer<IrUnitHandler>::create(
        ReaderInterface& reader,
        IrUnitHandler ir_unit_handler,
        std::shared_ptr<clp_s::search::Expression> query,
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

    return Deserializer{std::move(ir_unit_handler), std::move(metadata_json), std::move(query)};
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

            auto& [auto_gen_node_id_value_pairs,
                   user_gen_node_id_value_pairs]{node_id_value_pairs_result.value()};

            // TODO: Schema Resolution
            // matches_schema(autogen, usergen, m_query, m_resolutions)
            // TODO: Filter and conditionally continue

            auto result{KeyValuePairLogEvent::create(
                    m_auto_gen_keys_schema_tree,
                    m_user_gen_keys_schema_tree,
                    std::move(auto_gen_node_id_value_pairs),
                    std::move(user_gen_node_id_value_pairs),
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

            handle_resolution_update_step(is_auto_generated, node_locator, node_id);

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
    if (nullptr == m_query) {
        return;
    }

    std::vector<clp_s::search::Expression*> work_list;
    work_list.push_back(m_query.get());
    while (false == work_list.empty()) {
        auto expr = work_list.back();
        work_list.pop_back();
        if (expr->has_only_expression_operands()) {
            for (auto it = expr->op_begin(); it != expr->op_end(); ++it) {
                work_list.push_back(static_cast<clp_s::search::Expression*>(it->get()));
            }
        } else if (auto filter = dynamic_cast<clp_s::search::FilterExpr*>(expr); nullptr != filter)
        {
            auto col = filter->get_column().get();
            if (false == col->is_pure_wildcard()) {
                auto key = std::make_tuple(
                        SchemaTree::cRootId,
                        clp_s::constants::cAutogenNamespace == col->get_namespace()
                );
                auto value = std::make_tuple(col, col->descriptor_begin());
                if (false == col->get_descriptor_list().empty()) {
                    // TODO clean up
                    m_partial_resolutions[key].push_back(value);
                }
            }
        }
    }
}

template <IrUnitHandlerInterface IrUnitHandler>
requires(std::move_constructible<IrUnitHandler>)
void Deserializer<IrUnitHandler>::handle_resolution_update_step(
        bool is_auto_generated,
        SchemaTree::NodeLocator const& node_locator,
        SchemaTree::Node::id_t node_id
) {
    auto it = m_partial_resolutions.find(
            std::make_pair(node_locator.get_parent_id(), is_auto_generated)
    );
    if (m_partial_resolutions.end() == it) {
        return;
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
            }
        } else if (is_last_token && SchemaTree::Node::Type::Obj != node_locator.get_type()) {
            if (col->matches_any(node_to_literal_type(node_locator.get_type()))
                && (cur_token->wildcard() || cur_token->get_token() == node_locator.get_key_name()))
            {
                m_resolutions[col].push_back(node_id);
            }
        }
    }
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
        clp_s::search::Expression* expr,
        std::pair<
                KeyValuePairLogEvent::NodeIdValuePairs,
                KeyValuePairLogEvent::NodeIdValuePairs> const& node_id_value_pairs
) -> EvaluatedValue {
    if (auto and_expr = dynamic_cast<clp_s::search::AndExpr*>(expr); nullptr != and_expr) {
        bool encountered_unknown{false};
        for (auto it = and_expr->op_begin(); it != and_expr->op_end(); ++it) {
            auto nested_expr = static_cast<clp_s::search::Expression*>(it->get());
            auto result = evaluate_recursive(nested_expr, node_id_value_pairs);
            if (EvaluatedValue::Prune == result) {
                return result;
            } else if (EvaluatedValue::False == result) {
                return and_expr->is_inverted() ? EvaluatedValue::True : EvaluatedValue::False;
            } else if (EvaluatedValue::Unknown == result) {
                encountered_unknown = true;
            }
        }
        if (encountered_unknown) {
            return EvaluatedValue::Unknown;
        }
        return and_expr->is_inverted() ? EvaluatedValue::False : EvaluatedValue::True;
    } else if (auto or_expr = dynamic_cast<clp_s::search::OrExpr*>(expr); nullptr != or_expr) {
        bool all_prune = true;
        bool all_false = true;
        for (auto it = or_expr->op_begin(); it != or_expr->op_end(); ++it) {
            auto nested_expr = static_cast<clp_s::search::Expression*>(it->get());
            auto result = evaluate_recursive(nested_expr, node_id_value_pairs);
            if (EvaluatedValue::True == result) {
                return or_expr->is_inverted() ? EvaluatedValue::False : EvaluatedValue::True;
            } else if (EvaluatedValue::False == result) {
                all_prune = false;
            } else if (EvaluatedValue::Unknown == result) {
                all_false = false;
                all_prune = false;
            }
        }
        if (all_false) {
            return or_expr->is_inverted() ? EvaluatedValue::True : EvaluatedValue::False;
        } else if (all_prune) {
            return EvaluatedValue::Prune;
        }
        return EvaluatedValue::Unknown;
    } else {
        auto filter_expr = static_cast<clp_s::search::FilterExpr*>(expr);
        auto col = filter_expr->get_column().get();
        auto op = filter_expr->get_operation();

        if (col->is_pure_wildcard()) {
            return EvaluatedValue::Unknown;
        }
        return EvaluatedValue::Unknown;

        /*std::optional<SchemaTree::Node::id_t> node = std::nullopt;
        if (clp_s::constants::cAutogenNamespace == col->get_namespace()) {
            auto it = // lookup
            if (it == end) {
                return
            }
            for id in *it
                auto col_it = auto_gen_node_id_value_pairs.find(id)
                if (col_it != end)
        } else {

        }*/
    }
}
}  // namespace clp::ffi::ir_stream

#endif  // CLP_FFI_IR_STREAM_DESERIALIZER_HPP
