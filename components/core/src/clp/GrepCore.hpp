#ifndef CLP_GREPCORE_HPP
#define CLP_GREPCORE_HPP

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include <log_surgeon/Lexer.hpp>

#include "Defs.h"
#include "LogTypeDictionaryReader.hpp"
#include "Query.hpp"
#include "QueryToken.hpp"
#include "VariableDictionaryReader.hpp"

namespace clp {
class GrepCore {
public:
    // Methods
    /**
     * Processes a raw user query into a Query.
     *
     * Note: callers are responsible for ensuring that the search string does not contain repeated
     * wildcards "**" e.g. by using `clp::string_utils::clean_up_wildcard_search_string`.
     *
     * @param log_dict
     * @param var_dict
     * @param search_string
     * @param search_begin_ts
     * @param search_end_ts
     * @param ignore_case
     * @param lexer DFA for determining if input is in the schema
     * @param use_heuristic
     * @return Query if it may match a message, std::nullopt otherwise
     */
    static std::optional<Query> process_raw_query(
            LogTypeDictionaryReader const& log_dict,
            VariableDictionaryReader const& var_dict,
            std::string const& search_string,
            epochtime_t search_begin_ts,
            epochtime_t search_end_ts,
            bool ignore_case,
            log_surgeon::lexers::ByteLexer& lexer,
            bool use_heuristic
    );

    /**
     * Returns bounds of next potential variable (either a definite variable or a token with
     * wildcards)
     * @param value String containing token
     * @param begin_pos Begin position of last token, changes to begin position of next token
     * @param end_pos End position of last token, changes to end position of next token
     * @param is_var Whether the token is definitely a variable
     * @return true if another potential variable was found, false otherwise
     */
    static bool get_bounds_of_next_potential_var(
            std::string const& value,
            size_t& begin_pos,
            size_t& end_pos,
            bool& is_var
    );

    /**
     * Returns bounds of next potential variable (either a definite variable or a token with
     * wildcards)
     * @param value String containing token
     * @param begin_pos Begin position of last token, changes to begin position of next token
     * @param end_pos End position of last token, changes to end position of next token
     * @param is_var Whether the token is definitely a variable
     * @param lexer DFA for determining if input is in the schema
     * @return true if another potential variable was found, false otherwise
     */
    static bool get_bounds_of_next_potential_var(
            std::string const& value,
            size_t& begin_pos,
            size_t& end_pos,
            bool& is_var,
            log_surgeon::lexers::ByteLexer& lexer
    );

private:
    // Types
    enum class SubQueryMatchabilityResult : uint8_t {
        MayMatch,  // The subquery might match a message
        WontMatch,  // The subquery has no chance of matching a message
        SupercedesAllSubQueries  // The subquery will cause all messages to be matched
    };

    // Methods
    /**
     * Process a QueryToken that is definitely a variable.
     * @param query_token
     * @param var_dict
     * @param ignore_case
     * @param sub_query
     * @param logtype
     * @return true if this token might match a message, false otherwise
     */
    static bool process_var_token(
            QueryToken const& query_token,
            VariableDictionaryReader const& var_dict,
            bool ignore_case,
            SubQuery& sub_query,
            std::string& logtype
    );

    /**
     * Generates logtypes and variables for subquery.
     * @param log_dict
     * @param var_dict
     * @param processed_search_string
     * @param query_tokens
     * @param ignore_case
     * @param sub_query
     * @return SubQueryMatchabilityResult::SupercedesAllSubQueries
     * @return SubQueryMatchabilityResult::WontMatch
     * @return SubQueryMatchabilityResult::MayMatch
     */
    static SubQueryMatchabilityResult generate_logtypes_and_vars_for_subquery(
            LogTypeDictionaryReader const& log_dict,
            VariableDictionaryReader const& var_dict,
            std::string& processed_search_string,
            std::vector<QueryToken>& query_tokens,
            bool ignore_case,
            SubQuery& sub_query
    );
};
}  // namespace clp

#endif  // CLP_GREPCORE_HPP
