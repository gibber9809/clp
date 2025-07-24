#ifndef CLP_GREPCORE_HPP
#define CLP_GREPCORE_HPP

#include <optional>

#include <log_surgeon/Lexer.hpp>

#include "Defs.h"
#include "LogTypeDictionaryReader.hpp"
#include "Query.hpp"
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
};
}  // namespace clp

#endif  // CLP_GREPCORE_HPP
