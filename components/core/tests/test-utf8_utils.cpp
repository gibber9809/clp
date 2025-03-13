#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include <Catch2/single_include/catch2/catch.hpp>
#include <json/single_include/nlohmann/json.hpp>

#include "../src/clp/ffi/utils.hpp"
#include "../src/clp/utf8_utils.hpp"

using clp::ffi::validate_and_escape_utf8_string;
using clp::is_utf8_encoded;

namespace {
/**
 * @param raw
 * @return The input string after escaping any characters that are invalid in JSON strings.
 */
[[nodiscard]] auto get_expected_escaped_string(std::string_view raw) -> std::string;

/**
 * Generates a UTF-8 encoded byte sequence with the given code point and number of continuation
 * bytes. The range of the code point is not validated, which means the generated byte sequence can
 * be invalid (overlong or exceeding the valid range of UTF-8 code points).
 * @param code_point
 * @param num_continuation_bytes
 * @return The encoded UTF-8 byte sequence.
 */
[[nodiscard]] auto generate_utf8_byte_sequence(uint32_t code_point, size_t num_continuation_bytes)
        -> std::string;

auto get_expected_escaped_string(std::string_view raw) -> std::string {
    nlohmann::json const json_str = raw;  // Don't use '{}' initializer
    auto const dumped_str{json_str.dump()};
    // Strip the quotes that nlohmann::json adds
    return {dumped_str.begin() + 1, dumped_str.end() - 1};
}

auto generate_utf8_byte_sequence(uint32_t code_point, size_t num_continuation_bytes)
        -> std::string {
    REQUIRE((1 <= num_continuation_bytes && num_continuation_bytes <= 3));
    std::vector<char> encoded_bytes;
    while (encoded_bytes.size() < num_continuation_bytes) {
        auto const least_significant_byte{static_cast<uint8_t>(code_point)};
        encoded_bytes.push_back(
                static_cast<char>(
                        (least_significant_byte & ~clp::cUtf8ContinuationByteMask)
                        | clp::cUtf8ContinuationByteHeader
                )
        );
        code_point >>= clp::cUtf8NumContinuationByteCodePointBits;
    }

    uint8_t lead_byte_code_point_mask{};
    uint8_t lead_byte_header{};
    if (1 == num_continuation_bytes) {
        lead_byte_code_point_mask = static_cast<uint8_t>(~clp::cTwoByteUtf8CharHeaderMask);
        lead_byte_header = clp::cTwoByteUtf8CharHeader;
    } else if (2 == num_continuation_bytes) {
        lead_byte_code_point_mask = static_cast<uint8_t>(~clp::cThreeByteUtf8CharHeaderMask);
        lead_byte_header = clp::cThreeByteUtf8CharHeader;
    } else {  // 3 == num_continuation_bytes
        lead_byte_code_point_mask = static_cast<uint8_t>(~clp::cFourByteUtf8CharHeaderMask);
        lead_byte_header = clp::cFourByteUtf8CharHeader;
    }
    encoded_bytes.push_back(
            static_cast<char>(
                    (static_cast<uint8_t>(code_point) & lead_byte_code_point_mask)
                    | lead_byte_header
            )
    );

    return {encoded_bytes.rbegin(), encoded_bytes.rend()};
}
}  // namespace

TEST_CASE("escape_utf8_string_basic", "[utf8_utils]") {
    std::string test_str;
    std::optional<std::string> actual;

    // Test empty string
    actual = validate_and_escape_utf8_string(test_str);
    REQUIRE((actual.has_value() && actual.value() == get_expected_escaped_string(test_str)));

    // Test string that has nothing to escape
    test_str = "This string has nothing to escape :)";
    actual = validate_and_escape_utf8_string(test_str);
    REQUIRE((actual.has_value() && actual.value() == get_expected_escaped_string(test_str)));

    // Test string with all single byte UTF-8 characters, including those we escape.
    test_str.clear();
    for (uint8_t i{0}; i <= static_cast<uint8_t>(INT8_MAX); ++i) {
        test_str.push_back(static_cast<char>(i));
    }
    // Shuffle characters randomly
    // NOLINTNEXTLINE(cert-msc32-c, cert-msc51-cpp)
    std::shuffle(test_str.begin(), test_str.end(), std::default_random_engine{});
    actual = validate_and_escape_utf8_string(test_str);
    REQUIRE((actual.has_value() && actual.value() == get_expected_escaped_string(test_str)));

    // Test valid UTF-8 chars with continuation bytes
    std::vector<std::string> const valid_utf8{
            "\n",
            "\xF0\xA0\x80\x8F",  // https://en.wiktionary.org/wiki/%F0%A0%80%8F
            "a",
            "\xE4\xB8\xAD",  // https://en.wiktionary.org/wiki/%E4%B8%AD
            "\x1F",
            "\xC2\xA2",  // ¢
            "\\"
    };
    test_str.clear();
    for (auto const& str : valid_utf8) {
        test_str.append(str);
    }
    actual = validate_and_escape_utf8_string(test_str);
    REQUIRE((actual.has_value() && actual.value() == get_expected_escaped_string(test_str)));
}

TEST_CASE("escape_utf8_string_with_invalid_continuation", "[utf8_utils]") {
    std::string test_str;

    auto const valid_utf8_byte_sequence = GENERATE(
            generate_utf8_byte_sequence(0x80, 1),
            generate_utf8_byte_sequence(0x800, 2),
            generate_utf8_byte_sequence(0x1'0000, 3)
    );

    // Test incomplete continuation bytes
    auto const begin_it{valid_utf8_byte_sequence.cbegin()};
    std::string const valid{"Valid"};
    for (auto end_it{valid_utf8_byte_sequence.cend() - 1};
         valid_utf8_byte_sequence.cbegin() != end_it;
         --end_it)
    {
        std::string const incomplete_byte_sequence{begin_it, end_it};

        test_str = valid + incomplete_byte_sequence;
        REQUIRE((false == is_utf8_encoded(test_str)));
        REQUIRE((false == validate_and_escape_utf8_string(test_str).has_value()));

        test_str = incomplete_byte_sequence + valid;
        REQUIRE((false == is_utf8_encoded(test_str)));
        REQUIRE((false == validate_and_escape_utf8_string(test_str).has_value()));
    }

    // Test invalid lead byte
    test_str = valid_utf8_byte_sequence;
    constexpr char cInvalidLeadByte{'\xFF'};
    test_str.front() = cInvalidLeadByte;
    REQUIRE((false == is_utf8_encoded(test_str)));
    REQUIRE((false == validate_and_escape_utf8_string(test_str).has_value()));

    // Test invalid continuation bytes
    for (size_t idx{1}; idx < valid_utf8_byte_sequence.size(); ++idx) {
        test_str = valid_utf8_byte_sequence;
        constexpr uint8_t cInvalidContinuationByteMask{0x40};
        test_str.at(idx) |= cInvalidContinuationByteMask;
        REQUIRE((false == is_utf8_encoded(test_str)));
        REQUIRE((false == validate_and_escape_utf8_string(test_str).has_value()));
    }
}

TEST_CASE("validate_utf8_code_point_ranges", "[utf8_utils]") {
    // Test 1 byte encoding code point range
    for (auto code_point{clp::cOneByteUtf8CharCodePointLowerBound};
         code_point <= clp::cOneByteUtf8CharCodePointUpperBound;
         ++code_point)
    {
        REQUIRE(is_utf8_encoded(std::string{static_cast<char>(code_point)}));
        REQUIRE((false == is_utf8_encoded(generate_utf8_byte_sequence(code_point, 1))));
        REQUIRE((false == is_utf8_encoded(generate_utf8_byte_sequence(code_point, 2))));
        REQUIRE((false == is_utf8_encoded(generate_utf8_byte_sequence(code_point, 3))));
    }

    // Test 2 byte encoding code point range
    for (auto code_point{clp::cTwoByteUtf8CharCodePointLowerBound};
         code_point <= clp::cTwoByteUtf8CharCodePointUpperBound;
         ++code_point)
    {
        REQUIRE(is_utf8_encoded(generate_utf8_byte_sequence(code_point, 1)));
        REQUIRE((false == is_utf8_encoded(generate_utf8_byte_sequence(code_point, 2))));
        REQUIRE((false == is_utf8_encoded(generate_utf8_byte_sequence(code_point, 3))));
    }

    // Test 3 byte encoding code point range
    for (auto code_point{clp::cThreeByteUtf8CharCodePointLowerBound};
         code_point <= clp::cThreeByteUtf8CharCodePointUpperBound;
         ++code_point)
    {
        REQUIRE(is_utf8_encoded(generate_utf8_byte_sequence(code_point, 2)));
        REQUIRE((false == is_utf8_encoded(generate_utf8_byte_sequence(code_point, 3))));
    }

    // Test 4 byte encoding code point range
    for (auto code_point{clp::cFourByteUtf8CharCodePointLowerBound};
         code_point <= clp::cFourByteUtf8CharCodePointUpperBound;
         ++code_point)
    {
        REQUIRE(is_utf8_encoded(generate_utf8_byte_sequence(code_point, 3)));
    }

    // Test 4 byte encoding code point out of range
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    for (auto code_point{clp::cFourByteUtf8CharCodePointUpperBound + 1}; code_point <= 0x1F'FFFF;
         ++code_point)
    {
        REQUIRE((false == is_utf8_encoded(generate_utf8_byte_sequence(code_point, 3))));
    }
}
