"""Tests for adsbtrack.nnumber -- FAA N-number to ICAO hex conversion."""

import pytest

from adsbtrack.nnumber import icao_to_nnumber, nnumber_to_icao

# ---------------------------------------------------------------------------
# Known conversions
# ---------------------------------------------------------------------------


def test_known_conversion_n512wb():
    assert nnumber_to_icao("N512WB") == "a66ad3"


def test_known_conversion_n1():
    assert nnumber_to_icao("N1") == "a00001"


def test_known_conversion_lowercase_input():
    """The function should accept lowercase input and still return valid hex."""
    assert nnumber_to_icao("n512wb") == "a66ad3"


def test_known_conversion_with_whitespace():
    assert nnumber_to_icao("  N512WB  ") == "a66ad3"


# ---------------------------------------------------------------------------
# Roundtrip: icao_to_nnumber(nnumber_to_icao(n)) == n
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "nnumber",
    [
        "N1",
        "N9",
        "N10",
        "N99",
        "N100",
        "N999",
        "N1000",
        "N9999",
        "N10000",
        "N99999",
        "N1A",
        "N1AA",
        "N12AB",
        "N512WB",
        "N123",
        "N5678",
        "N1Z",
        "N1ZZ",
        "N90ZZ",
    ],
)
def test_roundtrip(nnumber):
    icao = nnumber_to_icao(nnumber)
    recovered = icao_to_nnumber(icao)
    assert recovered == nnumber


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_minimum_nnumber_n1():
    icao = nnumber_to_icao("N1")
    assert icao == "a00001"


def test_single_digit_range():
    """N1 through N9 should produce sequential ICAO codes."""
    prev = int(nnumber_to_icao("N1")[1:], 16)
    for digit in range(2, 10):
        cur = int(nnumber_to_icao(f"N{digit}")[1:], 16)
        assert cur > prev
        prev = cur


def test_suffix_single_letter():
    """N1A should convert and roundtrip cleanly."""
    icao = nnumber_to_icao("N1A")
    assert icao_to_nnumber(icao) == "N1A"


def test_suffix_double_letter():
    """N1AA should convert and roundtrip cleanly."""
    icao = nnumber_to_icao("N1AA")
    assert icao_to_nnumber(icao) == "N1AA"


def test_suffix_with_digits():
    """N12AB should convert and roundtrip cleanly."""
    icao = nnumber_to_icao("N12AB")
    assert icao_to_nnumber(icao) == "N12AB"


def test_five_digit_nnumber():
    """Five-digit numeric N-numbers should work."""
    icao = nnumber_to_icao("N10000")
    assert icao_to_nnumber(icao) == "N10000"


def test_last_char_can_be_letter_or_digit():
    """The 5th character position accepts both digits and letters."""
    icao_digit = nnumber_to_icao("N10001")
    icao_letter = nnumber_to_icao("N1000A")
    assert icao_digit != icao_letter
    assert icao_to_nnumber(icao_digit) == "N10001"
    assert icao_to_nnumber(icao_letter) == "N1000A"


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


def test_missing_n_prefix():
    with pytest.raises(ValueError, match="must start with 'N'"):
        nnumber_to_icao("512WB")


def test_empty_string():
    with pytest.raises(ValueError):
        nnumber_to_icao("")


def test_n_only():
    with pytest.raises(ValueError):
        nnumber_to_icao("N")


def test_too_long():
    with pytest.raises(ValueError):
        nnumber_to_icao("N123456")


def test_first_char_must_be_nonzero_digit():
    with pytest.raises(ValueError, match="digit 1-9"):
        nnumber_to_icao("N0")


def test_first_char_must_be_digit_not_letter():
    with pytest.raises(ValueError, match="digit 1-9"):
        nnumber_to_icao("NA")


def test_invalid_suffix_letter_i():
    """The letter I is not allowed in suffixes."""
    with pytest.raises(ValueError, match="Invalid"):
        nnumber_to_icao("N1I")


def test_invalid_suffix_letter_o():
    """The letter O is not allowed in suffixes."""
    with pytest.raises(ValueError, match="Invalid"):
        nnumber_to_icao("N1O")


def test_invalid_characters():
    with pytest.raises(ValueError, match="Invalid character"):
        nnumber_to_icao("N1$")


# ---------------------------------------------------------------------------
# icao_to_nnumber error cases
# ---------------------------------------------------------------------------


def test_icao_to_nnumber_known():
    assert icao_to_nnumber("a66ad3") == "N512WB"
    assert icao_to_nnumber("a00001") == "N1"


def test_icao_to_nnumber_uppercase_input():
    assert icao_to_nnumber("A66AD3") == "N512WB"


def test_icao_to_nnumber_invalid_prefix():
    with pytest.raises(ValueError, match="Invalid US ICAO"):
        icao_to_nnumber("b00001")


def test_icao_to_nnumber_wrong_length():
    with pytest.raises(ValueError):
        icao_to_nnumber("a001")


def test_icao_to_nnumber_zero_offset():
    """a00000 has offset 0 which is invalid (below the N-number range)."""
    with pytest.raises(ValueError):
        icao_to_nnumber("a00000")


def test_icao_to_nnumber_invalid_hex_chars():
    with pytest.raises(ValueError, match="Invalid hex character"):
        icao_to_nnumber("aGGGGG")
