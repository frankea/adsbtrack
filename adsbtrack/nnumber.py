"""Convert FAA N-numbers (tail numbers) to ICAO hex codes.

US aircraft ICAO addresses range from 0xA00001 to 0xADF7C7.
N-numbers follow the format N[1-9][0-9]{0,4}[A-Z]{0,2} where letters
I and O are excluded from suffixes. The total length after N is at most 5
characters (digits + letters combined).

The encoding uses a mixed-radix system where each digit position has a
bucket size determined by the number of valid tail numbers that can follow
that position.
"""

# Suffix alphabet: A-Z excluding I and O (24 letters)
CHARSET = "ABCDEFGHJKLMNPQRSTUVWXYZ"
DIGITSET = "0123456789"
ALLCHARS = CHARSET + DIGITSET

# suffix_size: number of possible suffix encodings
#   1 (no suffix) + 24 * (1 + 24) = 601
#   Breakdown: no suffix (1), single letter (24 options each with
#   25 sub-options: the letter alone + 24 two-letter combos)
SUFFIX_SIZE = 1 + len(CHARSET) * (1 + len(CHARSET))  # 601

# Bucket sizes for each digit position (innermost to outermost).
# bucket4: the 5th character position can be any of allchars (24 letters + 10 digits)
#   plus 1 for "no 5th character" = 35
# bucket3: 10 possible digits * bucket4 + suffix_size (for when only 3 digits)
# bucket2: 10 possible digits * bucket3 + suffix_size
# bucket1: 10 possible digits * bucket2 + suffix_size
BUCKET4_SIZE = 1 + len(CHARSET) + len(DIGITSET)  # 35
BUCKET3_SIZE = len(DIGITSET) * BUCKET4_SIZE + SUFFIX_SIZE  # 951
BUCKET2_SIZE = len(DIGITSET) * BUCKET3_SIZE + SUFFIX_SIZE  # 10111
BUCKET1_SIZE = len(DIGITSET) * BUCKET2_SIZE + SUFFIX_SIZE  # 101711


def _suffix_offset(suffix: str) -> int:
    """Encode a 0-2 letter suffix to a numeric offset.

    The encoding interleaves single and double letter suffixes:
        ''   -> 0
        'A'  -> 1
        'AA' -> 2
        'AB' -> 3
        ...
        'AZ' -> 24   (skipping I, O)
        'B'  -> 25
        'BA' -> 26
        ...
        'ZZ' -> 600
    """
    if len(suffix) == 0:
        return 0

    for ch in suffix:
        if ch not in CHARSET:
            raise ValueError(f"Invalid suffix letter '{ch}' (I and O are not allowed)")

    offset = (len(CHARSET) + 1) * CHARSET.index(suffix[0]) + 1
    if len(suffix) == 2:
        offset += CHARSET.index(suffix[1]) + 1
    return offset


def nnumber_to_icao(nnumber: str) -> str:
    """Convert an N-number like 'N512WB' to an ICAO hex code like 'a66ad3'.

    Args:
        nnumber: FAA registration number starting with 'N'.

    Returns:
        Lowercase hex string (6 characters) of the ICAO address.

    Raises:
        ValueError: If the N-number format is invalid.
    """
    nnumber = nnumber.upper().strip()

    if not nnumber or nnumber[0] != "N":
        raise ValueError(f"N-number must start with 'N': {nnumber}")
    if len(nnumber) < 2 or len(nnumber) > 6:
        raise ValueError(f"Invalid N-number length: {nnumber}")

    tail = nnumber[1:]

    # Validate format: digits followed by optional letters
    for ch in tail:
        if ch not in ALLCHARS:
            raise ValueError(f"Invalid character '{ch}' in {nnumber}")

    # Find where digits end and letters begin
    # Letters can only appear after all digits, and only at positions that
    # won't exceed 5 total characters after N.
    # Also, the first character must be a digit 1-9.
    if tail[0] not in DIGITSET or tail[0] == "0":
        raise ValueError(f"First character after N must be a digit 1-9: {nnumber}")

    # Check format: digits then letters (no mixing)
    if len(tail) > 3:
        for i in range(1, len(tail) - 2):
            if tail[i] in CHARSET:
                raise ValueError(f"Invalid N-number format (letters in wrong position): {nnumber}")

    count = 1  # ICAO offset starts at 1 (a00001 = N1 with offset 0 doesn't exist, offset 1 = first valid)

    for i in range(len(tail)):
        if i == 4:
            # 5th (last possible) character position: can be digit or letter
            count += ALLCHARS.index(tail[i]) + 1
        elif tail[i] in CHARSET:
            # First alphabetical character starts the suffix
            count += _suffix_offset(tail[i:])
            break
        else:
            # Numeric digit
            if i == 0:
                count += (int(tail[i]) - 1) * BUCKET1_SIZE
            elif i == 1:
                count += int(tail[i]) * BUCKET2_SIZE + SUFFIX_SIZE
            elif i == 2:
                count += int(tail[i]) * BUCKET3_SIZE + SUFFIX_SIZE
            elif i == 3:
                count += int(tail[i]) * BUCKET4_SIZE + SUFFIX_SIZE

    # Format as 6-character hex with 'a' prefix
    hex_suffix = format(count, "05x")
    return "a" + hex_suffix


def icao_to_nnumber(icao: str) -> str:
    """Convert an ICAO hex code like 'a66ad3' to an N-number like 'N512WB'.

    Args:
        icao: 6-character ICAO hex address starting with 'a'.

    Returns:
        N-number string (e.g. 'N512WB').

    Raises:
        ValueError: If the ICAO address is invalid or out of US range.
    """
    icao = icao.upper().strip()

    if len(icao) != 6 or icao[0] != "A":
        raise ValueError(f"Invalid US ICAO address: {icao}")

    for ch in icao:
        if ch not in "0123456789ABCDEF":
            raise ValueError(f"Invalid hex character in ICAO: {icao}")

    i = int(icao[1:], 16) - 1  # parse to int, subtract 1 for zero-based
    if i < 0:
        raise ValueError(f"ICAO address out of range: {icao}")

    output = "N"

    # Digit 1 (1-9)
    dig1 = i // BUCKET1_SIZE + 1
    rem1 = i % BUCKET1_SIZE
    if dig1 < 1 or dig1 > 9:
        raise ValueError(f"ICAO address out of US N-number range: {icao}")
    output += str(dig1)

    if rem1 < SUFFIX_SIZE:
        return output + _get_suffix(rem1)

    rem1 -= SUFFIX_SIZE
    # Digit 2 (0-9)
    dig2 = rem1 // BUCKET2_SIZE
    rem2 = rem1 % BUCKET2_SIZE
    output += str(dig2)

    if rem2 < SUFFIX_SIZE:
        return output + _get_suffix(rem2)

    rem2 -= SUFFIX_SIZE
    # Digit 3 (0-9)
    dig3 = rem2 // BUCKET3_SIZE
    rem3 = rem2 % BUCKET3_SIZE
    output += str(dig3)

    if rem3 < SUFFIX_SIZE:
        return output + _get_suffix(rem3)

    rem3 -= SUFFIX_SIZE
    # Digit 4 (0-9)
    dig4 = rem3 // BUCKET4_SIZE
    rem4 = rem3 % BUCKET4_SIZE
    output += str(dig4)

    if rem4 == 0:
        return output

    # Last character (digit or letter from ALLCHARS)
    return output + ALLCHARS[rem4 - 1]


def _get_suffix(offset: int) -> str:
    """Decode a suffix offset back to 0-2 letter string.

    Reverse of _suffix_offset():
        0    -> ''
        1    -> 'A'
        2    -> 'AA'
        ...
        24   -> 'AZ'
        25   -> 'B'
        26   -> 'BA'
        ...
        600  -> 'ZZ'
    """
    if offset == 0:
        return ""

    char0_idx = (offset - 1) // (len(CHARSET) + 1)
    rem = (offset - 1) % (len(CHARSET) + 1)
    char0 = CHARSET[char0_idx]

    if rem == 0:
        return char0
    return char0 + CHARSET[rem - 1]
