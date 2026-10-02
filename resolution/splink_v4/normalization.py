"""Source-aware contact normalisation; original fields are always retained."""

import re
import unicodedata


def text(value):
    if not value or not value.strip():
        return None
    value = unicodedata.normalize("NFKC", value).casefold()
    value = "".join(c for c in unicodedata.normalize("NFKD", value)
                    if not unicodedata.combining(c))
    value = re.sub(r"[^\w\s]", " ", value, flags=re.UNICODE)
    return " ".join(value.split()) or None


def email(value):
    if not value:
        return None
    value = value.strip().lower()
    if not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", value):
        return None
    local, domain = value.rsplit("@", 1)
    if re.match(r"^(noreply|noemail|unknown|guest|test)([+._-]|$)", local):
        return None
    # Provider-specific alias normalisation; never applies to Workspace domains.
    if domain in {"gmail.com", "googlemail.com"}:
        local = local.split("+", 1)[0].replace(".", "")
        domain = "gmail.com"
    return f"{local}@{domain}"


def phone(value, region="AU"):
    import phonenumbers
    if not value:
        return None
    try:
        number = phonenumbers.parse(value, region or "AU")
        if phonenumbers.is_possible_number(number):
            return phonenumbers.format_number(number, phonenumbers.PhoneNumberFormat.E164)
    except phonenumbers.NumberParseException:
        pass
    return None


def address(value):
    # Source fields already separate suburb/state/postcode. Preserve unit numbers
    # and Unicode. A native libpostal install is optional, never silently assumed.
    normalized = text(value)
    if normalized is None:
        return None
    replacements = {"street": "st", "road": "rd", "avenue": "ave", "lane": "ln",
                    "drive": "dr", "court": "ct", "apartment": "unit"}
    return " ".join(replacements.get(token, token) for token in normalized.split())


def given_key(value):
    """First given-name token, accent/case folded. Used only to count distinct users."""
    normalized = text(value)
    return normalized.split()[0] if normalized else None


def dob_variant(left, right):
    """Classify two valid DOBs: EXACT, DAY_MONTH_SWAP, YEAR_DIGIT, DAY_DIGIT or CONFLICT.

    Real-world data-entry variants (dd/mm vs mm/dd order and a single mistyped digit)
    are distinguished from genuinely different birthdays. None if either is missing.
    """
    if left is None or right is None:
        return None
    if left == right:
        return "EXACT"
    if left.year == right.year and left.month == right.day and left.day == right.month:
        return "DAY_MONTH_SWAP"

    def one_digit(a, b, width):
        x, y = f"{a:0{width}d}", f"{b:0{width}d}"
        return sum(p != q for p, q in zip(x, y)) == 1

    if (left.month, left.day) == (right.month, right.day) and one_digit(left.year, right.year, 4):
        return "YEAR_DIGIT"
    if (left.year, left.month) == (right.year, right.month) and one_digit(left.day, right.day, 2):
        return "DAY_DIGIT"
    return "CONFLICT"


# Development evidence (CHANGELOG): among candidate pairs with two valid DOBs, 139
# same-person pairs differ by a day/month swap and none by a single year/day digit.
COMPATIBLE_DOB_VARIANTS = frozenset({"EXACT", "DAY_MONTH_SWAP"})


def dob_compatible(left, right):
    variant = dob_variant(left, right)
    return None if variant is None else variant in COMPATIBLE_DOB_VARIANTS


def _edit_distance(a, b):
    """Damerau-Levenshtein (optimal string alignment) distance."""
    d = [[0] * (len(b) + 1) for _ in range(len(a) + 1)]
    for i in range(len(a) + 1):
        d[i][0] = i
    for j in range(len(b) + 1):
        d[0][j] = j
    for i in range(1, len(a) + 1):
        for j in range(1, len(b) + 1):
            cost = a[i - 1] != b[j - 1]
            d[i][j] = min(d[i - 1][j] + 1, d[i][j - 1] + 1, d[i - 1][j - 1] + cost)
            if i > 1 and j > 1 and a[i - 1] == b[j - 2] and a[i - 2] == b[j - 1]:
                d[i][j] = min(d[i][j], d[i - 2][j - 2] + 1)
    return d[-1][-1]


def _typo(a, b):
    """One keyboard slip (insert/delete/substitute/transpose), two for long names."""
    limit = 1 if max(len(a), len(b)) < 8 else 2
    return min(len(a), len(b)) >= 3 and _edit_distance(a, b) <= limit


def given_variant(left, right):
    """EXACT, NICKNAME, PREFIX, TYPO, INITIAL or CONFLICT for two given-name keys (None if missing)."""
    from .nicknames import related
    a, b = given_key(left), given_key(right)
    if not a or not b:
        return None
    if a == b:
        return "EXACT"
    if related(a, b):
        return "NICKNAME"
    short, long_ = sorted((a, b), key=len)
    if len(short) == 1 and long_.startswith(short):
        return "INITIAL"
    if len(short) >= 3 and long_.startswith(short):
        return "PREFIX"
    if _typo(a, b):
        return "TYPO"
    return "CONFLICT"


def family_variant(left, right):
    """EXACT, TOKEN (shared token of a compound surname), TYPO or CONFLICT (None if missing)."""
    a, b = text(left), text(right)
    if not a or not b:
        return None
    if a == b or a.replace(" ", "") == b.replace(" ", ""):
        return "EXACT"
    if set(a.replace("-", " ").split()) & set(b.replace("-", " ").split()):
        return "TOKEN"
    if _typo(a.replace(" ", ""), b.replace(" ", "")):
        return "TYPO"
    return "CONFLICT"
