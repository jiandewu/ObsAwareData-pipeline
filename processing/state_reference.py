#!/usr/bin/env python3
"""Canonical state reference table for the 50-state processing.

Single source of truth for the postal abbreviation / full name / 2-digit FIPS
code triple every per-state processing script needs once `--state` replaces a hardcoded SC
constant, so the mapping is defined once instead of duplicated (and risking
drift) across scripts. Scope is exactly the 50 states plus DC
(Guam, U.S. Virgin Islands, Puerto Rico, and other territories are out of
scope for now - each is missing at least one core data stream in currently
acquired sources). FIPS codes and full names are cross-checked against, and
match exactly, the nationwide `STATE_FIPS` mapping `extract_nwss.py`
already validated against the real NWSS file (0 unmapped jurisdiction names).
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass


@dataclass(frozen=True)
class StateInfo:
    abbr: str
    name: str
    fips: str


# (postal abbreviation, full name as used by NSSP/NWSS/Census, 2-digit FIPS)
_STATES = [
    ("AL", "Alabama", "01"), ("AK", "Alaska", "02"), ("AZ", "Arizona", "04"),
    ("AR", "Arkansas", "05"), ("CA", "California", "06"), ("CO", "Colorado", "08"),
    ("CT", "Connecticut", "09"), ("DE", "Delaware", "10"), ("DC", "District of Columbia", "11"),
    ("FL", "Florida", "12"), ("GA", "Georgia", "13"), ("HI", "Hawaii", "15"),
    ("ID", "Idaho", "16"), ("IL", "Illinois", "17"), ("IN", "Indiana", "18"),
    ("IA", "Iowa", "19"), ("KS", "Kansas", "20"), ("KY", "Kentucky", "21"),
    ("LA", "Louisiana", "22"), ("ME", "Maine", "23"), ("MD", "Maryland", "24"),
    ("MA", "Massachusetts", "25"), ("MI", "Michigan", "26"), ("MN", "Minnesota", "27"),
    ("MS", "Mississippi", "28"), ("MO", "Missouri", "29"), ("MT", "Montana", "30"),
    ("NE", "Nebraska", "31"), ("NV", "Nevada", "32"), ("NH", "New Hampshire", "33"),
    ("NJ", "New Jersey", "34"), ("NM", "New Mexico", "35"), ("NY", "New York", "36"),
    ("NC", "North Carolina", "37"), ("ND", "North Dakota", "38"), ("OH", "Ohio", "39"),
    ("OK", "Oklahoma", "40"), ("OR", "Oregon", "41"), ("PA", "Pennsylvania", "42"),
    ("RI", "Rhode Island", "44"), ("SC", "South Carolina", "45"), ("SD", "South Dakota", "46"),
    ("TN", "Tennessee", "47"), ("TX", "Texas", "48"), ("UT", "Utah", "49"),
    ("VT", "Vermont", "50"), ("VA", "Virginia", "51"), ("WA", "Washington", "53"),
    ("WV", "West Virginia", "54"), ("WI", "Wisconsin", "55"), ("WY", "Wyoming", "56"),
]

STATES: dict[str, StateInfo] = {abbr: StateInfo(abbr, name, fips) for abbr, name, fips in _STATES}
IN_SCOPE_ABBRS: frozenset[str] = frozenset(STATES.keys())
NAME_BY_ABBR: dict[str, str] = {abbr: s.name for abbr, s in STATES.items()}
FIPS_BY_ABBR: dict[str, str] = {abbr: s.fips for abbr, s in STATES.items()}
ABBR_BY_NAME: dict[str, str] = {s.name: abbr for abbr, s in STATES.items()}
ABBR_BY_FIPS: dict[str, str] = {s.fips: abbr for abbr, s in STATES.items()}

assert len(STATES) == 51, "expected exactly 50 states + DC"

# County-equivalent naming suffixes confirmed nationwide by audit_national_geography.py - checked
# longest/most-specific first so e.g. "... City and Borough" doesn't match on "Borough" alone.
# Some counties genuinely have no suffix at all (Connecticut's planning regions, DC, Virginia's
# independent cities in this list's " city" form only) - callers must handle a no-match case.
COUNTY_NAME_SUFFIXES = [
    " City and Borough",
    " Census Area",
    " Borough",
    " Parish",
    " Municipio",
    " Municipality",
    " city",
    " County",
    " District",
]

_ST_TOKEN_RE = re.compile(r"\bst\b")


def normalize_county_name(name: str) -> str:
    """Suffix-stripped, case/abbreviation/spacing-insensitive key for matching one
    source's county-equivalent name against another's for the *same* state (e.g.
    NWSS's `Counties_Served` against Census's `CTYNAME`, or against NSSP's own
    `raw_geography_id` county list). Closes three real, observed mismatch patterns
    (found testing build_nwss_county_geography.py/build_hsa_combined.py against Louisiana/Alaska/Virginia's real
    data, not just asserted): `St.`/`Saint` abbreviation (Louisiana's `St. Bernard
    Parish` vs. `Saint Bernard`; also several of the national geography audit's Missouri/Wisconsin
    unmatched-county findings, e.g. `Saint Louis`), `LaSalle`/`La Salle` spacing, and
    a suffix written in a different case than Census's own (Virginia's NWSS data
    capitalizes `City` - `Petersburg City` - where Census's `CTYNAME` is lowercase
    `Petersburg city`; casefold happens *before* the suffix check here specifically
    so a differently-cased suffix still gets recognized and stripped).

    Two genuinely different jurisdictions can still collide after this
    normalization (e.g. Virginia's `Fairfax`/`Franklin` city vs. county) - that is
    a real ambiguity, not a bug in this function, and callers must treat a
    collision as unresolvable rather than picking one side arbitrarily.

    Also folds out possessive apostrophes (found testing
    Maryland's real data): Census's `CTYNAME` spells `St. Mary's County`,
    `Prince George's County`, and `Queen Anne's County` with a `'s`, while
    NWSS's own `Counties_Served` and NSSP's `raw_geography_id` both spell the
    same counties without the apostrophe (`Saint Marys`, `Prince Georges`) -
    an unnormalized apostrophe caused Maryland's real `ID:736` NWSS site to be
    wrongly excluded from `build_nwss_county_geography.py`'s county-level table
    as a false "no match" rather than resolving (it already resolved fine at
    HSA grain via `build_hsa_combined.py`, which matches against NSSP's own
    apostrophe-free county list, not Census's - confirming the gap was
    specific to this function's apostrophe-handling, not a genuine geography
    ambiguity). Curly and straight apostrophes are both handled.

    Also folds out diacritics (found testing New
    Mexico's real data): Census's `CTYNAME` spells `Doña Ana County` with a
    tilde, while NWSS's own `Counties_Served` spells the same county `Dona
    Ana` (no diacritic, and not multi-county at all) - an unnormalized ñ
    caused this single-county NM site to be wrongly reported as an unmatched
    (would-be cross-state) site in `build_nwss_county_geography.py`'s pending
    report instead of resolving as an ordinary single-county match. NFKD-
    decomposing and stripping combining marks folds any accented Latin
    letter to its base form before the suffix/apostrophe/`St.` handling
    above runs.
    """
    folded = name.casefold()
    folded = "".join(
        ch for ch in unicodedata.normalize("NFKD", folded) if not unicodedata.combining(ch)
    )
    for suffix in COUNTY_NAME_SUFFIXES:
        folded_suffix = suffix.casefold()
        if folded.endswith(folded_suffix):
            folded = folded[: -len(folded_suffix)]
            break
    folded = folded.replace(".", "")
    folded = folded.replace("'", "").replace("’", "")
    folded = _ST_TOKEN_RE.sub("saint", folded)
    return re.sub(r"\s+", "", folded)


def resolve_state(abbr: str) -> StateInfo:
    """Look up a state by its 2-letter postal abbreviation (case-insensitive).

    Raises ValueError (not a silent None) for anything outside the 50-states-plus-DC
    scope - including Guam/USVI/Puerto Rico/other territories, which are excluded on
    purpose (see module docstring), and HHS-region/USA-aggregate values that must
    never be treated as a state.
    """
    key = abbr.strip().upper()
    if key not in STATES:
        raise ValueError(
            f"'{abbr}' is not one of the 50 states + DC in scope. "
            f"Valid values: {sorted(STATES.keys())}"
        )
    return STATES[key]
