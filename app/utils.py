"""
app/utils.py
------------
Shared utility functions used across multiple modules.
"""

import csv
import io


def sniff_delimiter(raw: bytes, sample_size: int = 8192) -> str:
    """
    Auto-detect the delimiter of a delimited text file.

    Strategy:
    1. Try csv.Sniffer with no delimiter restriction — analyses full character
       set and picks the most consistent separator. Works for any delimiter
       including uncommon ones like ~ | ^ etc.
    2. If Sniffer fails (ambiguous or single-column), fall back to counting
       occurrences of common delimiters in the first line and picking the winner.
    3. Final fallback: comma.
    """
    sample = raw[:sample_size].decode("utf-8", errors="replace")

    # --- attempt 1: unrestricted Sniffer ---
    try:
        dialect = csv.Sniffer().sniff(sample)
        # Sanity check: delimiter should produce more than 1 column on the first line
        first_line = sample.split("\n")[0]
        if first_line.count(dialect.delimiter) >= 1:
            return dialect.delimiter
    except csv.Error:
        pass

    # --- attempt 2: count occurrences in header row ---
    first_line = sample.split("\n")[0]
    candidates = [",", "\t", ";", "|", "~", "^", ":", " "]
    counts = {d: first_line.count(d) for d in candidates}
    best = max(counts, key=counts.get)
    if counts[best] > 0:
        return best

    # --- fallback ---
    return ","


def fmt_delimiter(d: str) -> str:
    """Return a human-readable label for a delimiter character."""
    labels = {
        ",":  ", (comma)",
        "\t": "\\t (tab)",
        ";":  "; (semicolon)",
        "|":  "| (pipe)",
        "~":  "~ (tilde)",
        "^":  "^ (caret)",
        ":":  ": (colon)",
        " ":  "  (space)",
    }
    return labels.get(d, repr(d))
