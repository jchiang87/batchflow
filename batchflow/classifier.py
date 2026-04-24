"""
classifier.py — ErrorClassifier with user-extensible pattern registry.

The classifier looks at HTCondor HoldReason strings and decides whether
a failure is transient (restart is worth trying) or fatal (human or
agent attention needed).

Pattern registry
----------------
Loaded from a YAML file at construction time.  The built-in patterns
cover common HTCondor and cluster infrastructure failures; site-specific
patterns (e.g. LSST Butler errors) are added via the same file.

    # error_patterns.yaml
    transient:
      - pattern: "Failed to send file"
      - pattern: "Network timeout"
      - pattern: "Worker evicted"
        max_restarts: 5          # optional per-pattern override
      - pattern: "Docker pull"
    fatal:
      - pattern: "dataset not found"
      - pattern: "Permission denied"
      - pattern: "Unknown instrument"
      - pattern: "Butler registry"

Classification output
---------------------
The classifier returns a Classification dataclass rather than a bare
string so consumers can inspect confidence and act on suggested_action
without re-implementing the logic.
"""
from __future__ import annotations

import re
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

import yaml

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Built-in patterns (shipped with the package)
# ---------------------------------------------------------------------------

_BUILTIN_TRANSIENT = [
    r"Failed to send file",
    r"Network timeout",
    r"Worker evicted",
    r"Job was evicted",
    r"Docker.*pull",
    r"connection reset",
    r"Unable to connect",
    r"No space left on device",
    r"Disk quota exceeded",
    r"Preempting",
    r"shadow exception",
    r"startd reported",
]

_BUILTIN_FATAL = [
    r"dataset not found",
    r"Butler registry",
    r"Permission denied",
    r"No such file or directory",
    r"Unknown instrument",
    r"ValueError",
    r"KeyError",
    r"ImportError",
    r"MemoryError",
    r"Segmentation fault",
]


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Classification:
    """
    Result of classifying one or more HTCondor hold reasons.

    Attributes
    ----------
    verdict : str
        ``"transient"``, ``"fatal"``, or ``"unknown"``.
    confidence : float
        0.0–1.0.  1.0 means every hold reason matched the verdict bucket;
        lower values indicate mixed or unrecognised reasons.
    suggested_action : str
        ``"restart"``, ``"abort"``, or ``"investigate"``.
    matched_patterns : list[str]
        The patterns that fired, for logging/audit.
    """
    verdict:          str
    confidence:       float
    suggested_action: str
    matched_patterns: list[str]


# ---------------------------------------------------------------------------
# Classifier
# ---------------------------------------------------------------------------

class ErrorClassifier:
    """
    Classifies HTCondor hold reasons as transient or fatal.

    Parameters
    ----------
    patterns_file : str | Path | None
        Optional YAML file with site-specific patterns (see module
        docstring).  Built-in patterns are always loaded.
    """

    def __init__(
        self,
        patterns_file: str | Path | None = None,
    ) -> None:
        self._transient: list[re.Pattern] = [
            re.compile(p, re.IGNORECASE) for p in _BUILTIN_TRANSIENT
        ]
        self._fatal: list[re.Pattern] = [
            re.compile(p, re.IGNORECASE) for p in _BUILTIN_FATAL
        ]

        if patterns_file:
            self._load_file(Path(patterns_file))

    def _load_file(self, path: Path) -> None:
        if not path.exists():
            log.warning("ErrorClassifier: patterns file not found: %s", path)
            return
        with open(path) as fh:
            data = yaml.safe_load(fh) or {}
        for entry in data.get("transient", []):
            pat = entry["pattern"] if isinstance(entry, dict) else entry
            self._transient.append(re.compile(pat, re.IGNORECASE))
        for entry in data.get("fatal", []):
            pat = entry["pattern"] if isinstance(entry, dict) else entry
            self._fatal.append(re.compile(pat, re.IGNORECASE))
        log.debug(
            "ErrorClassifier: loaded %d transient + %d fatal patterns from %s",
            len(self._transient), len(self._fatal), path,
        )

    def add_transient(self, pattern: str) -> None:
        """Programmatically add a transient pattern at runtime."""
        self._transient.append(re.compile(pattern, re.IGNORECASE))

    def add_fatal(self, pattern: str) -> None:
        """Programmatically add a fatal pattern at runtime."""
        self._fatal.append(re.compile(pattern, re.IGNORECASE))

    def classify(self, hold_reasons: Sequence[str]) -> Classification:
        """
        Classify a collection of hold-reason strings.

        Each reason is matched independently; the overall verdict is the
        majority bucket (ties go to 'fatal' to be conservative).
        Unmatched reasons count as 'unknown'.
        """
        if not hold_reasons:
            return Classification(
                verdict="unknown",
                confidence=0.0,
                suggested_action="investigate",
                matched_patterns=[],
            )

        transient_hits: list[str] = []
        fatal_hits:     list[str] = []
        unknown_count = 0

        for reason in hold_reasons:
            matched_t = [p.pattern for p in self._transient if p.search(reason)]
            matched_f = [p.pattern for p in self._fatal     if p.search(reason)]

            if matched_t and not matched_f:
                transient_hits.extend(matched_t)
            elif matched_f:
                fatal_hits.extend(matched_f)
            else:
                unknown_count += 1
                log.debug("Unmatched hold reason: %r", reason)

        total = len(hold_reasons)
        n_transient = len([r for r in hold_reasons
                           if any(p.search(r) for p in self._transient)])
        n_fatal     = len([r for r in hold_reasons
                           if any(p.search(r) for p in self._fatal)])

        if n_transient > n_fatal and n_transient > unknown_count:
            verdict = "transient"
            confidence = n_transient / total
            action = "restart"
            patterns = transient_hits
        elif n_fatal > 0:
            verdict = "fatal"
            confidence = n_fatal / total
            action = "abort"
            patterns = fatal_hits
        else:
            verdict = "unknown"
            confidence = 0.0
            action = "investigate"
            patterns = []

        return Classification(
            verdict=verdict,
            confidence=round(confidence, 2),
            suggested_action=action,
            matched_patterns=patterns,
        )
