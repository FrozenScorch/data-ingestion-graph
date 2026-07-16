"""Deterministic native-text quality heuristics."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TextQuality:
    score: float
    character_count: int
    replacement_ratio: float
    reason: str


def evaluate_text_quality(text: str) -> TextQuality:
    visible = [char for char in text if not char.isspace()]
    count = len(visible)
    if not visible:
        return TextQuality(0.0, 0, 0.0, "empty")
    replacements = sum(char in "�\x00" for char in visible)
    replacement_ratio = replacements / count
    alnum = sum(char.isalnum() for char in visible) / count
    score = max(0.0, min(1.0, min(1.0, count / 200.0) * 0.65 + alnum * 0.35 - replacement_ratio))
    reason = "sufficient" if score >= 0.65 else "low_text_quality"
    return TextQuality(score, count, replacement_ratio, reason)
