from __future__ import annotations

import re


DATE_RE = re.compile(r"\b(?:\d{1,2}[/-]){2}\d{2,4}\b")
CURRENCY_RE = re.compile(r"(?<!\w)(?:USD|EUR|GBP|\$|€|£)\s?\d[\d,]*(?:\.\d+)?(?:\s?[mbkMBK])?")
PERCENT_RE = re.compile(r"\b\d+(?:\.\d+)?%")
NUMBER_RE = re.compile(r"\b\d[\d,]*(?:\.\d+)?\b")


def delexicalize_text(text: str) -> str:
    value = text or ""
    value = DATE_RE.sub("[DATE]", value)
    value = CURRENCY_RE.sub("[CURRENCY]", value)
    value = PERCENT_RE.sub("[PERCENT]", value)
    value = NUMBER_RE.sub("[NUMBER]", value)
    return value
