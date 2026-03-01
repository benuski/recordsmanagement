#!/usr/bin/env python3
"""Quick test to verify word2number replacement works correctly."""

import re
from word2number import w2n

# Test cases from retention schedules
test_cases = [
    ("Retain 5 years", 5),
    ("Retain five years", 5),
    ("Retain ten years", 10),
    ("Retain fifteen years", 15),
    ("Retain twenty years", 20),
    ("Retain seventy-five years", 75),
    ("Retain one year", 1),
    ("Retain two years", 2),
    ("permanently", None),
    ("Retain indefinitely", None),
]

def extract_retention_years(text: str) -> int | None:
    """Mimics the logic from extractor_engine.py and oh/parser.py"""
    digit_match = re.search(r'(\d+)\s*year', text, re.IGNORECASE)
    word_match = re.search(r'\b([a-zA-Z]+(?:-[a-zA-Z]+)?)\b\s*year', text, re.IGNORECASE)

    if digit_match:
        return int(digit_match.group(1))
    elif word_match:
        try:
            return w2n.word_to_num(word_match.group(1).lower())
        except ValueError:
            return None
    elif 'permanent' in text.lower():
        return None
    else:
        return None

print("Testing word2number integration...\n")
passed = 0
failed = 0

for test_input, expected in test_cases:
    result = extract_retention_years(test_input)
    status = "✓" if result == expected else "✗"

    if result == expected:
        passed += 1
        print(f"{status} '{test_input}' -> {result}")
    else:
        failed += 1
        print(f"{status} '{test_input}' -> {result} (expected {expected})")

print(f"\n{'='*50}")
print(f"Results: {passed} passed, {failed} failed")

if failed == 0:
    print("All tests passed! ✓")
    exit(0)
else:
    print(f"Some tests failed. Please review.")
    exit(1)
