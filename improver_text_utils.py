# improver_text_utils.py

from __future__ import annotations

from typing import List


def approx_token_count(text: str) -> int:
    return max(1, int(len(text) / 4))


def split_into_tokenish_chunks(text: str, chunk_tok: int, overlap_tok: int) -> List[str]:
    step = max(1, (chunk_tok - overlap_tok) * 4)
    width = max(step, chunk_tok * 4)
    chunks: List[str] = []
    i = 0
    n = len(text)
    while i < n:
        chunks.append(text[i:i + width])
        i += step
    return chunks


def should_summarise_text(text: str, min_tokens: int = 60) -> bool:
    if not isinstance(text, str):
        return False
    stripped = text.strip()
    if not stripped:
        return False
    if approx_token_count(stripped) < min_tokens:
        return False
    letters = sum(c.isalpha() for c in stripped)
    if letters and (letters / len(stripped)) < 0.4:
        return False
    return True
