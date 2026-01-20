# improver_engine.py

from __future__ import annotations

import logging
from typing import Any, Dict, List, Tuple, Optional

from improver_config import (
    PRIMARY_MODEL,
    EXTREME_CTX_THRESHOLD_TOK,
    NEAR_LIMIT_CTX_THRESHOLD_TOK,
    CHUNK_TARGET_TOK,
    CHUNK_OVERLAP_TOK,
    DEFAULT_NUM_PREDICT,
    COMBINE_NUM_PREDICT,
    SUMMARY_MAX_ATTEMPTS,
    BASE_VLLM_HOST,
)
from improver_llm_client import call_vllm_chat, warm_up_model
from improver_prompts import (UNIVERSAL_SUMMARY_PROMPT, DEFAULT_PROMPT, CHUNK_SUMMARY_PROMPT, COMBINE_PROMPT,
                              METADATA_PROMPT)
from improver_text_utils import approx_token_count, split_into_tokenish_chunks, should_summarise_text
from improver_extractors import extract_summary_json, extract_metadata_text, extract_metadata_keywords

logger = logging.getLogger(__name__)


def _require_model() -> str:
    model = (PRIMARY_MODEL or "").strip()
    if not model:
        raise RuntimeError("Missing VLLM_MODEL for improver")
    if not (BASE_VLLM_HOST or "").strip():
        raise RuntimeError("Missing RUNPOD_VLLM_HOST for improver")
    return model


def _summarise_text(model: str, text: str) -> str:
    tok = approx_token_count(text)

    for attempt in range(1, SUMMARY_MAX_ATTEMPTS + 1):
        try:
            # Chunk if huge
            if tok > EXTREME_CTX_THRESHOLD_TOK:
                chunks = split_into_tokenish_chunks(text, CHUNK_TARGET_TOK, CHUNK_OVERLAP_TOK)
                partials: List[str] = []

                for ch in chunks:
                    raw = call_vllm_chat(
                        model=model,
                        prompt=CHUNK_SUMMARY_PROMPT,
                        content=ch,
                        options_override={"max_tokens": DEFAULT_NUM_PREDICT},
                    )
                    partials.append(extract_summary_json(raw)["summary"])

                combined = "\n\n---- PART ----\n".join(partials)

                raw = call_vllm_chat(
                    model=model,
                    prompt=COMBINE_PROMPT,
                    content=combined,
                    options_override={"max_tokens": COMBINE_NUM_PREDICT},
                )
                return extract_summary_json(raw)["summary"]

            max_toks = DEFAULT_NUM_PREDICT if tok <= NEAR_LIMIT_CTX_THRESHOLD_TOK else DEFAULT_NUM_PREDICT
            raw = call_vllm_chat(
                model=model,
                prompt=UNIVERSAL_SUMMARY_PROMPT,
                content=text,
                options_override={"max_tokens": DEFAULT_NUM_PREDICT},
            )
            return extract_summary_json(raw)["summary"]


        except Exception as e:
            if attempt < SUMMARY_MAX_ATTEMPTS:
                logger.warning("[Improver] summary attempt %d failed: %s", attempt, e)
            else:
                raise


def _run_metadata_field(
    *,
    model: str,
    summary_en: str,
    field: str,
    existing_value: Any | None,
) -> Any:
    existing_str = ""
    if isinstance(existing_value, list):
        existing_str = ", ".join(map(str, existing_value))
    elif isinstance(existing_value, str):
        existing_str = existing_value

    meta_context = (
        f"FIELD: {field}\n\n"
        f"EXISTING VALUE:\n{existing_str}\n\n"
        f"SUMMARY:\n{summary_en}"
    )

    raw = call_vllm_chat(
        model=model,
        prompt=METADATA_PROMPT,
        content=meta_context,
        options_override={"max_tokens": DEFAULT_NUM_PREDICT, "temperature": 0.3},
    )

    if field == "KEYWORDS":
        return extract_metadata_keywords(raw)

    return extract_metadata_text(raw)


def improve_doc_in_place(doc: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """
    Mutate doc:
      - ko_content_flat_summarised
      - title_llm / subtitle_llm / description_llm / keywords_llm
      - doc["improved"]=1 on success

    Returns (ok, reason_if_failed).
    """
    try:
        model = _require_model()

        content = doc.get("ko_content_flat")
        if not isinstance(content, str) or not content.strip():
            # No content → do not LLM
            doc["improved"] = 0
            return False, "missing_ko_content_flat"

        # Warm-up once per process call (cheap no-op after first)
        warm_up_model(model)

        # --- summary ---
        summary_field = "ko_content_flat_summarised"
        summary = doc.get(summary_field)

        if not (isinstance(summary, str) and summary.strip()):
            # If it is too short, just copy content
            if not should_summarise_text(content):
                doc[summary_field] = content
            else:
                doc[summary_field] = _summarise_text(model, content)

        summary_text = doc.get(summary_field)
        summary_text = summary_text if isinstance(summary_text, str) else ""

        # --- metadata (title/subtitle/description/keywords) ---
        # Only attempt metadata generation if summary has enough substance
        if not summary_text.strip() or len(summary_text.split()) < 50:
            # Fallback: keep originals as the “llm” values (idempotent)
            doc.setdefault("title_llm", doc.get("title", ""))
            doc.setdefault("subtitle_llm", doc.get("subtitle", ""))
            doc.setdefault("description_llm", doc.get("description", ""))
            doc.setdefault("keywords_llm", doc.get("keywords", []))
            doc["improved"] = 1
            return True, None

        # Generate only if missing (idempotent)
        if not doc.get("title_llm"):
            existing_title = doc.get("title", "")
            v = _run_metadata_field(model=model, summary_en=summary_text, field="TITLE", existing_value=existing_title)
            if not isinstance(v, str) or not v.strip():
                v = existing_title
            doc["title_llm"] = v

        if not doc.get("subtitle_llm"):
            existing_subtitle = doc.get("subtitle", "")
            v = _run_metadata_field(model=model, summary_en=summary_text, field="SUBTITLE", existing_value=existing_subtitle)
            if not isinstance(v, str) or not v.strip():
                v = existing_subtitle
            doc["subtitle_llm"] = v

        if not doc.get("description_llm"):
            existing_description = doc.get("description", "")
            v = _run_metadata_field(model=model, summary_en=summary_text, field="DESCRIPTION", existing_value=existing_description)
            if not isinstance(v, str) or not v.strip():
                v = existing_description
            doc["description_llm"] = v

        if not doc.get("keywords_llm"):
            existing_keywords = doc.get("keywords", [])
            v = _run_metadata_field(model=model, summary_en=summary_text, field="KEYWORDS", existing_value=existing_keywords)
            doc["keywords_llm"] = v

        doc["improved"] = 1
        return True, None

    except Exception as e:
        logger.exception("[Improver] improve_doc_in_place failed: %s", e)
        doc["improved"] = int(doc.get("improved") or 0)  # do not force to 1
        return False, str(e)
