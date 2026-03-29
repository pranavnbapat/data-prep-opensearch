# improver_engine.py

from __future__ import annotations

import logging
from typing import Any, Dict, List, Tuple, Optional

from stages.improver.config import (PRIMARY_MODEL, EXTREME_CTX_THRESHOLD_TOK, NEAR_LIMIT_CTX_THRESHOLD_TOK,
                                    CHUNK_TARGET_TOK, CHUNK_OVERLAP_TOK, DEFAULT_NUM_PREDICT,
                                    COMBINE_NUM_PREDICT, SUMMARY_MAX_ATTEMPTS, METADATA_MAX_ATTEMPTS,
                                    METADATA_INPUT_MAX_CHARS, METADATA_EXISTING_VALUE_MAX_CHARS,
                                    BASE_VLLM_HOST, VLLM_API_KEY)
from stages.improver.extractors import extract_summary_json, extract_metadata_text, extract_metadata_keywords
from stages.improver.llm_client import call_vllm_chat, warm_up_model
from stages.improver.prompts import (UNIVERSAL_SUMMARY_PROMPT, DEFAULT_PROMPT, CHUNK_SUMMARY_PROMPT,
                                     COMBINE_PROMPT, METADATA_PROMPT, POLISH_PROMPT)
from stages.improver.text_utils import approx_token_count, split_into_tokenish_chunks, should_summarise_text
logger = logging.getLogger(__name__)


def _truncate_for_metadata(text: str, *, limit: int) -> str:
    if limit <= 0:
        return text
    if len(text) <= limit:
        return text
    head = max(0, limit // 2)
    tail = max(0, limit - head - 64)
    suffix = text[-tail:] if tail > 0 else ""
    return f"{text[:head]}\n\n[... truncated ...]\n\n{suffix}"


def _summary_stats(source_text: str, summary_payload: Dict[str, Any], *, mode: str) -> Dict[str, Any]:
    source_words = len((source_text or "").split())
    summary_text = str(summary_payload.get("summary") or "").strip()
    summary_words = len(summary_text.split())
    compression_ratio = round(source_words / max(summary_words, 1), 3) if source_words > 0 else None
    return {
        "mode": mode,
        "summary": summary_text,
        "coverage_score": summary_payload.get("coverage_score"),
        "density_score": summary_payload.get("density_score"),
        "compression_judgement": summary_payload.get("compression_judgement"),
        "faithfulness_confidence": summary_payload.get("faithfulness_confidence"),
        "notes": summary_payload.get("notes"),
        "source_word_count": source_words,
        "summary_word_count": summary_words,
        "source_char_count": len(source_text or ""),
        "summary_char_count": len(summary_text),
        "compression_ratio": compression_ratio,
    }


def _require_model() -> str:
    model = (PRIMARY_MODEL or "").strip()
    if not model:
        raise RuntimeError("Missing VLLM_MODEL for improver")
    if not (BASE_VLLM_HOST or "").strip():
        raise RuntimeError("Missing RUNPOD_VLLM_HOST for improver")
    if not (VLLM_API_KEY or "").strip():
        raise RuntimeError("Missing VLLM_API_KEY (token) for improver")
    return model


def _summarise_text(model: str, text: str) -> Dict[str, Any]:
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
                return extract_summary_json(raw)

            max_toks = DEFAULT_NUM_PREDICT if tok <= NEAR_LIMIT_CTX_THRESHOLD_TOK else DEFAULT_NUM_PREDICT
            raw = call_vllm_chat(
                model=model,
                prompt=UNIVERSAL_SUMMARY_PROMPT,
                content=text,
                options_override={"max_tokens": DEFAULT_NUM_PREDICT},
            )
            return extract_summary_json(raw)


        except Exception as e:
            if attempt < SUMMARY_MAX_ATTEMPTS:
                logger.warning("[Improver] summary attempt %d failed: %s", attempt, e)
            else:
                raise


def _polish_summary_text(model: str, text: str) -> Dict[str, Any]:
    for attempt in range(1, SUMMARY_MAX_ATTEMPTS + 1):
        try:
            raw = call_vllm_chat(
                model=model,
                prompt=POLISH_PROMPT,
                content=text,
                options_override={"max_tokens": DEFAULT_NUM_PREDICT, "temperature": 0.1},
            )
            return extract_summary_json(raw)
        except Exception as e:
            if attempt < SUMMARY_MAX_ATTEMPTS:
                logger.warning("[Improver] polish attempt %d failed: %s", attempt, e)
            else:
                raise


def _run_metadata_field(
    *,
    model: str,
    summary_en: str,
    field: str,
    existing_value: Any | None,
    llid: Optional[str] = None,
) -> Any:
    existing_str = ""
    if isinstance(existing_value, list):
        existing_str = ", ".join(map(str, existing_value))
    elif isinstance(existing_value, str):
        existing_str = existing_value

    original_summary_len = len(summary_en or "")
    original_existing_len = len(existing_str or "")
    summary_en = _truncate_for_metadata(summary_en or "", limit=METADATA_INPUT_MAX_CHARS)
    existing_str = _truncate_for_metadata(existing_str or "", limit=METADATA_EXISTING_VALUE_MAX_CHARS)
    if len(summary_en) < original_summary_len or len(existing_str) < original_existing_len:
        logger.warning(
            "[ImproverMetadataTruncate] id=%s field=%s summary_chars=%s->%s existing_chars=%s->%s",
            llid,
            field,
            original_summary_len,
            len(summary_en),
            original_existing_len,
            len(existing_str),
        )

    meta_context = (
        f"FIELD: {field}\n\n"
        f"EXISTING VALUE:\n{existing_str}\n\n"
        f"SUMMARY:\n{summary_en}"
    )

    last_error: Exception | None = None
    for attempt in range(1, METADATA_MAX_ATTEMPTS + 1):
        try:
            raw = call_vllm_chat(
                model=model,
                prompt=METADATA_PROMPT,
                content=meta_context,
                options_override={"max_tokens": DEFAULT_NUM_PREDICT, "temperature": 0.3},
            )

            if field == "KEYWORDS":
                return extract_metadata_keywords(raw)

            return extract_metadata_text(raw)
        except Exception as e:
            last_error = e
            if attempt < METADATA_MAX_ATTEMPTS:
                logger.warning("[Improver] metadata field=%s attempt %d failed: %s", field, attempt, e)
            else:
                raise

    if last_error is not None:
        raise last_error
    raise RuntimeError(f"Metadata generation failed for field={field}")


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
        llid = str(doc.get("_orig_id") or doc.get("_id") or "")

        content = doc.get("ko_content_flat")
        if not isinstance(content, str) or not content.strip():
            # No content → do not LLM
            doc["improved"] = 0
            return False, "missing_ko_content_flat"

        # Warm-up once per process call (cheap no-op after first)
        warm_up_model(model)

        # --- summary ---
        summary_field = "ko_content_flat_summarised"
        summary_stats_field = "ko_content_flat_summarised_stats"
        summary = doc.get(summary_field)
        summary_flag = int(doc.get("ko_content_is_summary") or 0) == 1
        vision_summary = doc.get("ko_content_flat_vision")

        if not (isinstance(summary, str) and summary.strip()):
            if summary_flag and isinstance(vision_summary, str) and vision_summary.strip():
                polished_payload = _polish_summary_text(model, vision_summary)
                doc[summary_field] = polished_payload["summary"]
                doc[summary_stats_field] = _summary_stats(
                    vision_summary,
                    polished_payload,
                    mode="light_polish_from_vision_summary",
                )
            elif not should_summarise_text(content):
                doc[summary_field] = content
                doc[summary_stats_field] = _summary_stats(
                    content,
                    {
                        "summary": content,
                        "coverage_score": 1.0,
                        "density_score": 1.0,
                        "compression_judgement": "low",
                        "faithfulness_confidence": 1.0,
                        "notes": "Summary skipped because content was already short.",
                    },
                    mode="copied_short_content",
                )
            else:
                summary_payload = _summarise_text(model, content)
                doc[summary_field] = summary_payload["summary"]
                doc[summary_stats_field] = _summary_stats(content, summary_payload, mode="llm")
        elif not isinstance(doc.get(summary_stats_field), dict):
            doc[summary_stats_field] = _summary_stats(
                content,
                {
                    "summary": summary,
                    "coverage_score": None,
                    "density_score": None,
                    "compression_judgement": None,
                    "faithfulness_confidence": None,
                    "notes": "Summary stats reconstructed from existing summary.",
                },
                mode="reconstructed",
            )

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
            v = _run_metadata_field(model=model, summary_en=summary_text, field="TITLE", existing_value=existing_title, llid=llid)
            if not isinstance(v, str) or not v.strip():
                v = existing_title
            doc["title_llm"] = v

        if not doc.get("subtitle_llm"):
            existing_subtitle = doc.get("subtitle", "")
            v = _run_metadata_field(model=model, summary_en=summary_text, field="SUBTITLE", existing_value=existing_subtitle, llid=llid)
            if not isinstance(v, str) or not v.strip():
                v = existing_subtitle
            doc["subtitle_llm"] = v

        if not doc.get("description_llm"):
            existing_description = doc.get("description", "")
            v = _run_metadata_field(model=model, summary_en=summary_text, field="DESCRIPTION", existing_value=existing_description, llid=llid)
            if not isinstance(v, str) or not v.strip():
                v = existing_description
            doc["description_llm"] = v

        if not doc.get("keywords_llm"):
            existing_keywords = doc.get("keywords", [])
            v = _run_metadata_field(model=model, summary_en=summary_text, field="KEYWORDS", existing_value=existing_keywords, llid=llid)
            doc["keywords_llm"] = v

        doc["improved"] = 1
        return True, None

    except Exception as e:
        logger.exception("[Improver] improve_doc_in_place failed: %s", e)
        doc["improved"] = int(doc.get("improved") or 0)  # do not force to 1
        return False, str(e)
