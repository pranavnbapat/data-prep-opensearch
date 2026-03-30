from __future__ import annotations

import json
import logging
import os
import hashlib
import subprocess
import tempfile
from datetime import datetime
from typing import Any, Dict, List, Optional

from stages.downloader.fingerprints import compute_source_fp
from stages.enricher.utils import get_public_session, probe_remote_content_type
from stages.enricher.vision import _download_pdf_bytes, _pdf_page_count
logger = logging.getLogger(__name__)


def mysql_enabled() -> bool:
    return bool(
        (os.getenv("MYSQL_HOST") or "").strip()
        and (os.getenv("MYSQL_USER") or "").strip()
        and (os.getenv("MYSQL_PASSWORD") or "").strip()
        and (os.getenv("MYSQL_DATABASE") or "").strip()
    )


def _require_pymysql():
    try:
        import pymysql  # type: ignore
    except Exception as e:  # pragma: no cover - dependency gate
        raise RuntimeError(
            "MySQL support requires pymysql. Add it to the runtime environment before using MySQL endpoints."
        ) from e
    return pymysql


def _connect():
    pymysql = _require_pymysql()
    return pymysql.connect(
        host=(os.getenv("MYSQL_HOST") or "").strip(),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=(os.getenv("MYSQL_USER") or "").strip(),
        password=(os.getenv("MYSQL_PASSWORD") or "").strip(),
        database=(os.getenv("MYSQL_DATABASE") or "").strip(),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
        connect_timeout=int(os.getenv("MYSQL_CONNECT_TIMEOUT", "10")),
    )


def ensure_schema() -> None:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS ko_records (
                  llid VARCHAR(64) NOT NULL,
                  env_mode VARCHAR(8) NOT NULL,
                  source_doc_json LONGTEXT NOT NULL,
                  current_doc_json LONGTEXT NULL,
                  source_fp VARCHAR(64) NULL,
                  current_fp VARCHAR(64) NULL,
                  source_url TEXT NULL,
                  source_mimetype VARCHAR(255) NULL,
                  source_kind VARCHAR(32) NULL,
                  is_deferred TINYINT(1) NOT NULL DEFAULT 0,
                  pdf_page_count INT NULL,
                  deferred_reason VARCHAR(64) NULL,
                  processing_eligible TINYINT(1) NOT NULL DEFAULT 1,
                  processing_ineligible_reason VARCHAR(64) NULL,
                  security_fp VARCHAR(64) NULL,
                  security_status VARCHAR(16) NULL,
                  security_scope VARCHAR(16) NULL,
                  security_engine VARCHAR(32) NULL,
                  security_checked_at DATETIME NULL,
                  security_quarantined TINYINT(1) NOT NULL DEFAULT 0,
                  security_escalated TINYINT(1) NOT NULL DEFAULT 0,
                  security_reason VARCHAR(64) NULL,
                  sync_status VARCHAR(32) NOT NULL DEFAULT 'pending',
                  fast_pipeline_status VARCHAR(32) NOT NULL DEFAULT 'pending',
                  deferred_pipeline_status VARCHAR(32) NOT NULL DEFAULT 'pending',
                  synced_at DATETIME NULL,
                  enriched_at DATETIME NULL,
                  improved_at DATETIME NULL,
                  background_completed_at DATETIME NULL,
                  updated_at DATETIME NOT NULL,
                  PRIMARY KEY (llid, env_mode),
                  KEY idx_ko_records_env_deferred (env_mode, is_deferred),
                  KEY idx_ko_records_env_fast (env_mode, fast_pipeline_status),
                  KEY idx_ko_records_env_deferred_status (env_mode, deferred_pipeline_status)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS ko_records_history (
                  id BIGINT NOT NULL AUTO_INCREMENT,
                  llid VARCHAR(64) NOT NULL,
                  env_mode VARCHAR(8) NOT NULL,
                  history_kind VARCHAR(32) NOT NULL,
                  source_doc_json LONGTEXT NULL,
                  current_doc_json LONGTEXT NULL,
                  source_fp VARCHAR(64) NULL,
                  current_fp VARCHAR(64) NULL,
                  source_url TEXT NULL,
                  source_mimetype VARCHAR(255) NULL,
                  source_kind VARCHAR(32) NULL,
                  is_deferred TINYINT(1) NOT NULL DEFAULT 0,
                  pdf_page_count INT NULL,
                  deferred_reason VARCHAR(64) NULL,
                  processing_eligible TINYINT(1) NOT NULL DEFAULT 1,
                  processing_ineligible_reason VARCHAR(64) NULL,
                  security_fp VARCHAR(64) NULL,
                  security_status VARCHAR(16) NULL,
                  security_scope VARCHAR(16) NULL,
                  security_engine VARCHAR(32) NULL,
                  security_checked_at DATETIME NULL,
                  security_quarantined TINYINT(1) NOT NULL DEFAULT 0,
                  security_escalated TINYINT(1) NOT NULL DEFAULT 0,
                  security_reason VARCHAR(64) NULL,
                  sync_status VARCHAR(32) NULL,
                  fast_pipeline_status VARCHAR(32) NULL,
                  deferred_pipeline_status VARCHAR(32) NULL,
                  synced_at DATETIME NULL,
                  enriched_at DATETIME NULL,
                  improved_at DATETIME NULL,
                  background_completed_at DATETIME NULL,
                  updated_at DATETIME NULL,
                  archived_at DATETIME NOT NULL,
                  PRIMARY KEY (id),
                  KEY idx_ko_records_history_lookup (llid, env_mode, archived_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            _ensure_column(
                cur,
                table_name="ko_records",
                column_name="processing_eligible",
                ddl="ALTER TABLE ko_records ADD COLUMN processing_eligible TINYINT(1) NOT NULL DEFAULT 1",
            )
            _ensure_column(
                cur,
                table_name="ko_records",
                column_name="processing_ineligible_reason",
                ddl="ALTER TABLE ko_records ADD COLUMN processing_ineligible_reason VARCHAR(64) NULL",
            )
            _ensure_column(
                cur,
                table_name="ko_records_history",
                column_name="processing_eligible",
                ddl="ALTER TABLE ko_records_history ADD COLUMN processing_eligible TINYINT(1) NOT NULL DEFAULT 1",
            )
            _ensure_column(
                cur,
                table_name="ko_records_history",
                column_name="processing_ineligible_reason",
                ddl="ALTER TABLE ko_records_history ADD COLUMN processing_ineligible_reason VARCHAR(64) NULL",
            )
            for table_name in ("ko_records", "ko_records_history"):
                _ensure_column(
                    cur,
                    table_name=table_name,
                    column_name="source_kind",
                    ddl=f"ALTER TABLE {table_name} ADD COLUMN source_kind VARCHAR(32) NULL",
                )
            for table_name in ("ko_records", "ko_records_history"):
                prefix = f"ALTER TABLE {table_name} ADD COLUMN"
                _ensure_column(cur, table_name=table_name, column_name="security_fp", ddl=f"{prefix} security_fp VARCHAR(64) NULL")
                _ensure_column(cur, table_name=table_name, column_name="security_status", ddl=f"{prefix} security_status VARCHAR(16) NULL")
                _ensure_column(cur, table_name=table_name, column_name="security_scope", ddl=f"{prefix} security_scope VARCHAR(16) NULL")
                _ensure_column(cur, table_name=table_name, column_name="security_engine", ddl=f"{prefix} security_engine VARCHAR(32) NULL")
                _ensure_column(cur, table_name=table_name, column_name="security_checked_at", ddl=f"{prefix} security_checked_at DATETIME NULL")
                _ensure_column(cur, table_name=table_name, column_name="security_quarantined", ddl=f"{prefix} security_quarantined TINYINT(1) NOT NULL DEFAULT 0")
                _ensure_column(cur, table_name=table_name, column_name="security_escalated", ddl=f"{prefix} security_escalated TINYINT(1) NOT NULL DEFAULT 0")
                _ensure_column(cur, table_name=table_name, column_name="security_reason", ddl=f"{prefix} security_reason VARCHAR(64) NULL")
        conn.commit()


def _ensure_column(cur, *, table_name: str, column_name: str, ddl: str) -> None:
    cur.execute(
        """
        SELECT COUNT(*) AS c
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = %s
          AND COLUMN_NAME = %s
        """,
        (table_name, column_name),
    )
    row = cur.fetchone() or {}
    if int(row.get("c") or 0) == 0:
        cur.execute(ddl)


def _doc_llid(doc: Dict[str, Any]) -> Optional[str]:
    llid = doc.get("_orig_id") or doc.get("_id")
    return llid.strip() if isinstance(llid, str) and llid.strip() else None


def _fast_pdf_max_pages() -> int:
    return max(1, int(os.getenv("FAST_PIPELINE_PDF_MAX_PAGES", "10")))


def _processing_max_file_size_bytes() -> int:
    return max(1, int(os.getenv("PROCESSING_MAX_FILE_SIZE_BYTES", str(1024 ** 3))))


def _processing_max_pdf_pages() -> int:
    return max(1, int(os.getenv("PROCESSING_MAX_PDF_PAGES", "100")))


def _processing_max_office_pages() -> int:
    return max(1, int(os.getenv("PROCESSING_MAX_OFFICE_PAGES", "100")))


def _processing_max_text_bytes() -> int:
    return max(1, int(os.getenv("PROCESSING_MAX_TEXT_FILE_BYTES", str(5 * 1024 * 1024))))


def _processing_max_text_chars() -> int:
    return max(1, int(os.getenv("PROCESSING_MAX_TEXT_CHARS", "500000")))


def _processing_max_image_dim() -> int:
    return max(1, int(os.getenv("PROCESSING_MAX_IMAGE_DIM_PX", "10000")))


def _processing_max_media_duration_sec() -> float:
    return max(1.0, float(os.getenv("PROCESSING_MAX_MEDIA_DURATION_SEC", "3000")))


def _url_scan_strict() -> bool:
    return (os.getenv("AGRI_GATE_URL_STRICT", os.getenv("EUF_URL_SCAN_STRICT", "false")).strip().lower() in {"1", "true", "yes", "y", "on"})


def _file_scan_enabled() -> bool:
    return (os.getenv("AGRI_GATE_ENABLED", "true").strip().lower() in {"1", "true", "yes", "y", "on"})


def _file_scan_strict() -> bool:
    return (os.getenv("AGRI_GATE_FILE_STRICT", os.getenv("EUF_FILE_SCAN_STRICT", "false")).strip().lower() in {"1", "true", "yes", "y", "on"})


def _security_probe_timeout() -> int:
    return max(5, int(os.getenv("PROCESSING_PROBE_TIMEOUT", "30")))


def _agri_gate_enabled() -> bool:
    return (os.getenv("AGRI_GATE_ENABLED", "true").strip().lower() in {"1", "true", "yes", "y", "on"})


def _agri_gate_base_url() -> str:
    return (os.getenv("AGRI_GATE_BASE_URL") or "https://agrigate.nexavion.com").strip().rstrip("/")


def _agri_gate_api_token() -> str:
    return (os.getenv("AGRI_GATE_API_TOKEN") or "").strip()


def _agri_gate_timeout() -> int:
    return max(5, int(os.getenv("AGRI_GATE_TIMEOUT", "60")))


def _security_rescan_max_age_days() -> int:
    return max(0, int(os.getenv("SECURITY_RESCAN_MAX_AGE_DAYS", "30")))


def _source_url_from_doc(doc: Dict[str, Any]) -> Optional[str]:
    for key in ("@id", "ko_file_id"):
        value = doc.get(key)
        if isinstance(value, str):
            v = value.strip()
            if v:
                return v
    return None


def _is_generic_mimetype(value: Optional[str]) -> bool:
    v = (value or "").strip().lower()
    return v in {"", "application/octet-stream", "binary/octet-stream"}


def _normalize_source_metadata(doc: Dict[str, Any]) -> tuple[Dict[str, Any], Optional[str], Optional[str], str]:
    out = dict(doc)
    source_url = _source_url_from_doc(out)
    ko_is_hosted = bool(out.get("ko_is_hosted"))
    existing_mimetype = (out.get("ko_object_mimetype") or "").strip().lower() if isinstance(out.get("ko_object_mimetype"), str) else ""
    final_mimetype = existing_mimetype or None

    if source_url:
        probed = probe_remote_content_type(source_url, timeout=_security_probe_timeout())
        if isinstance(probed, str):
            probed = probed.strip().lower()
            if probed and (not final_mimetype or _is_generic_mimetype(final_mimetype) or probed != "application/octet-stream"):
                final_mimetype = probed

    if ko_is_hosted:
        source_kind = "hosted_file"
    else:
        if final_mimetype in {"text/html", "application/xhtml+xml"}:
            source_kind = "webpage"
        elif isinstance(final_mimetype, str) and final_mimetype.strip():
            source_kind = "external_file"
        else:
            source_kind = "external_url"

    if final_mimetype:
        out["ko_object_mimetype"] = final_mimetype
    out["source_kind"] = source_kind
    return out, source_url, final_mimetype, source_kind


def _security_decision(*, status: str, scope: str, engine: str, reason: str, checked_at: Optional[datetime] = None, quarantined: bool = False, escalated: bool = False) -> Dict[str, Any]:
    return {
        "status": status,
        "scope": scope,
        "engine": engine,
        "checked_at": checked_at or datetime.utcnow(),
        "quarantined": quarantined,
        "escalated": escalated,
        "reason": reason,
    }


def _compute_security_fp(doc: Dict[str, Any]) -> str:
    ko_is_hosted = bool(doc.get("ko_is_hosted"))
    if ko_is_hosted:
        size_raw = doc.get("ko_object_size")
        try:
            size_val = int(size_raw) if size_raw is not None and str(size_raw).strip() else None
        except Exception:
            size_val = None
        obj = {
            "mode": "hosted_file",
            "ko_is_hosted": True,
            "ko_file_id": (doc.get("ko_file_id") or "").strip() if isinstance(doc.get("ko_file_id"), str) else None,
            "ko_object_mimetype": (doc.get("ko_object_mimetype") or "").strip().lower() if isinstance(doc.get("ko_object_mimetype"), str) else None,
            "ko_object_extension": (doc.get("ko_object_extension") or "").strip().lower() if isinstance(doc.get("ko_object_extension"), str) else None,
            "ko_object_size": size_val,
        }
    else:
        obj = {
            "mode": "external_url",
            "ko_is_hosted": False,
            "@id": (doc.get("@id") or "").strip() if isinstance(doc.get("@id"), str) else None,
            "resolved_url": (doc.get("resolved_url") or "").strip() if isinstance(doc.get("resolved_url"), str) else None,
        }
    payload = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _parse_checked_at(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)
    except Exception:
        return None


def _agri_gate_headers() -> Dict[str, str]:
    token = _agri_gate_api_token()
    headers = {"accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _decision_from_agri_gate(data: Dict[str, Any], *, fallback_scope: str, strict_error: bool) -> Dict[str, Any]:
    status = str((data or {}).get("status") or "error").strip().lower() or "error"
    scope = str((data or {}).get("scope") or fallback_scope).strip() or fallback_scope
    engine = str((data or {}).get("primary_engine") or "agri_gate").strip() or "agri_gate"
    reason = str((data or {}).get("reason_code") or "agri_gate_unknown").strip() or "agri_gate_unknown"
    checked_at = _parse_checked_at((data or {}).get("checked_at"))
    quarantined = bool((data or {}).get("quarantined"))
    escalated = bool((data or {}).get("escalation"))
    if status == "error" and strict_error:
        escalated = True
    return _security_decision(
        status=status,
        scope=scope,
        engine=engine,
        reason=reason,
        checked_at=checked_at,
        quarantined=quarantined,
        escalated=escalated,
    )


def _reuse_existing_security(existing: Dict[str, Any]) -> Dict[str, Any]:
    return _security_decision(
        status=str(existing.get("security_status") or "skipped"),
        scope=str(existing.get("security_scope") or "none"),
        engine=str(existing.get("security_engine") or "none"),
        reason=str(existing.get("security_reason") or "security_unknown"),
        checked_at=_parse_checked_at(existing.get("security_checked_at")),
        quarantined=bool(existing.get("security_quarantined")),
        escalated=bool(existing.get("security_escalated")),
    )


def _security_scan_stale(existing: Dict[str, Any]) -> bool:
    max_age_days = _security_rescan_max_age_days()
    if max_age_days <= 0:
        return False
    checked_at = _parse_checked_at(existing.get("security_checked_at"))
    if checked_at is None:
        return True
    if checked_at.tzinfo is not None:
        checked_at = checked_at.replace(tzinfo=None)
    age = datetime.utcnow() - checked_at
    return age.total_seconds() > (max_age_days * 86400)


def _scan_url_security(url: str) -> Dict[str, Any]:
    if not _agri_gate_enabled():
        return _security_decision(status="skipped", scope="url", engine="agri_gate", reason="agri_gate_disabled")
    sess = get_public_session(timeout=_agri_gate_timeout())
    endpoint = _agri_gate_base_url() + "/v1/scan/url"
    try:
        resp = sess.post(
            endpoint,
            headers={**_agri_gate_headers(), "content-type": "application/json"},
            json={"url": url},
            timeout=_agri_gate_timeout(),
        )
        resp.raise_for_status()
        data = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
        return _decision_from_agri_gate(data if isinstance(data, dict) else {}, fallback_scope="url", strict_error=_url_scan_strict())
    except Exception as e:
        logger.warning("[SecurityAgriGateUrlFail] url=%s err=%r", url, e)
        return _security_decision(
            status="error" if _url_scan_strict() else "skipped",
            scope="url",
            engine="agri_gate",
            reason="agri_gate_url_error",
            escalated=_url_scan_strict(),
        )


def _source_filename_for_scan(doc: Dict[str, Any], url: Optional[str], mimetype: Optional[str]) -> str:
    name = (doc.get("ko_object_name") or "").strip() if isinstance(doc.get("ko_object_name"), str) else ""
    if name:
        return name
    ext = (doc.get("ko_object_extension") or "").strip() if isinstance(doc.get("ko_object_extension"), str) else ""
    if ext and not ext.startswith("."):
        ext = "." + ext
    if isinstance(url, str) and url.strip():
        base = os.path.basename(url.split("?", 1)[0].rstrip("/"))
        if base:
            return base
    if ext:
        return "scan" + ext
    if mimetype == "application/pdf":
        return "scan.pdf"
    return "scan.bin"


def _scan_file_security(doc: Dict[str, Any]) -> Dict[str, Any]:
    if not _agri_gate_enabled():
        return _security_decision(status="skipped", scope="file", engine="agri_gate", reason="agri_gate_disabled")
    file_url = _source_url_from_doc(doc)
    source_kind = (doc.get("source_kind") or "").strip().lower() if isinstance(doc.get("source_kind"), str) else ""
    if not file_url:
        return _security_decision(status="skipped", scope="file", engine="none", reason="no_file_url")
    if source_kind not in {"hosted_file", "external_file"} and not bool(doc.get("ko_is_hosted")):
        return _security_decision(status="skipped", scope="file", engine="agri_gate", reason="not_file_resource")
    raw = _download_remote_bytes(file_url, timeout=max(_agri_gate_timeout(), _security_probe_timeout()))
    if raw is None:
        return _security_decision(
            status="error",
            scope="file",
            engine="agri_gate",
            reason="file_download_failed",
            escalated=_file_scan_strict(),
        )
    filename = _source_filename_for_scan(
        doc,
        file_url,
        (doc.get("ko_object_mimetype") or "").strip().lower() if isinstance(doc.get("ko_object_mimetype"), str) else None,
    )
    mimetype = (doc.get("ko_object_mimetype") or "application/octet-stream").strip() if isinstance(doc.get("ko_object_mimetype"), str) else "application/octet-stream"
    sess = get_public_session(timeout=_agri_gate_timeout())
    endpoint = _agri_gate_base_url() + "/v1/scan/file"
    try:
        resp = sess.post(
            endpoint,
            headers=_agri_gate_headers(),
            files={"file": (filename, raw, mimetype)},
            timeout=max(_agri_gate_timeout(), 300),
        )
        resp.raise_for_status()
        data = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
        return _decision_from_agri_gate(data if isinstance(data, dict) else {}, fallback_scope="file", strict_error=_file_scan_strict())
    except Exception as e:
        logger.warning("[SecurityAgriGateFileFail] llid=%s url=%s err=%r", _doc_llid(doc), file_url, e)
        return _security_decision(
            status="error" if _file_scan_strict() else "skipped",
            scope="file",
            engine="agri_gate",
            reason="agri_gate_file_error",
            escalated=_file_scan_strict(),
        )


def _classify_security(doc: Dict[str, Any], *, existing: Optional[Dict[str, Any]] = None) -> tuple[str, Dict[str, Any]]:
    security_fp = _compute_security_fp(doc)
    if existing and (existing.get("security_fp") or "") == security_fp and not _security_scan_stale(existing):
        return security_fp, _reuse_existing_security(existing)
    decisions: List[Dict[str, Any]] = []
    url_decision: Optional[Dict[str, Any]] = None
    file_decision: Optional[Dict[str, Any]] = None
    source_url = (doc.get("@id") or doc.get("ko_file_id") or "").strip() if isinstance(doc.get("@id") or doc.get("ko_file_id"), str) else ""
    if source_url:
        url_decision = _scan_url_security(source_url)
        decisions.append(url_decision)
    if bool(doc.get("ko_is_hosted")) and isinstance(doc.get("ko_file_id"), str) and doc.get("ko_file_id").strip():
        file_decision = _scan_file_security(doc)
        decisions.append(file_decision)
    if not decisions:
        return security_fp, _security_decision(status="skipped", scope="none", engine="none", reason="no_security_target")
    if any(d["status"] == "malicious" for d in decisions):
        chosen = next(d for d in decisions if d["status"] == "malicious")
    elif any(d["status"] == "error" and d.get("escalated") for d in decisions):
        chosen = next(d for d in decisions if d["status"] == "error" and d.get("escalated"))
    elif any(d["status"] == "error" for d in decisions):
        chosen = next(d for d in decisions if d["status"] == "error")
    elif file_decision and file_decision["status"] == "clean":
        chosen = file_decision
    elif url_decision and url_decision["status"] == "clean":
        chosen = url_decision
    elif any(d["status"] == "clean" for d in decisions):
        chosen = next(d for d in decisions if d["status"] == "clean")
    else:
        chosen = decisions[0]
    scope_parts = []
    for d in decisions:
        scope = d.get("scope")
        if scope and scope not in scope_parts and scope != "none":
            scope_parts.append(scope)
    chosen["scope"] = ",".join(scope_parts) if scope_parts else chosen.get("scope") or "none"
    return security_fp, chosen


def _compute_current_doc_fp(doc: Dict[str, Any]) -> str:
    payload = json.dumps(doc, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _storage_max_ko_content_flat_chars() -> int:
    return max(1, int(os.getenv("STORAGE_MAX_KO_CONTENT_FLAT_CHARS", os.getenv("PROCESSING_MAX_TEXT_CHARS", "500000"))))


def _storage_max_ko_content_flat_vision_chars() -> int:
    return max(1, int(os.getenv("STORAGE_MAX_KO_CONTENT_FLAT_VISION_CHARS", "200000")))


def _storage_max_ko_content_flat_summarised_chars() -> int:
    return max(1, int(os.getenv("STORAGE_MAX_KO_CONTENT_FLAT_SUMMARISED_CHARS", "120000")))


def _storage_max_description_llm_chars() -> int:
    return max(1, int(os.getenv("STORAGE_MAX_DESCRIPTION_LLM_CHARS", "16000")))


def _truncate_for_storage(value: Any, max_chars: int) -> Any:
    if not isinstance(value, str):
        return value
    if len(value) <= max_chars:
        return value
    suffix = "\n\n[TRUNCATED_FOR_MYSQL_STORAGE]"
    keep = max(0, max_chars - len(suffix))
    return value[:keep].rstrip() + suffix


def _sanitize_doc_for_storage(doc: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(doc)
    llid = _doc_llid(out) or "unknown"
    limits = {
        "ko_content_flat": _storage_max_ko_content_flat_chars(),
        "ko_content_flat_vision": _storage_max_ko_content_flat_vision_chars(),
        "ko_content_flat_summarised": _storage_max_ko_content_flat_summarised_chars(),
        "description_llm": _storage_max_description_llm_chars(),
    }
    for field, max_chars in limits.items():
        before = out.get(field)
        after = _truncate_for_storage(before, max_chars)
        if isinstance(before, str) and isinstance(after, str) and len(after) < len(before):
            logger.warning(
                "[MysqlStorageTruncate] id=%s field=%s chars=%s->%s",
                llid,
                field,
                len(before),
                len(after),
            )
        out[field] = after

    stats = out.get("ko_content_flat_summarised_stats")
    if isinstance(stats, dict):
        stats_out = dict(stats)
        before = stats_out.get("summary")
        after = _truncate_for_storage(before, _storage_max_ko_content_flat_summarised_chars())
        if isinstance(before, str) and isinstance(after, str) and len(after) < len(before):
            logger.warning(
                "[MysqlStorageTruncate] id=%s field=%s chars=%s->%s",
                llid,
                "ko_content_flat_summarised_stats.summary",
                len(before),
                len(after),
            )
            stats_out["summary"] = after
            stats_out["summary_char_count"] = len(after)
            stats_out["summary_word_count"] = len(after.split())
        out["ko_content_flat_summarised_stats"] = stats_out

    return out


def _download_remote_bytes(url: str, *, timeout: int) -> Optional[bytes]:
    sess = get_public_session(timeout=timeout)
    retries = max(1, int(os.getenv("PROCESSING_PROBE_RETRIES", "3")))
    base_sleep = float(os.getenv("PROCESSING_PROBE_BACKOFF_SEC", "1.0"))
    for attempt in range(1, retries + 1):
        try:
            resp = sess.get(url, allow_redirects=True)
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            logger.warning("[ProcessingProbeFetchFail] target=%s attempt=%s/%s err=%r", url, attempt, retries, e)
            if attempt < retries:
                import time
                time.sleep(base_sleep * attempt)
    return None


def _looks_like_office_doc(doc: Dict[str, Any]) -> bool:
    ext = ((doc.get("ko_object_extension") or "") if isinstance(doc.get("ko_object_extension"), str) else "").strip().lower()
    mimetype = ((doc.get("ko_object_mimetype") or "") if isinstance(doc.get("ko_object_mimetype"), str) else "").strip().lower()
    office_exts = {".doc", ".docx", ".ppt", ".pptx", ".xls", ".xlsx"}
    if ext in office_exts:
        return True
    office_mimes = (
        "application/msword",
        "application/vnd.ms-powerpoint",
        "application/vnd.ms-excel",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    return mimetype in office_mimes


def _looks_like_text_doc(doc: Dict[str, Any]) -> bool:
    ext = ((doc.get("ko_object_extension") or "") if isinstance(doc.get("ko_object_extension"), str) else "").strip().lower()
    mimetype = ((doc.get("ko_object_mimetype") or "") if isinstance(doc.get("ko_object_mimetype"), str) else "").strip().lower()
    return ext in {".txt", ".csv", ".tsv"} or mimetype in {"text/plain", "text/csv", "text/tab-separated-values"}


def _is_image_doc(doc: Dict[str, Any]) -> bool:
    mimetype = ((doc.get("ko_object_mimetype") or "") if isinstance(doc.get("ko_object_mimetype"), str) else "").strip().lower()
    ext = ((doc.get("ko_object_extension") or "") if isinstance(doc.get("ko_object_extension"), str) else "").strip().lower()
    return mimetype in {"image/jpeg", "image/png"} or ext in {".jpg", ".jpeg", ".png"}


def _is_media_doc(doc: Dict[str, Any]) -> bool:
    mimetype = ((doc.get("ko_object_mimetype") or "") if isinstance(doc.get("ko_object_mimetype"), str) else "").strip().lower()
    return mimetype.startswith("audio/") or mimetype.startswith("video/")


def _probe_image_dimensions(raw: bytes, *, timeout: int) -> Optional[tuple[int, int]]:
    try:
        with tempfile.TemporaryDirectory(prefix="processing_image_") as tmpdir:
            img_path = os.path.join(tmpdir, "input.bin")
            with open(img_path, "wb") as f:
                f.write(raw)
            cmd = ["/usr/bin/identify", "-format", "%w %h", img_path]
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=max(timeout, 10))
            if proc.returncode != 0:
                logger.warning("[ProcessingImageProbeFail] rc=%s stderr=%s", proc.returncode, (proc.stderr or "")[:300])
                return None
            parts = (proc.stdout or "").strip().split()
            if len(parts) != 2:
                return None
            return int(parts[0]), int(parts[1])
    except Exception as e:
        logger.warning("[ProcessingImageProbeFail] err=%r", e)
        return None


def _probe_office_pdf_pages(raw: bytes, *, extension: str, timeout: int) -> Optional[int]:
    ext = extension if extension.startswith(".") else f".{extension}"
    try:
        with tempfile.TemporaryDirectory(prefix="processing_office_") as tmpdir:
            src_path = os.path.join(tmpdir, f"input{ext}")
            out_dir = os.path.join(tmpdir, "out")
            os.makedirs(out_dir, exist_ok=True)
            with open(src_path, "wb") as f:
                f.write(raw)
            cmd = [
                "/usr/bin/libreoffice",
                "--headless",
                "--convert-to",
                "pdf",
                "--outdir",
                out_dir,
                src_path,
            ]
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=max(timeout, 60))
            if proc.returncode != 0:
                logger.warning("[ProcessingOfficeProbeFail] rc=%s stderr=%s", proc.returncode, (proc.stderr or "")[:500])
                return None
            pdf_candidates = [os.path.join(out_dir, name) for name in os.listdir(out_dir) if name.lower().endswith(".pdf")]
            if not pdf_candidates:
                return None
            with open(pdf_candidates[0], "rb") as f:
                pdf_bytes = f.read()
            return _pdf_page_count(pdf_bytes, timeout=timeout)
    except Exception as e:
        logger.warning("[ProcessingOfficeProbeFail] err=%r", e)
        return None


def _compute_pdf_page_count(doc: Dict[str, Any]) -> Optional[int]:
    if not bool(doc.get("ko_is_hosted")):
        return None
    mimetype = (doc.get("ko_object_mimetype") or "").strip().lower()
    if mimetype != "application/pdf":
        return None
    url = (doc.get("ko_file_id") or "").strip()
    if not url:
        return None
    try:
        pdf_bytes = _download_pdf_bytes(url, timeout=int(os.getenv("MYSQL_SYNC_PDFINFO_TIMEOUT", "30")))
        if pdf_bytes is None:
            return None
        if not pdf_bytes.startswith(b"%PDF-"):
            logger.warning(
                "[MysqlSyncPdfInfoSkip] llid=%s title=%r url=%s reason=not_a_real_pdf_header",
                _doc_llid(doc),
                doc.get("title"),
                url,
            )
            return None
        return _pdf_page_count(pdf_bytes, timeout=int(os.getenv("MYSQL_SYNC_PDFINFO_TIMEOUT", "30")))
    except Exception as e:
        logger.warning(
            "[MysqlSyncPdfInfoFail] llid=%s title=%r url=%s err=%r",
            _doc_llid(doc),
            doc.get("title"),
            url,
            e,
        )
        return None


def _classify_processing_eligibility(doc: Dict[str, Any], *, pdf_page_count: Optional[int]) -> tuple[int, Optional[str]]:
    size_raw = doc.get("ko_object_size")
    try:
        size_val = int(size_raw) if size_raw is not None else None
    except Exception:
        size_val = None
    if isinstance(size_val, int) and size_val > _processing_max_file_size_bytes():
        return 0, "file_too_large"

    if pdf_page_count is not None and pdf_page_count > _processing_max_pdf_pages():
        return 0, "pdf_page_limit"

    if _looks_like_text_doc(doc):
        if isinstance(size_val, int) and size_val > _processing_max_text_bytes():
            return 0, "text_file_too_large"
        content = doc.get("ko_content_flat")
        if isinstance(content, str) and len(content) > _processing_max_text_chars():
            return 0, "text_content_too_long"

    probe_url = (doc.get("ko_file_id") or "").strip() if isinstance(doc.get("ko_file_id"), str) else ""
    probe_timeout = int(os.getenv("PROCESSING_PROBE_TIMEOUT", "30"))

    if _looks_like_office_doc(doc) and probe_url:
        raw = _download_remote_bytes(probe_url, timeout=probe_timeout)
        if raw is not None:
            office_pages = _probe_office_pdf_pages(raw, extension=str(doc.get("ko_object_extension") or ""), timeout=probe_timeout)
            if office_pages is not None and office_pages > _processing_max_office_pages():
                return 0, "office_page_limit"

    if _is_image_doc(doc) and probe_url:
        raw = _download_remote_bytes(probe_url, timeout=probe_timeout)
        if raw is not None:
            dims = _probe_image_dimensions(raw, timeout=probe_timeout)
            if dims is not None:
                width, height = dims
                if max(width, height) > _processing_max_image_dim():
                    return 0, "image_dimension_limit"

    if _is_media_doc(doc) and probe_url:
        from stages.enricher.utils import probe_media_duration_seconds
        duration = probe_media_duration_seconds(probe_url, timeout=probe_timeout)
        if duration is not None and duration > _processing_max_media_duration_sec():
            return 0, "media_duration_limit"

    return 1, None


def _classify_deferred(doc: Dict[str, Any], *, existing: Optional[Dict[str, Any]] = None) -> tuple[int, Optional[int], Optional[str], int, Optional[str], str, Dict[str, Any]]:
    page_count = _compute_pdf_page_count(doc)
    security_fp, security = _classify_security(doc, existing=existing)
    processing_eligible, processing_ineligible_reason = _classify_processing_eligibility(doc, pdf_page_count=page_count)
    if security["status"] == "malicious":
        processing_eligible, processing_ineligible_reason = 0, f"security_{security['reason']}"
    elif security["status"] == "error" and bool(security.get("escalated")):
        processing_eligible, processing_ineligible_reason = 0, f"security_{security['reason']}"
    if page_count is None:
        return 0, None, None, processing_eligible, processing_ineligible_reason, security_fp, security
    max_pages = _fast_pdf_max_pages()
    if page_count > max_pages:
        return 1, page_count, "pdf_over_fast_limit", processing_eligible, processing_ineligible_reason, security_fp, security
    return 0, page_count, None, processing_eligible, processing_ineligible_reason, security_fp, security


def _archive_current_row(cur, row: Dict[str, Any], *, history_kind: str, archived_at: datetime) -> None:
    cur.execute(
        """
        INSERT INTO ko_records_history (
          llid, env_mode, history_kind, source_doc_json, current_doc_json,
          source_fp, current_fp, source_url, source_mimetype, source_kind, is_deferred,
          pdf_page_count, deferred_reason, processing_eligible, processing_ineligible_reason,
          security_fp, security_status, security_scope, security_engine, security_checked_at, security_quarantined, security_escalated, security_reason,
          sync_status, fast_pipeline_status,
          deferred_pipeline_status, synced_at, enriched_at, improved_at,
          background_completed_at, updated_at, archived_at
        ) VALUES (
          %s, %s, %s, %s, %s,
          %s, %s, %s, %s, %s, %s,
          %s, %s, %s, %s,
          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
          %s, %s, %s, %s,
          %s, %s, %s
        )
        """,
        (
            row.get("llid"),
            row.get("env_mode"),
            history_kind,
            row.get("source_doc_json"),
            row.get("current_doc_json"),
            row.get("source_fp"),
            row.get("current_fp"),
            row.get("source_url"),
            row.get("source_mimetype"),
            row.get("source_kind"),
            int(bool(row.get("is_deferred"))),
            row.get("pdf_page_count"),
            row.get("deferred_reason"),
            int(bool(row.get("processing_eligible", 1))),
            row.get("processing_ineligible_reason"),
            row.get("security_fp"),
            row.get("security_status"),
            row.get("security_scope"),
            row.get("security_engine"),
            row.get("security_checked_at"),
            int(bool(row.get("security_quarantined", 0))),
            int(bool(row.get("security_escalated", 0))),
            row.get("security_reason"),
            row.get("sync_status"),
            row.get("fast_pipeline_status"),
            row.get("deferred_pipeline_status"),
            row.get("synced_at"),
            row.get("enriched_at"),
            row.get("improved_at"),
            row.get("background_completed_at"),
            row.get("updated_at"),
            archived_at,
        ),
    )


def upsert_source_docs(*, env_mode: str, docs: List[Dict[str, Any]]) -> Dict[str, int]:
    ensure_schema()
    synced = 0
    deferred = 0
    unchanged = 0
    changed = 0
    now = datetime.utcnow()

    with _connect() as conn:
        with conn.cursor() as cur:
            for doc in docs:
                normalized_doc, source_url, source_mimetype, source_kind = _normalize_source_metadata(doc)
                llid = _doc_llid(normalized_doc)
                if not llid:
                    continue
                source_fp = compute_source_fp(normalized_doc)
                cur.execute(
                    """
                    SELECT * FROM ko_records
                    WHERE llid = %s AND env_mode = %s
                    """,
                    (llid, env_mode.upper()),
                )
                existing = cur.fetchone()
                is_deferred, pdf_page_count, deferred_reason, processing_eligible, processing_ineligible_reason, security_fp, security = _classify_deferred(normalized_doc, existing=existing)
                deferred += int(bool(is_deferred))
                if existing and existing.get("source_fp") == source_fp:
                    cur.execute(
                        """
                        UPDATE ko_records
                        SET sync_status = 'synced',
                            synced_at = %s,
                            is_deferred = %s,
                            pdf_page_count = %s,
                            deferred_reason = %s,
                            source_url = %s,
                            source_mimetype = %s,
                            source_kind = %s,
                            processing_eligible = %s,
                            processing_ineligible_reason = %s,
                            security_fp = %s,
                            security_status = %s,
                            security_scope = %s,
                            security_engine = %s,
                            security_checked_at = %s,
                            security_quarantined = %s,
                            security_escalated = %s,
                            security_reason = %s
                        WHERE llid = %s AND env_mode = %s
                        """,
                        (
                            now,
                            int(bool(is_deferred)),
                            pdf_page_count,
                            deferred_reason,
                            source_url,
                            source_mimetype,
                            source_kind,
                            int(bool(processing_eligible)),
                            processing_ineligible_reason,
                            security_fp,
                            security["status"],
                            security["scope"],
                            security["engine"],
                            security["checked_at"],
                            int(bool(security["quarantined"])),
                            int(bool(security["escalated"])),
                            security["reason"],
                            llid,
                            env_mode.upper(),
                        ),
                    )
                    unchanged += 1
                    synced += 1
                    continue

                if existing:
                    _archive_current_row(cur, existing, history_kind="source_update", archived_at=now)
                    changed += 1

                cur.execute(
                    """
                    INSERT INTO ko_records (
                      llid, env_mode, source_doc_json, current_doc_json, source_fp, source_url, source_mimetype,
                      source_kind,
                      is_deferred, pdf_page_count, deferred_reason, processing_eligible, processing_ineligible_reason,
                      security_fp, security_status, security_scope, security_engine, security_checked_at, security_quarantined, security_escalated, security_reason,
                      sync_status, updated_at, synced_at
                    ) VALUES (
                      %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s,
                      'synced', %s, %s
                    )
                    ON DUPLICATE KEY UPDATE
                      source_doc_json = VALUES(source_doc_json),
                      source_fp = VALUES(source_fp),
                      source_url = VALUES(source_url),
                      source_mimetype = VALUES(source_mimetype),
                      source_kind = VALUES(source_kind),
                      is_deferred = VALUES(is_deferred),
                      pdf_page_count = VALUES(pdf_page_count),
                      deferred_reason = VALUES(deferred_reason),
                      processing_eligible = VALUES(processing_eligible),
                      processing_ineligible_reason = VALUES(processing_ineligible_reason),
                      security_fp = VALUES(security_fp),
                      security_status = VALUES(security_status),
                      security_scope = VALUES(security_scope),
                      security_engine = VALUES(security_engine),
                      security_checked_at = VALUES(security_checked_at),
                      security_quarantined = VALUES(security_quarantined),
                      security_escalated = VALUES(security_escalated),
                      security_reason = VALUES(security_reason),
                      sync_status = 'synced',
                      updated_at = VALUES(updated_at),
                      synced_at = VALUES(synced_at)
                    """,
                    (
                        llid,
                        env_mode.upper(),
                        json.dumps(normalized_doc, ensure_ascii=False),
                        existing.get("current_doc_json") if existing else None,
                        source_fp,
                        source_url,
                        source_mimetype,
                        source_kind,
                        int(bool(is_deferred)),
                        pdf_page_count,
                        deferred_reason,
                        int(bool(processing_eligible)),
                        processing_ineligible_reason,
                        security_fp,
                        security["status"],
                        security["scope"],
                        security["engine"],
                        security["checked_at"],
                        int(bool(security["quarantined"])),
                        int(bool(security["escalated"])),
                        security["reason"],
                        now,
                        now,
                    ),
                )
                synced += 1
        conn.commit()

    return {"synced": synced, "deferred": deferred, "changed": changed, "unchanged": unchanged}


def fetch_source_docs(
    *,
    env_mode: str,
    deferred_only: Optional[bool] = None,
    llids: Optional[List[str]] = None,
    max_docs: Optional[int] = None,
) -> List[Dict[str, Any]]:
    ensure_schema()
    where = ["env_mode = %s"]
    params: List[Any] = [env_mode.upper()]

    if deferred_only is True:
        where.append("is_deferred = 1")
    elif deferred_only is False:
        where.append("is_deferred = 0")
    where.append("processing_eligible = 1")

    if llids:
        placeholders = ", ".join(["%s"] * len(llids))
        where.append(f"llid IN ({placeholders})")
        params.extend(llids)

    sql = (
        "SELECT source_doc_json FROM ko_records "
        f"WHERE {' AND '.join(where)} "
        "ORDER BY synced_at ASC"
    )
    if isinstance(max_docs, int) and max_docs > 0:
        sql += " LIMIT %s"
        params.append(max_docs)

    out: List[Dict[str, Any]] = []
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall() or []
            for row in rows:
                try:
                    doc = json.loads(row["source_doc_json"])
                    if isinstance(doc, dict):
                        out.append(doc)
                except Exception:
                    continue
    return out


def upsert_current_docs(*, env_mode: str, docs: List[Dict[str, Any]], background: bool, stage: str) -> Dict[str, int]:
    ensure_schema()
    if stage not in {"enriched", "improved"}:
        raise ValueError(f"Unsupported stage={stage!r}")
    updated = 0
    unchanged = 0
    now = datetime.utcnow()
    status_col = "deferred_pipeline_status" if background else "fast_pipeline_status"
    status_value = "success" if stage == "improved" else "enriched"

    with _connect() as conn:
        with conn.cursor() as cur:
            for doc in docs:
                safe_doc = _sanitize_doc_for_storage(doc)
                llid = _doc_llid(safe_doc)
                if not llid:
                    continue
                current_fp = _compute_current_doc_fp(safe_doc)
                current_doc_json = json.dumps(safe_doc, ensure_ascii=False)
                cur.execute(
                    """
                    SELECT * FROM ko_records
                    WHERE llid = %s AND env_mode = %s
                    """,
                    (llid, env_mode.upper()),
                )
                existing = cur.fetchone()
                if not existing:
                    continue
                if existing.get("current_fp") == current_fp:
                    cur.execute(
                        f"""
                        UPDATE ko_records
                        SET current_doc_json = %s,
                            {status_col} = %s,
                            enriched_at = %s,
                            improved_at = %s,
                            background_completed_at = %s,
                            updated_at = %s
                        WHERE llid = %s AND env_mode = %s
                        """,
                        (
                            current_doc_json,
                            status_value,
                            now if int(safe_doc.get("enriched") or 0) == 1 else existing.get("enriched_at"),
                            now if stage == "improved" and int(safe_doc.get("improved") or 0) == 1 else existing.get("improved_at"),
                            now if background and stage == "improved" else existing.get("background_completed_at"),
                            now,
                            llid,
                            env_mode.upper(),
                        ),
                    )
                    unchanged += 1
                    continue

                _archive_current_row(cur, existing, history_kind="processed_update", archived_at=now)
                cur.execute(
                    f"""
                    UPDATE ko_records
                    SET current_doc_json = %s,
                        current_fp = %s,
                        {status_col} = %s,
                        enriched_at = %s,
                        improved_at = %s,
                        background_completed_at = %s,
                        updated_at = %s
                    WHERE llid = %s AND env_mode = %s
                    """,
                    (
                        current_doc_json,
                        current_fp,
                        status_value,
                        now if int(safe_doc.get("enriched") or 0) == 1 else None,
                        now if stage == "improved" and int(safe_doc.get("improved") or 0) == 1 else existing.get("improved_at"),
                        now if background and stage == "improved" else existing.get("background_completed_at"),
                        now,
                        llid,
                        env_mode.upper(),
                    ),
                )
                updated += int(cur.rowcount > 0)
        conn.commit()

    return {"updated": updated, "unchanged": unchanged}


def upsert_processed_docs(*, env_mode: str, docs: List[Dict[str, Any]], background: bool) -> Dict[str, int]:
    return upsert_current_docs(env_mode=env_mode, docs=docs, background=background, stage="improved")


def fetch_record(*, env_mode: str, llid: str) -> Optional[Dict[str, Any]]:
    ensure_schema()
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT llid, env_mode, source_doc_json, current_doc_json, source_url, source_mimetype, source_kind, is_deferred,
                       pdf_page_count, deferred_reason, processing_eligible, processing_ineligible_reason,
                       security_status, security_scope, security_engine, security_checked_at, security_quarantined, security_escalated, security_reason,
                       sync_status, fast_pipeline_status,
                       deferred_pipeline_status, synced_at, enriched_at, improved_at,
                       background_completed_at, updated_at
                FROM ko_records
                WHERE llid = %s AND env_mode = %s
                """,
                (llid, env_mode.upper()),
            )
            row = cur.fetchone()
            if not row:
                return None
    for key in ("source_doc_json", "current_doc_json"):
        raw = row.get(key)
        if isinstance(raw, str):
            try:
                row[key[:-5]] = json.loads(raw)
            except Exception:
                row[key[:-5]] = None
        row.pop(key, None)
    return row


def export_docs(*, env_mode: str, processed_only: bool, eligible_only: bool) -> List[Dict[str, Any]]:
    ensure_schema()
    out: List[Dict[str, Any]] = []
    where = ["env_mode = %s"]
    params: List[Any] = [env_mode.upper()]
    if processed_only:
        where.append("current_doc_json IS NOT NULL")
    if eligible_only:
        where.append("processing_eligible = 1")
    sql = (
        "SELECT source_doc_json, current_doc_json FROM ko_records "
        f"WHERE {' AND '.join(where)} "
        "ORDER BY synced_at ASC"
    )
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall() or []
            for row in rows:
                raw = row.get("current_doc_json") or row.get("source_doc_json")
                if not isinstance(raw, str):
                    continue
                try:
                    doc = json.loads(raw)
                except Exception:
                    continue
                if isinstance(doc, dict):
                    out.append(doc)
    return out


def summarize_status(*, env_mode: str) -> Dict[str, Any]:
    ensure_schema()
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  COUNT(*) AS total_records,
                  SUM(CASE WHEN is_deferred = 1 THEN 1 ELSE 0 END) AS deferred_records,
                  SUM(CASE WHEN processing_eligible = 0 THEN 1 ELSE 0 END) AS ineligible_records,
                  SUM(CASE WHEN fast_pipeline_status = 'success' THEN 1 ELSE 0 END) AS fast_completed,
                  SUM(CASE WHEN deferred_pipeline_status = 'success' THEN 1 ELSE 0 END) AS deferred_completed
                FROM ko_records
                WHERE env_mode = %s
                """,
                (env_mode.upper(),),
            )
            row = cur.fetchone() or {}
    return row


def repair_source_metadata(*, env_mode: str, max_docs: Optional[int], llids: Optional[List[str]] = None) -> Dict[str, int]:
    ensure_schema()
    changed = 0
    unchanged = 0
    scanned = 0
    now = datetime.utcnow()

    where = ["env_mode = %s"]
    params: List[Any] = [env_mode.upper()]
    if llids:
        placeholders = ",".join(["%s"] * len(llids))
        where.append(f"llid IN ({placeholders})")
        params.extend(llids)

    sql = (
        "SELECT * FROM ko_records "
        f"WHERE {' AND '.join(where)} "
        "ORDER BY synced_at ASC"
    )
    if isinstance(max_docs, int) and max_docs > 0:
        sql += " LIMIT %s"
        params.append(max_docs)

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall() or []
            for row in rows:
                raw = row.get("source_doc_json")
                if not isinstance(raw, str):
                    continue
                try:
                    source_doc = json.loads(raw)
                except Exception:
                    continue
                if not isinstance(source_doc, dict):
                    continue
                scanned += 1
                normalized_doc, source_url, source_mimetype, source_kind = _normalize_source_metadata(source_doc)
                new_source_json = json.dumps(normalized_doc, ensure_ascii=False)
                new_source_fp = compute_source_fp(normalized_doc)
                if (
                    row.get("source_doc_json") == new_source_json
                    and (row.get("source_fp") or "") == new_source_fp
                    and (row.get("source_url") or None) == source_url
                    and (row.get("source_mimetype") or None) == source_mimetype
                    and (row.get("source_kind") or None) == source_kind
                ):
                    unchanged += 1
                    continue
                _archive_current_row(cur, row, history_kind="source_metadata_repair", archived_at=now)
                cur.execute(
                    """
                    UPDATE ko_records
                    SET source_doc_json = %s,
                        source_fp = %s,
                        source_url = %s,
                        source_mimetype = %s,
                        source_kind = %s,
                        updated_at = %s
                    WHERE llid = %s AND env_mode = %s
                    """,
                    (
                        new_source_json,
                        new_source_fp,
                        source_url,
                        source_mimetype,
                        source_kind,
                        now,
                        row.get("llid"),
                        env_mode.upper(),
                    ),
                )
                changed += int(cur.rowcount > 0)
        conn.commit()

    return {"scanned": scanned, "changed": changed, "unchanged": unchanged}


def repair_current_date_metadata(*, env_mode: str, max_docs: Optional[int], llids: Optional[List[str]] = None) -> Dict[str, int]:
    ensure_schema()
    scanned = 0
    changed = 0
    unchanged = 0
    skipped_no_current = 0
    now = datetime.utcnow()

    where = ["env_mode = %s"]
    params: List[Any] = [env_mode.upper()]
    if llids:
        placeholders = ",".join(["%s"] * len(llids))
        where.append(f"llid IN ({placeholders})")
        params.extend(llids)

    sql = (
        "SELECT * FROM ko_records "
        f"WHERE {' AND '.join(where)} "
        "ORDER BY synced_at ASC"
    )
    if isinstance(max_docs, int) and max_docs > 0:
        sql += " LIMIT %s"
        params.append(max_docs)

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall() or []
            for row in rows:
                source_raw = row.get("source_doc_json")
                current_raw = row.get("current_doc_json")
                if not isinstance(source_raw, str):
                    continue
                scanned += 1
                if not isinstance(current_raw, str) or not current_raw.strip():
                    skipped_no_current += 1
                    continue
                try:
                    source_doc = json.loads(source_raw)
                    current_doc = json.loads(current_raw)
                except Exception:
                    unchanged += 1
                    continue
                if not isinstance(source_doc, dict) or not isinstance(current_doc, dict):
                    unchanged += 1
                    continue

                src_date = source_doc.get("date_of_completion")
                src_date_source = source_doc.get("date_of_completion_source")
                cur_date = current_doc.get("date_of_completion")
                cur_date_source = current_doc.get("date_of_completion_source")

                if src_date == cur_date and src_date_source == cur_date_source:
                    unchanged += 1
                    continue

                patched = dict(current_doc)
                if src_date is None:
                    patched.pop("date_of_completion", None)
                else:
                    patched["date_of_completion"] = src_date

                if src_date_source is None:
                    patched.pop("date_of_completion_source", None)
                else:
                    patched["date_of_completion_source"] = src_date_source

                patched = _sanitize_doc_for_storage(patched)
                new_current_json = json.dumps(patched, ensure_ascii=False)
                new_current_fp = _compute_current_doc_fp(patched)

                _archive_current_row(cur, row, history_kind="current_date_metadata_repair", archived_at=now)
                cur.execute(
                    """
                    UPDATE ko_records
                    SET current_doc_json = %s,
                        current_fp = %s,
                        updated_at = %s
                    WHERE llid = %s AND env_mode = %s
                    """,
                    (
                        new_current_json,
                        new_current_fp,
                        now,
                        row.get("llid"),
                        env_mode.upper(),
                    ),
                )
                changed += int(cur.rowcount > 0)
        conn.commit()

    return {
        "scanned": scanned,
        "changed": changed,
        "unchanged": unchanged,
        "skipped_no_current": skipped_no_current,
    }
