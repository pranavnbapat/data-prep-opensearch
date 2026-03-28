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
from stages.enricher.utils import get_public_session
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
                  is_deferred TINYINT(1) NOT NULL DEFAULT 0,
                  pdf_page_count INT NULL,
                  deferred_reason VARCHAR(64) NULL,
                  processing_eligible TINYINT(1) NOT NULL DEFAULT 1,
                  processing_ineligible_reason VARCHAR(64) NULL,
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
                  is_deferred TINYINT(1) NOT NULL DEFAULT 0,
                  pdf_page_count INT NULL,
                  deferred_reason VARCHAR(64) NULL,
                  processing_eligible TINYINT(1) NOT NULL DEFAULT 1,
                  processing_ineligible_reason VARCHAR(64) NULL,
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
            cur.execute(
                """
                ALTER TABLE ko_records
                  ADD COLUMN IF NOT EXISTS processing_eligible TINYINT(1) NOT NULL DEFAULT 1
                """
            )
            cur.execute(
                """
                ALTER TABLE ko_records
                  ADD COLUMN IF NOT EXISTS processing_ineligible_reason VARCHAR(64) NULL
                """
            )
            cur.execute(
                """
                ALTER TABLE ko_records_history
                  ADD COLUMN IF NOT EXISTS processing_eligible TINYINT(1) NOT NULL DEFAULT 1
                """
            )
            cur.execute(
                """
                ALTER TABLE ko_records_history
                  ADD COLUMN IF NOT EXISTS processing_ineligible_reason VARCHAR(64) NULL
                """
            )
        conn.commit()


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


def _compute_current_doc_fp(doc: Dict[str, Any]) -> str:
    payload = json.dumps(doc, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


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


def _classify_deferred(doc: Dict[str, Any]) -> tuple[int, Optional[int], Optional[str], int, Optional[str]]:
    page_count = _compute_pdf_page_count(doc)
    processing_eligible, processing_ineligible_reason = _classify_processing_eligibility(doc, pdf_page_count=page_count)
    if page_count is None:
        return 0, None, None, processing_eligible, processing_ineligible_reason
    max_pages = _fast_pdf_max_pages()
    if page_count > max_pages:
        return 1, page_count, "pdf_over_fast_limit", processing_eligible, processing_ineligible_reason
    return 0, page_count, None, processing_eligible, processing_ineligible_reason


def _archive_current_row(cur, row: Dict[str, Any], *, history_kind: str, archived_at: datetime) -> None:
    cur.execute(
        """
        INSERT INTO ko_records_history (
          llid, env_mode, history_kind, source_doc_json, current_doc_json,
          source_fp, current_fp, source_url, source_mimetype, is_deferred,
          pdf_page_count, deferred_reason, processing_eligible, processing_ineligible_reason, sync_status, fast_pipeline_status,
          deferred_pipeline_status, synced_at, enriched_at, improved_at,
          background_completed_at, updated_at, archived_at
        ) VALUES (
          %s, %s, %s, %s, %s,
          %s, %s, %s, %s, %s,
          %s, %s, %s, %s, %s, %s,
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
            int(bool(row.get("is_deferred"))),
            row.get("pdf_page_count"),
            row.get("deferred_reason"),
            int(bool(row.get("processing_eligible", 1))),
            row.get("processing_ineligible_reason"),
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
                llid = _doc_llid(doc)
                if not llid:
                    continue
                source_fp = compute_source_fp(doc)
                is_deferred, pdf_page_count, deferred_reason, processing_eligible, processing_ineligible_reason = _classify_deferred(doc)
                deferred += int(bool(is_deferred))
                cur.execute(
                    """
                    SELECT * FROM ko_records
                    WHERE llid = %s AND env_mode = %s
                    """,
                    (llid, env_mode.upper()),
                )
                existing = cur.fetchone()
                if existing and existing.get("source_fp") == source_fp:
                    cur.execute(
                        """
                        UPDATE ko_records
                        SET sync_status = 'synced',
                            synced_at = %s,
                            is_deferred = %s,
                            pdf_page_count = %s,
                            deferred_reason = %s,
                            processing_eligible = %s,
                            processing_ineligible_reason = %s
                        WHERE llid = %s AND env_mode = %s
                        """,
                        (
                            now,
                            int(bool(is_deferred)),
                            pdf_page_count,
                            deferred_reason,
                            int(bool(processing_eligible)),
                            processing_ineligible_reason,
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
                      is_deferred, pdf_page_count, deferred_reason, processing_eligible, processing_ineligible_reason,
                      sync_status, updated_at, synced_at
                    ) VALUES (
                      %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s,
                      'synced', %s, %s
                    )
                    ON DUPLICATE KEY UPDATE
                      source_doc_json = VALUES(source_doc_json),
                      source_fp = VALUES(source_fp),
                      source_url = VALUES(source_url),
                      source_mimetype = VALUES(source_mimetype),
                      is_deferred = VALUES(is_deferred),
                      pdf_page_count = VALUES(pdf_page_count),
                      deferred_reason = VALUES(deferred_reason),
                      processing_eligible = VALUES(processing_eligible),
                      processing_ineligible_reason = VALUES(processing_ineligible_reason),
                      sync_status = 'synced',
                      updated_at = VALUES(updated_at),
                      synced_at = VALUES(synced_at)
                    """,
                    (
                        llid,
                        env_mode.upper(),
                        json.dumps(doc, ensure_ascii=False),
                        existing.get("current_doc_json") if existing else None,
                        source_fp,
                        doc.get("@id") or doc.get("ko_file_id"),
                        doc.get("ko_object_mimetype"),
                        int(bool(is_deferred)),
                        pdf_page_count,
                        deferred_reason,
                        int(bool(processing_eligible)),
                        processing_ineligible_reason,
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
                llid = _doc_llid(doc)
                if not llid:
                    continue
                current_fp = _compute_current_doc_fp(doc)
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
                            json.dumps(doc, ensure_ascii=False),
                            status_value,
                            now if int(doc.get("enriched") or 0) == 1 else existing.get("enriched_at"),
                            now if stage == "improved" and int(doc.get("improved") or 0) == 1 else existing.get("improved_at"),
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
                        json.dumps(doc, ensure_ascii=False),
                        current_fp,
                        status_value,
                        now if int(doc.get("enriched") or 0) == 1 else None,
                        now if stage == "improved" and int(doc.get("improved") or 0) == 1 else existing.get("improved_at"),
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
                SELECT llid, env_mode, source_doc_json, current_doc_json, is_deferred,
                       pdf_page_count, deferred_reason, processing_eligible, processing_ineligible_reason, sync_status, fast_pipeline_status,
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


def export_docs(*, env_mode: str, processed_only: bool) -> List[Dict[str, Any]]:
    ensure_schema()
    out: List[Dict[str, Any]] = []
    where = ["env_mode = %s"]
    params: List[Any] = [env_mode.upper()]
    if processed_only:
        where.append("current_doc_json IS NOT NULL")
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
