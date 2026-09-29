"""
ActBlue connector for CCEF connections library.

Read-only access to the ActBlue CSV API — the only programmatic export ActBlue
offers entities. It is asynchronous:

  1. ``POST /csvs`` with ``csv_type`` + a date range starts an export job and
     returns its ``id``. Every POST is a real job on ActBlue's side.
  2. ``GET /csvs/{id}`` returns ``status`` (``in_progress`` while running) and,
     when done, a ``download_url`` that is valid for about 10 minutes.
  3. The download is a plain CSV (one row per line item).

Authentication is HTTP Basic with the credential's ``uuid`` and ``secret``,
read from ``ACTBLUE_API_KEY_PASSWORD`` as ``{"uuid": ..., "secret": ...}``.
There is no whoami endpoint; ``GET /csvs/<random uuid>`` answering 404 "CSV
request not found" proves the key authenticates (401 = bad key).

Date ranges: ``date_range_start`` is inclusive, ``date_range_end`` exclusive,
both ``YYYY-MM-DD``.

The report types are the ones CC's BigQuery mirror (proj-tmc-mem-com
.actblue_table) is loaded from: paid_contributions, refunded_contributions,
managed_form_contributions, cancelled_recurring_contributions.
"""

import csv
import io
import logging
import time
from datetime import date
from typing import Any, Dict, List, Optional, Union

import requests

from ..core.base import BaseConnection
from ..core.retry import retry_actblue_operation
from ..exceptions import (
    AuthenticationError,
    ConnectionError,
    CredentialError,
    RateLimitError,
)

logger = logging.getLogger(__name__)

ACTBLUE_API_BASE = "https://secure.actblue.com/api/v1"

CSV_TYPES = (
    "paid_contributions",
    "refunded_contributions",
    "managed_form_contributions",
    "cancelled_recurring_contributions",
)

DateLike = Union[str, date]


def _iso(d: DateLike) -> str:
    return d.isoformat() if isinstance(d, date) else str(d)


class ActBlueConnector(BaseConnection):
    """
    ActBlue CSV API connector (read-only).

    Examples:
        >>> ab = ActBlueConnector()
        >>> rows = ab.fetch_csv("paid_contributions", "2026-08-22", "2026-08-23")
        >>> rows[0].keys()   # the CSV header, as ActBlue names it
    """

    def __init__(self) -> None:
        """Initialize the ActBlue connector."""
        super().__init__()
        self._auth: Optional[tuple] = None

    def connect(self) -> None:
        """
        Load the ActBlue credential. Makes no API call.

        Raises:
            CredentialError: If ``ACTBLUE_API_KEY_PASSWORD`` is missing or malformed
            ConnectionError: If credential loading fails for any other reason
        """
        try:
            creds = self._credential_manager.get_actblue_credentials()
            self._auth = (creds["uuid"], creds["secret"])
            self._is_connected = True
            logger.info("Successfully connected to ActBlue")
        except CredentialError:
            logger.error("Failed to connect to ActBlue: credentials missing")
            raise
        except Exception as e:
            logger.error(f"Failed to connect to ActBlue: {str(e)}")
            raise ConnectionError(f"Failed to connect to ActBlue: {str(e)}") from e

    def disconnect(self) -> None:
        """Clear the credential and connection state."""
        self._auth = None
        self._is_connected = False
        logger.debug("Disconnected from ActBlue")

    def health_check(self) -> bool:
        """
        Verify the credential authenticates, without starting an export:
        ``GET /csvs/<nil uuid>`` → 404 means authenticated.
        """
        if not self._is_connected or not self._auth:
            return False
        try:
            resp = requests.get(
                f"{ACTBLUE_API_BASE}/csvs/00000000-0000-0000-0000-000000000000",
                auth=self._auth, timeout=30,
            )
            return resp.status_code == 404
        except requests.RequestException:
            return False

    # ── Internal helpers ──────────────────────────────────────────────

    def _ensure_connected(self) -> None:
        if not self._is_connected or not self._auth:
            self.connect()

    def _request(self, method: str, path: str, json_body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        self._ensure_connected()
        try:
            resp = requests.request(
                method, f"{ACTBLUE_API_BASE}{path}", auth=self._auth,
                json=json_body, headers={"Accept": "application/json"}, timeout=60,
            )
        except requests.RequestException as e:
            raise ConnectionError(f"ActBlue request failed: {e}") from e

        if resp.status_code == 401:
            raise AuthenticationError(f"ActBlue authentication failed: {resp.text[:200]}")
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 5))
            raise RateLimitError(f"ActBlue rate limit exceeded, retry after {retry_after}s",
                                 retry_after=retry_after)
        if resp.status_code >= 400:
            raise ConnectionError(f"ActBlue API error {resp.status_code} on {path}: {resp.text[:300]}")
        try:
            return resp.json()
        except ValueError as e:
            raise ConnectionError(f"ActBlue returned non-JSON on {path}: {resp.text[:200]}") from e

    # ── Public API ────────────────────────────────────────────────────

    @retry_actblue_operation
    def request_csv(self, csv_type: str, date_range_start: DateLike, date_range_end: DateLike) -> str:
        """
        Start an export job. Returns the CSV request id.

        Args:
            csv_type: One of :data:`CSV_TYPES`
            date_range_start: Inclusive start date (``YYYY-MM-DD`` or ``date``)
            date_range_end: Exclusive end date
        """
        if csv_type not in CSV_TYPES:
            raise ValueError(f"csv_type must be one of {CSV_TYPES}, got {csv_type!r}")
        body = {"csv_type": csv_type, "date_range_start": _iso(date_range_start),
                "date_range_end": _iso(date_range_end)}
        result = self._request("POST", "/csvs", json_body=body)
        csv_id = result.get("id")
        if not csv_id:
            raise ConnectionError(f"ActBlue CSV request returned no id: {result}")
        return str(csv_id)

    @retry_actblue_operation
    def get_csv_status(self, csv_id: str) -> Dict[str, Any]:
        """Poll an export job: ``{"id", "status", "download_url"?}``."""
        return self._request("GET", f"/csvs/{csv_id}")

    def wait_for_csv(self, csv_id: str, timeout: int = 600, poll_seconds: int = 5) -> str:
        """Poll until the export is ready; return its download URL."""
        deadline = time.monotonic() + timeout
        while True:
            status = self.get_csv_status(csv_id)
            if status.get("download_url"):
                return status["download_url"]
            if status.get("status") not in (None, "in_progress", "pending", "queued"):
                raise ConnectionError(f"ActBlue CSV {csv_id} ended with status {status.get('status')!r}")
            if time.monotonic() > deadline:
                raise ConnectionError(f"ActBlue CSV {csv_id} not ready after {timeout}s")
            time.sleep(poll_seconds)

    def download_csv(self, download_url: str) -> List[Dict[str, str]]:
        """Download a finished export (URL valid ~10 min) and parse it into rows."""
        try:
            resp = requests.get(download_url, timeout=120)
        except requests.RequestException as e:
            raise ConnectionError(f"ActBlue CSV download failed: {e}") from e
        if resp.status_code >= 400:
            raise ConnectionError(f"ActBlue CSV download error {resp.status_code}: {resp.text[:200]}")
        text = resp.content.decode("utf-8-sig")
        return list(csv.DictReader(io.StringIO(text)))

    def fetch_csv(self, csv_type: str, date_range_start: DateLike, date_range_end: DateLike,
                  timeout: int = 600) -> List[Dict[str, str]]:
        """Request, wait for, and download one export. Returns its rows."""
        csv_id = self.request_csv(csv_type, date_range_start, date_range_end)
        logger.info(f"ActBlue CSV {csv_id} requested ({csv_type} {date_range_start}..{date_range_end})")
        return self.download_csv(self.wait_for_csv(csv_id, timeout=timeout))
