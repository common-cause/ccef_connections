"""
Google Drive connector for CCEF connections library.

File-level Drive access: find/create folders, list a folder, upload binaries,
rename, and grant access. This is the connector for moving FILES into Drive.
Spreadsheet creation and cell writes belong to SheetsWriterConnector, which
drives the Sheets API and only touches Drive to place a spreadsheet.

Uses the same GOOGLE_SHEETS_CREDENTIALS_PASSWORD service account as the two
Sheets connectors, so a folder that SA can already write is immediately
usable here.

SHARED DRIVES: every call passes supportsAllDrives/includeItemsFromAllDrives.
A service account can hold a FOLDER-level grant on a shared drive without
being a member of the drive itself -- in that case Drive reports no shared
drive memberships while the folder is still fully writable. Do not infer
"no access" from an empty drives.list().

Examples:
    >>> drive = DriveConnector()
    >>> folder_id = drive.get_or_create_folder("OH", parent_id="1F9L_...")
    >>> info = drive.upload_file("/tmp/photo.jpg", "Incident 535.jpg", folder_id)
    >>> info["webViewLink"]
"""

import logging
import os
from typing import Any, Dict, List, Optional

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build as build_service
from googleapiclient.http import MediaFileUpload

from ..core.base import BaseConnection
from ..core.retry import retry_google_operation
from ..exceptions import ConnectionError, CredentialError

logger = logging.getLogger(__name__)

FOLDER_MIME = "application/vnd.google-apps.folder"


def _escape(value: str) -> str:
    """Escape a literal for a Drive query string."""
    return value.replace("\\", "\\\\").replace("'", "\\'")


class DriveConnector(BaseConnection):
    """Google Drive file connector with service account authentication."""

    SCOPES = ["https://www.googleapis.com/auth/drive"]

    def __init__(self) -> None:
        super().__init__()
        self._credentials: Optional[Credentials] = None

    # -- Lifecycle ----------------------------------------------------------

    def connect(self) -> None:
        """
        Establish a Drive v3 client using service account credentials.

        Raises:
            CredentialError: If credentials are missing or invalid
            ConnectionError: If the client cannot be built
        """
        try:
            creds_dict = self._credential_manager.get_google_sheets_credentials()
            self._credentials = Credentials.from_service_account_info(
                creds_dict, scopes=self.SCOPES
            )
            self._client = build_service(
                "drive", "v3", credentials=self._credentials, cache_discovery=False
            )
            self._is_connected = True
            logger.info("Connected to Google Drive")
        except CredentialError:
            raise
        except Exception as exc:
            raise ConnectionError(f"Failed to connect to Google Drive: {exc}") from exc

    def disconnect(self) -> None:
        """Release the Drive client."""
        self._client = None
        self._credentials = None
        self._is_connected = False

    def health_check(self) -> bool:
        """Return True if a trivial Drive call succeeds."""
        try:
            self._svc().files().list(
                pageSize=1,
                fields="files(id)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            ).execute()
            return True
        except Exception as exc:
            logger.warning(f"Drive health check failed: {exc}")
            return False

    def _svc(self) -> Any:
        if not self._is_connected or self._client is None:
            self.connect()
        if self._client is None:
            raise ConnectionError("Not connected to Google Drive")
        return self._client

    # -- Folders ------------------------------------------------------------

    @retry_google_operation
    def find_folder(self, name: str, parent_id: str) -> Optional[str]:
        """
        Find a folder by exact name inside a parent folder.

        Returns:
            The folder id, or None if no folder of that name is there.
        """
        query = (
            f"'{_escape(parent_id)}' in parents and name = '{_escape(name)}' "
            f"and mimeType = '{FOLDER_MIME}' and trashed = false"
        )
        hits = (
            self._svc()
            .files()
            .list(
                q=query,
                fields="files(id,name)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
            .get("files", [])
        )
        return hits[0]["id"] if hits else None

    def get_or_create_folder(self, name: str, parent_id: str) -> str:
        """
        Find a folder by name inside parent_id, creating it if absent.

        Args:
            name: Folder name, matched exactly
            parent_id: The folder to look in / create under

        Returns:
            The folder id.
        """
        existing = self.find_folder(name, parent_id)
        if existing:
            return existing
        created = (
            self._svc()
            .files()
            .create(
                body={"name": name, "mimeType": FOLDER_MIME, "parents": [parent_id]},
                fields="id",
                supportsAllDrives=True,
            )
            .execute()
        )
        logger.info(f"Created Drive folder {name!r} under {parent_id}")
        return created["id"]

    @retry_google_operation
    def list_folder(self, folder_id: str, fields: str = "id,name") -> List[Dict[str, Any]]:
        """
        List every non-trashed item directly inside a folder, following pages.

        Args:
            folder_id: The folder to list
            fields: Per-file fields to request (always includes id and name)

        Returns:
            A list of file resources.
        """
        out: List[Dict[str, Any]] = []
        token = None
        while True:
            resp = (
                self._svc()
                .files()
                .list(
                    q=f"'{_escape(folder_id)}' in parents and trashed = false",
                    fields=f"nextPageToken, files({fields})",
                    pageSize=1000,
                    pageToken=token,
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                )
                .execute()
            )
            out.extend(resp.get("files", []))
            token = resp.get("nextPageToken")
            if not token:
                return out

    # -- Files --------------------------------------------------------------

    @retry_google_operation
    def upload_file(
        self,
        local_path: str,
        name: str,
        folder_id: str,
        mime_type: Optional[str] = None,
        resumable: bool = True,
    ) -> Dict[str, Any]:
        """
        Upload a local file into a Drive folder.

        This always CREATES. Drive permits two files of the same name in one
        folder, so a caller that must not duplicate is responsible for
        checking first (see list_folder).

        Args:
            local_path: Path to the file on disk
            name: Name to give the file in Drive
            folder_id: Destination folder
            mime_type: Content type; guessed by Drive when None
            resumable: Use a resumable upload (right for anything large)

        Returns:
            {"id": ..., "name": ..., "webViewLink": ...}
        """
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Local file not found: {local_path}")
        media = MediaFileUpload(
            local_path,
            mimetype=mime_type or "application/octet-stream",
            resumable=resumable,
        )
        return (
            self._svc()
            .files()
            .create(
                body={"name": name, "parents": [folder_id]},
                media_body=media,
                fields="id,name,webViewLink",
                supportsAllDrives=True,
            )
            .execute()
        )

    @retry_google_operation
    def rename_file(self, file_id: str, new_name: str) -> Dict[str, Any]:
        """Rename a Drive file in place, keeping its id and link."""
        return (
            self._svc()
            .files()
            .update(
                fileId=file_id,
                body={"name": new_name},
                fields="id,name,webViewLink",
                supportsAllDrives=True,
            )
            .execute()
        )

    @retry_google_operation
    def get_file(self, file_id: str, fields: str = "id,name,webViewLink") -> Dict[str, Any]:
        """Fetch one file's metadata."""
        return (
            self._svc()
            .files()
            .get(fileId=file_id, fields=fields, supportsAllDrives=True)
            .execute()
        )

    @retry_google_operation
    def share_file(
        self, file_id: str, email: str, role: str = "reader", notify: bool = False
    ) -> None:
        """
        Grant a person access to a file or folder.

        Idempotent in effect: re-granting a role someone already holds is
        accepted by Drive. This never REVOKES -- removing access is left as a
        deliberate human action.

        Args:
            file_id: File or folder to share
            email: Address to grant
            role: 'reader' | 'commenter' | 'writer'
            notify: Send Google's notification email
        """
        self._svc().permissions().create(
            fileId=file_id,
            body={"type": "user", "role": role, "emailAddress": email},
            sendNotificationEmail=notify,
            supportsAllDrives=True,
        ).execute()
        logger.info(f"Granted {role} on {file_id} to {email}")