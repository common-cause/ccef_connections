"""Tests for the Google Drive file connector."""

from unittest.mock import MagicMock, patch

import pytest

from ccef_connections.connectors.drive import FOLDER_MIME, DriveConnector, _escape
from ccef_connections.exceptions import ConnectionError, CredentialError


# -- Fixtures ----------------------------------------------------------------


FAKE_SERVICE_ACCOUNT = {
    "type": "service_account",
    "project_id": "test-project",
    "private_key_id": "key123",
    "private_key": "-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----\n",
    "client_email": "test@test-project.iam.gserviceaccount.com",
    "client_id": "123456789",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
}

SAMPLE_FOLDER_ID = "0ABcDeFgHiJkLmNoPqRs"
SAMPLE_FILE_ID = "1aBcDeFgHiJkLmNoPqRsTuVwXyZ"


@pytest.fixture
def connector():
    """A DriveConnector with mocked credentials."""
    with patch.object(DriveConnector, "_credential_manager", create=True) as mock_cm:
        mock_cm.get_google_sheets_credentials.return_value = FAKE_SERVICE_ACCOUNT
        c = DriveConnector()
        c._credential_manager = mock_cm
        yield c


@pytest.fixture
def connected(connector):
    """A connector already 'connected' with a fake Drive client."""
    connector._client = MagicMock()
    connector._credentials = MagicMock()
    connector._is_connected = True
    return connector


def _files(connected):
    return connected._client.files.return_value


# -- Initialization ----------------------------------------------------------


class TestInit:
    def test_initial_state(self):
        c = DriveConnector()
        assert c._client is None
        assert c._credentials is None
        assert not c.is_connected()

    def test_scope_is_full_drive(self):
        # Uploading and renaming need write scope; drive.readonly is not enough.
        assert DriveConnector.SCOPES == ["https://www.googleapis.com/auth/drive"]


class TestConnect:
    def test_credential_error_propagates(self, connector):
        connector._credential_manager.get_google_sheets_credentials.side_effect = (
            CredentialError("missing")
        )
        with pytest.raises(CredentialError):
            connector.connect()
        assert not connector.is_connected()

    def test_other_failures_become_connection_error(self, connector):
        connector._credential_manager.get_google_sheets_credentials.side_effect = (
            ValueError("bad json")
        )
        with pytest.raises(ConnectionError):
            connector.connect()

    def test_disconnect_clears_state(self, connected):
        connected.disconnect()
        assert connected._client is None
        assert connected._credentials is None
        assert not connected.is_connected()


# -- Query escaping ----------------------------------------------------------


class TestEscape:
    def test_escapes_single_quote(self):
        # A folder named "St. Mary's" must not break out of the query literal.
        assert _escape("St. Mary's") == "St. Mary\\'s"

    def test_escapes_backslash(self):
        assert _escape("a\\b") == "a\\\\b"

    def test_leaves_plain_text_alone(self):
        assert _escape("OH") == "OH"


# -- Folders -----------------------------------------------------------------


class TestFindFolder:
    def test_returns_id_when_found(self, connected):
        _files(connected).list.return_value.execute.return_value = {
            "files": [{"id": SAMPLE_FOLDER_ID, "name": "OH"}]
        }
        assert connected.find_folder("OH", "parent") == SAMPLE_FOLDER_ID

    def test_returns_none_when_absent(self, connected):
        _files(connected).list.return_value.execute.return_value = {"files": []}
        assert connected.find_folder("OH", "parent") is None

    def test_query_is_scoped_and_typed(self, connected):
        _files(connected).list.return_value.execute.return_value = {"files": []}
        connected.find_folder("OH", "parent")
        q = _files(connected).list.call_args.kwargs["q"]
        assert "'parent' in parents" in q
        assert "name = 'OH'" in q
        assert FOLDER_MIME in q
        assert "trashed = false" in q

    def test_includes_shared_drives(self, connected):
        # A folder-level grant on a shared drive is invisible without these.
        _files(connected).list.return_value.execute.return_value = {"files": []}
        connected.find_folder("OH", "parent")
        kwargs = _files(connected).list.call_args.kwargs
        assert kwargs["supportsAllDrives"] is True
        assert kwargs["includeItemsFromAllDrives"] is True


class TestGetOrCreateFolder:
    def test_reuses_existing(self, connected):
        _files(connected).list.return_value.execute.return_value = {
            "files": [{"id": SAMPLE_FOLDER_ID, "name": "OH"}]
        }
        assert connected.get_or_create_folder("OH", "parent") == SAMPLE_FOLDER_ID
        _files(connected).create.assert_not_called()

    def test_creates_when_absent(self, connected):
        _files(connected).list.return_value.execute.return_value = {"files": []}
        _files(connected).create.return_value.execute.return_value = {"id": "new123"}
        assert connected.get_or_create_folder("OH", "parent") == "new123"
        body = _files(connected).create.call_args.kwargs["body"]
        assert body["name"] == "OH"
        assert body["mimeType"] == FOLDER_MIME
        assert body["parents"] == ["parent"]


class TestListFolder:
    def test_follows_pages(self, connected):
        _files(connected).list.return_value.execute.side_effect = [
            {"files": [{"id": "1", "name": "a"}], "nextPageToken": "tok"},
            {"files": [{"id": "2", "name": "b"}]},
        ]
        got = connected.list_folder(SAMPLE_FOLDER_ID)
        assert [f["name"] for f in got] == ["a", "b"]

    def test_empty_folder(self, connected):
        _files(connected).list.return_value.execute.return_value = {"files": []}
        assert connected.list_folder(SAMPLE_FOLDER_ID) == []

    def test_requested_fields_always_include_paging(self, connected):
        _files(connected).list.return_value.execute.return_value = {"files": []}
        connected.list_folder(SAMPLE_FOLDER_ID, fields="id,name,webViewLink")
        fields = _files(connected).list.call_args.kwargs["fields"]
        assert "nextPageToken" in fields
        assert "webViewLink" in fields


# -- Files -------------------------------------------------------------------


class TestUploadFile:
    def test_missing_local_file_raises_before_any_call(self, connected, tmp_path):
        with pytest.raises(FileNotFoundError):
            connected.upload_file(
                str(tmp_path / "nope.jpg"), "nope.jpg", SAMPLE_FOLDER_ID
            )
        _files(connected).create.assert_not_called()

    def test_uploads_into_folder(self, connected, tmp_path):
        local = tmp_path / "photo.jpg"
        local.write_bytes(b"jpegbytes")
        _files(connected).create.return_value.execute.return_value = {
            "id": SAMPLE_FILE_ID,
            "name": "photo.jpg",
            "webViewLink": "https://drive.google.com/file/d/x/view",
        }
        with patch("ccef_connections.connectors.drive.MediaFileUpload") as media:
            out = connected.upload_file(
                str(local), "photo.jpg", SAMPLE_FOLDER_ID, mime_type="image/jpeg"
            )
        assert out["id"] == SAMPLE_FILE_ID
        assert media.call_args.kwargs["mimetype"] == "image/jpeg"
        body = _files(connected).create.call_args.kwargs["body"]
        assert body == {"name": "photo.jpg", "parents": [SAMPLE_FOLDER_ID]}
        assert _files(connected).create.call_args.kwargs["supportsAllDrives"] is True

    def test_returns_link_field(self, connected, tmp_path):
        local = tmp_path / "p.jpg"
        local.write_bytes(b"x")
        _files(connected).create.return_value.execute.return_value = {"id": "i"}
        with patch("ccef_connections.connectors.drive.MediaFileUpload"):
            connected.upload_file(str(local), "p.jpg", SAMPLE_FOLDER_ID)
        assert "webViewLink" in _files(connected).create.call_args.kwargs["fields"]


class TestRenameFile:
    def test_renames_in_place(self, connected):
        _files(connected).update.return_value.execute.return_value = {
            "id": SAMPLE_FILE_ID,
            "name": "new.jpg",
        }
        out = connected.rename_file(SAMPLE_FILE_ID, "new.jpg")
        assert out["name"] == "new.jpg"
        kwargs = _files(connected).update.call_args.kwargs
        assert kwargs["fileId"] == SAMPLE_FILE_ID
        assert kwargs["body"] == {"name": "new.jpg"}
        # Renaming must not move or re-parent the file.
        assert "addParents" not in kwargs


class TestShareFile:
    def test_grants_role_without_notifying(self, connected):
        connected.share_file(SAMPLE_FILE_ID, "someone@example.org", role="writer")
        kwargs = connected._client.permissions.return_value.create.call_args.kwargs
        assert kwargs["body"] == {
            "type": "user",
            "role": "writer",
            "emailAddress": "someone@example.org",
        }
        assert kwargs["sendNotificationEmail"] is False

    def test_defaults_to_reader(self, connected):
        connected.share_file(SAMPLE_FILE_ID, "someone@example.org")
        kwargs = connected._client.permissions.return_value.create.call_args.kwargs
        assert kwargs["body"]["role"] == "reader"


# -- Health ------------------------------------------------------------------


class TestHealthCheck:
    def test_true_when_call_succeeds(self, connected):
        _files(connected).list.return_value.execute.return_value = {"files": []}
        assert connected.health_check() is True

    def test_false_when_call_fails(self, connected):
        _files(connected).list.return_value.execute.side_effect = Exception("boom")
        assert connected.health_check() is False
