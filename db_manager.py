"""Google Sheets data access layer for the InstaPro project.

This module wraps Google Sheets access behind a small, explicit API so the bot
and web API can store and retrieve user registration data consistently.

Expected column order in the sheet:
- telegram_id
- insta_id
- cookie
- user_agent
- last_run
- whitelist
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import gspread
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials


# Load environment variables from a local .env file if one exists.
load_dotenv()

# Shared sheet schema. Do not rename or reorder these headers.
SHEET_HEADERS: list[str] = [
    "telegram_id",
    "insta_id",
    "cookie",
    "user_agent",
    "last_run",
    "whitelist",
]


@dataclass(slots=True)
class UserRecord:
    """Represents one user row stored in Google Sheets."""

    telegram_id: str
    insta_id: str
    cookie: str
    user_agent: str
    last_run: str = ""
    whitelist: str = ""


class GoogleSheetsDB:
    """Google Sheets-backed storage manager for user registration data."""

    def __init__(self) -> None:
        """Initialize the database connection and ensure sheet headers exist.

        Raises:
            ValueError: If required environment variables are missing.
            RuntimeError: If Google authorization or sheet access fails.
        """
        self.credentials_file = os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE", "").strip()
        self.sheet_name = os.getenv("GOOGLE_SHEET_NAME", "").strip()

        if not self.credentials_file:
            raise ValueError(
                "Missing required environment variable: GOOGLE_SHEETS_CREDENTIALS_FILE"
            )

        if not self.sheet_name:
            raise ValueError("Missing required environment variable: GOOGLE_SHEET_NAME")

        self.client = self._authorize()
        self.worksheet = self._open_sheet()
        self._ensure_headers()

    def _authorize(self) -> gspread.Client:
        """Authorize and return a Google Sheets client.

        Returns:
            An authorized gspread client.

        Raises:
            RuntimeError: If the credentials file is missing or invalid.
        """
        if not os.path.exists(self.credentials_file):
            raise RuntimeError(
                f"Google credentials file not found: {self.credentials_file}"
            )

        scopes = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]

        try:
            credentials = ServiceAccountCredentials.from_json_keyfile_name(
                self.credentials_file,
                scopes,
            )
            return gspread.authorize(credentials)
        except Exception as exc:
            raise RuntimeError(
                "Failed to authorize Google Sheets client. "
                "Check credentials file contents and sharing permissions."
            ) from exc

    def _open_sheet(self) -> gspread.Worksheet:
        """Open the configured spreadsheet and return the first worksheet.

        Returns:
            The first worksheet in the configured spreadsheet.

        Raises:
            RuntimeError: If the spreadsheet cannot be opened.
        """
        try:
            spreadsheet = self.client.open(self.sheet_name)
            return spreadsheet.sheet1
        except Exception as exc:
            raise RuntimeError(
                f"Failed to open Google Sheet '{self.sheet_name}'. "
                "Verify the sheet name and ensure the service account has access."
            ) from exc

    def _ensure_headers(self) -> None:
        """Ensure the first row contains the required headers.

        If the sheet is blank, the full required header row is created.
        If headers match the previous 5-column schema, the missing whitelist
        header is appended without disturbing existing data.
        If headers are otherwise incompatible, an error is raised.

        Raises:
            RuntimeError: If headers cannot be read/written or are invalid.
        """
        try:
            first_row = self.worksheet.row_values(1)
        except Exception as exc:
            raise RuntimeError("Failed to read header row from Google Sheets.") from exc

        if not first_row:
            try:
                self.worksheet.update("A1:F1", [SHEET_HEADERS])
            except Exception as exc:
                raise RuntimeError("Failed to write required headers to Google Sheets.") from exc
            return

        normalized_headers = [value.strip() for value in first_row]
        previous_headers = SHEET_HEADERS[:-1]

        if normalized_headers[: len(SHEET_HEADERS)] == SHEET_HEADERS:
            return

        if normalized_headers[: len(previous_headers)] == previous_headers:
            current_whitelist_header = (
                normalized_headers[len(previous_headers)]
                if len(normalized_headers) >= len(SHEET_HEADERS)
                else ""
            )
            if current_whitelist_header == SHEET_HEADERS[-1]:
                return
            try:
                self.worksheet.update_cell(1, len(SHEET_HEADERS), SHEET_HEADERS[-1])
            except Exception as exc:
                raise RuntimeError(
                    "Failed to append the whitelist header to Google Sheets."
                ) from exc
            return

        raise RuntimeError(
            "Google Sheet headers do not match the required schema. "
            f"Expected {SHEET_HEADERS}, found {first_row}."
        )

    def _pad_row(self, row: list[Any]) -> list[str]:
        """Pad a row to the current schema width without losing old data."""
        normalized_row = [str(value) for value in row]
        if len(normalized_row) < len(SHEET_HEADERS):
            normalized_row.extend([""] * (len(SHEET_HEADERS) - len(normalized_row)))
        return normalized_row[: len(SHEET_HEADERS)]

    def _get_all_records(self) -> list[dict[str, Any]]:
        """Return all sheet records mapped by the required headers.

        Returns:
            A list of row dictionaries.

        Raises:
            RuntimeError: If records cannot be read.
        """
        try:
            return self.worksheet.get_all_records(
                expected_headers=SHEET_HEADERS,
                head=1,
                default_blank="",
            )
        except Exception as exc:
            raise RuntimeError("Failed to fetch records from Google Sheets.") from exc

    def get_user_by_telegram_id(self, telegram_id: str | int) -> UserRecord | None:
        """Look up a user by telegram_id using a first-column row scan.

        Args:
            telegram_id: Telegram user ID.

        Returns:
            A UserRecord if found, otherwise None.

        Raises:
            RuntimeError: If the worksheet cannot be read.
        """
        target_id = str(telegram_id).strip()

        try:
            all_rows = self.worksheet.get_all_values()
        except Exception as exc:
            raise RuntimeError("Failed to read rows from Google Sheets.") from exc

        if len(all_rows) <= 1:
            return None

        for row in all_rows[1:]:
            padded_row = self._pad_row(row)
            if str(padded_row[0]).strip() == target_id:
                return UserRecord(
                    telegram_id=str(padded_row[0]).strip(),
                    insta_id=str(padded_row[1]).strip(),
                    cookie=str(padded_row[2]),
                    user_agent=str(padded_row[3]),
                    last_run=str(padded_row[4]).strip(),
                    whitelist=str(padded_row[5]).strip(),
                )

        return None

    def upsert_user(
        self,
        telegram_id: str | int,
        insta_id: str | int,
        cookie: str,
        user_agent: str,
        last_run: str | None = None,
        whitelist: str = "",
    ) -> UserRecord:
        """Insert or update a user row identified by telegram_id.

        Args:
            telegram_id: Telegram user ID.
            insta_id: Instagram user ID.
            cookie: Raw Instagram cookie string.
            user_agent: Stored Instagram mobile user-agent.
            last_run: Optional last execution timestamp.
            whitelist: Comma-separated Instagram account IDs to exclude.

        Returns:
            The inserted or updated UserRecord.

        Raises:
            RuntimeError: If read/write operations fail.
        """
        record = UserRecord(
            telegram_id=str(telegram_id).strip(),
            insta_id=str(insta_id).strip(),
            cookie=cookie.strip(),
            user_agent=user_agent.strip(),
            last_run=(last_run or "").strip(),
            whitelist=whitelist.strip(),
        )

        try:
            all_rows = self.worksheet.get_all_values()
        except Exception as exc:
            raise RuntimeError("Failed to read rows before upsert.") from exc

        target_row_index: int | None = None
        if len(all_rows) > 1:
            for index, row in enumerate(all_rows[1:], start=2):
                padded_row = self._pad_row(row)
                current_telegram_id = str(padded_row[0]).strip()
                if current_telegram_id == record.telegram_id:
                    target_row_index = index
                    break

        row_values = [
            record.telegram_id,
            record.insta_id,
            record.cookie,
            record.user_agent,
            record.last_run,
            record.whitelist,
        ]

        try:
            if target_row_index is not None:
                self.worksheet.update(f"A{target_row_index}:F{target_row_index}", [row_values])
            else:
                self.worksheet.append_row(row_values, value_input_option="RAW")
        except Exception as exc:
            raise RuntimeError("Failed to upsert user record in Google Sheets.") from exc

        return record

    def update_last_run(self, telegram_id: str | int, timestamp: str) -> None:
        """Update the last_run value for an existing user.

        Args:
            telegram_id: Telegram user ID.
            timestamp: Timestamp string to store.

        Raises:
            ValueError: If the user does not exist.
            RuntimeError: If the sheet cannot be read or updated.
        """
        target_id = str(telegram_id).strip()

        try:
            all_rows = self.worksheet.get_all_values()
        except Exception as exc:
            raise RuntimeError("Failed to read rows before updating last_run.") from exc

        target_row_index: int | None = None
        if len(all_rows) > 1:
            for index, row in enumerate(all_rows[1:], start=2):
                padded_row = self._pad_row(row)
                current_telegram_id = str(padded_row[0]).strip()
                if current_telegram_id == target_id:
                    target_row_index = index
                    break

        if target_row_index is None:
            raise ValueError(f"User with telegram_id '{target_id}' was not found.")

        try:
            self.worksheet.update_cell(target_row_index, 5, timestamp.strip())
        except Exception as exc:
            raise RuntimeError("Failed to update last_run in Google Sheets.") from exc

    def update_whitelist(self, telegram_id: str | int, whitelist: str) -> None:
        """Update the whitelist value for an existing user.

        Args:
            telegram_id: Telegram user ID.
            whitelist: Comma-separated Instagram account IDs to exclude.

        Raises:
            ValueError: If the user does not exist.
            RuntimeError: If the sheet cannot be read or updated.
        """
        target_id = str(telegram_id).strip()

        try:
            all_rows = self.worksheet.get_all_values()
        except Exception as exc:
            raise RuntimeError("Failed to read rows before updating whitelist.") from exc

        target_row_index: int | None = None
        if len(all_rows) > 1:
            for index, row in enumerate(all_rows[1:], start=2):
                padded_row = self._pad_row(row)
                current_telegram_id = str(padded_row[0]).strip()
                if current_telegram_id == target_id:
                    target_row_index = index
                    break

        if target_row_index is None:
            raise ValueError(f"User with telegram_id '{target_id}' was not found.")

        try:
            self.worksheet.update_cell(target_row_index, 6, whitelist.strip())
        except Exception as exc:
            raise RuntimeError("Failed to update whitelist in Google Sheets.") from exc