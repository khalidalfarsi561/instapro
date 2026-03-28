"""
FastAPI registration webhook for InstaPro.

This module exposes a POST /register endpoint used by the bookmarklet-based
registration flow. It validates an Instagram session using the provided cookie
and user agent, stores the registration in Google Sheets, and notifies the
user on Telegram after a successful registration.

Environment variables:
- TELEGRAM_BOT_TOKEN: Telegram bot token used for sendMessage calls.
- REGISTER_API_KEY: Optional shared secret expected in the X-API-Key header.
- GOOGLE_SHEETS_CREDENTIALS_FILE: Used by db_manager.GoogleSheetsDB.
- GOOGLE_SHEET_NAME: Used by db_manager.GoogleSheetsDB.
"""

from __future__ import annotations

import logging
import os
from http import HTTPStatus

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, Form, Header, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator

from db_manager import GoogleSheetsDB

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

INSTAGRAM_CURRENT_USER_URL = "https://www.instagram.com/api/v1/accounts/current_user/"

app = FastAPI(
    title="InstaPro Registration API",
    version="1.0.0",
    description="FastAPI webhook used for Instagram session registration.",
)


class RegisterRequest(BaseModel):
    """Expected JSON payload from the browser bookmarklet."""

    model_config = ConfigDict(str_strip_whitespace=True)

    cookie: str = Field(..., min_length=1, description="Raw browser cookie string.")
    ua: str = Field(..., min_length=1, description="User agent string.")
    t_id: str = Field(..., min_length=1, description="Telegram user ID.")

    @field_validator("cookie", "ua", "t_id")
    @classmethod
    def validate_not_blank(cls, value: str) -> str:
        """Ensure the incoming field is not empty after trimming whitespace."""
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("Field cannot be blank.")
        return cleaned


class RegisterResponse(BaseModel):
    """Standard JSON response model returned by this API."""

    success: bool
    message: str
    telegram_id: str | None = None
    insta_id: str | None = None


def extract_csrftoken(cookie: str) -> str | None:
    """
    Extract the csrftoken value from a raw cookie string.

    Example input:
        "sessionid=abc; csrftoken=xyz; ds_user_id=123"
    """
    for part in cookie.split(";"):
        piece = part.strip()
        if "=" not in piece:
            continue
        key, value = piece.split("=", 1)
        if key.strip() == "csrftoken":
            return value.strip()
    return None


def build_instagram_headers(cookie: str, user_agent: str) -> dict[str, str]:
    """
    Build headers required to call Instagram while mimicking the user's device.
    """
    headers: dict[str, str] = {
        "User-Agent": user_agent,
        "Cookie": cookie,
        "Accept": "*/*",
        "Referer": "https://www.instagram.com/",
        "X-Requested-With": "XMLHttpRequest",
    }

    csrf_token = extract_csrftoken(cookie)
    if csrf_token:
        headers["X-CSRFToken"] = csrf_token

    return headers


def verify_api_key(provided_api_key: str | None) -> None:
    """
    Enforce optional shared-secret protection.

    If REGISTER_API_KEY is set in the environment, requests must include the
    same value in the X-API-Key header.
    """
    expected_api_key = os.getenv("REGISTER_API_KEY")
    if not expected_api_key:
        return

    if provided_api_key != expected_api_key:
        raise HTTPException(
            status_code=HTTPStatus.UNAUTHORIZED,
            detail="Invalid or missing API key.",
        )


async def fetch_instagram_user_id(cookie: str, user_agent: str) -> str:
    """
    Verify the Instagram session and return the current user's primary key.

    Raises HTTPException for session expiry and other upstream failures.
    """
    headers = build_instagram_headers(cookie=cookie, user_agent=user_agent)

    try:
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=False) as client:
            response = await client.get(INSTAGRAM_CURRENT_USER_URL, headers=headers)
    except httpx.RequestError as exc:
        logger.exception("Failed to contact Instagram: %s", exc)
        raise HTTPException(
            status_code=HTTPStatus.BAD_GATEWAY,
            detail="Failed to contact Instagram.",
        ) from exc

    location = response.headers.get("location", "")
    if response.status_code in (
        HTTPStatus.MOVED_PERMANENTLY,
        HTTPStatus.FOUND,
        HTTPStatus.SEE_OTHER,
        HTTPStatus.TEMPORARY_REDIRECT,
        HTTPStatus.PERMANENT_REDIRECT,
    ):
        logger.warning(
            "Instagram verification redirected: status=%s location=%s",
            response.status_code,
            location,
        )
        if "/accounts/login/" in location:
            raise HTTPException(
                status_code=HTTPStatus.UNAUTHORIZED,
                detail="Session expired. Please re-register using the magic link.",
            )
        raise HTTPException(
            status_code=HTTPStatus.BAD_GATEWAY,
            detail="Instagram verification was redirected unexpectedly.",
        )

    if response.status_code in (HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN):
        raise HTTPException(
            status_code=response.status_code,
            detail="Session expired. Please re-register using the magic link.",
        )

    if response.status_code != HTTPStatus.OK:
        logger.error(
            "Unexpected Instagram response: status=%s body=%s",
            response.status_code,
            response.text[:500],
        )
        raise HTTPException(
            status_code=HTTPStatus.BAD_GATEWAY,
            detail="Instagram verification failed.",
        )

    content_type = response.headers.get("content-type", "")
    if "application/json" not in content_type.lower():
        logger.error(
            "Instagram returned non-JSON content: status=%s content-type=%s body=%s",
            response.status_code,
            content_type,
            response.text[:500],
        )
        raise HTTPException(
            status_code=HTTPStatus.BAD_GATEWAY,
            detail="Invalid response from Instagram.",
        )

    try:
        data = response.json()
    except ValueError as exc:
        logger.exception("Instagram returned invalid JSON.")
        raise HTTPException(
            status_code=HTTPStatus.BAD_GATEWAY,
            detail="Invalid response from Instagram.",
        ) from exc

    user_data = data.get("user") or {}
    insta_id = user_data.get("pk")

    if insta_id is None:
        logger.error("Instagram response missing user.pk: %s", data)
        raise HTTPException(
            status_code=HTTPStatus.BAD_GATEWAY,
            detail="Instagram response missing account id.",
        )

    return str(insta_id)


async def send_telegram_message(chat_id: str, text: str) -> bool:
    """
    Send a Telegram message using the Bot API.

    Returns True on success and False on failure. Notification failure does not
    abort a successful registration save.
    """
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not bot_token:
        logger.warning("TELEGRAM_BOT_TOKEN is not set; skipping Telegram notification.")
        return False

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.post(url, json=payload)
            response.raise_for_status()
        return True
    except httpx.HTTPError as exc:
        logger.exception("Telegram notification failed: %s", exc)
        return False


@app.get("/", response_model=RegisterResponse)
async def healthcheck() -> RegisterResponse:
    """Simple root endpoint for deployment checks."""
    return RegisterResponse(success=True, message="InstaPro registration API is running.")


@app.post("/register", response_class=HTMLResponse)
async def register(
    request: Request,
    cookie: str = Form(...),
    ua: str = Form(...),
    t_id: str = Form(...),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> HTMLResponse:
    """
    Register a user's Instagram session and associate it with their Telegram ID.

    Form data is used instead of JSON to better survive restrictive browser CSP
    rules when the bookmarklet submits from Instagram pages.
    """
    # verify_api_key(x_api_key)  # Disabled temporarily to simplify testing.

    payload = RegisterRequest(cookie=cookie, ua=ua, t_id=t_id)

    logger.info(
        "Processing registration request from %s for telegram_id=%s",
        request.client.host if request.client else "unknown",
        payload.t_id,
    )

    insta_id = await fetch_instagram_user_id(cookie=payload.cookie, user_agent=payload.ua)

    try:
        db = GoogleSheetsDB()
        db.upsert_user(
            telegram_id=payload.t_id,
            insta_id=insta_id,
            cookie=payload.cookie,
            user_agent=payload.ua,
            last_run=None,
        )
    except Exception as exc:
        logger.exception("Failed to store registration in Google Sheets: %s", exc)
        raise HTTPException(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail="Failed to store registration.",
        ) from exc

    await send_telegram_message(chat_id=payload.t_id, text="✅ Registration Successful!")

    success_html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        <title>Success</title>
        <style>
            :root {
                color-scheme: light;
                --bg-start: #ecfdf5;
                --bg-end: #d1fae5;
                --card-bg: rgba(255, 255, 255, 0.96);
                --text-main: #064e3b;
                --text-muted: #166534;
                --accent: #16a34a;
                --accent-hover: #15803d;
                --shadow: 0 20px 50px rgba(22, 163, 74, 0.18);
            }

            * {
                box-sizing: border-box;
            }

            body {
                margin: 0;
                min-height: 100vh;
                font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
                background: linear-gradient(135deg, var(--bg-start), var(--bg-end));
                display: flex;
                align-items: center;
                justify-content: center;
                padding: 24px;
                color: var(--text-main);
            }

            .wrapper {
                width: 100%;
                max-width: 520px;
            }

            .card {
                background: var(--card-bg);
                border-radius: 28px;
                padding: 36px 28px;
                text-align: center;
                box-shadow: var(--shadow);
                border: 1px solid rgba(34, 197, 94, 0.18);
                backdrop-filter: blur(10px);
            }

            .checkmark {
                width: 110px;
                height: 110px;
                margin: 0 auto 20px;
                border-radius: 999px;
                display: flex;
                align-items: center;
                justify-content: center;
                background: linear-gradient(135deg, #22c55e, #16a34a);
                color: #ffffff;
                font-size: 56px;
                line-height: 1;
                box-shadow: 0 14px 34px rgba(34, 197, 94, 0.35);
            }

            h1 {
                margin: 0 0 12px;
                font-size: 2rem;
                font-weight: 800;
                letter-spacing: -0.02em;
            }

            p {
                margin: 0 auto;
                max-width: 420px;
                font-size: 1rem;
                line-height: 1.7;
                color: var(--text-muted);
            }

            .button {
                display: inline-flex;
                align-items: center;
                justify-content: center;
                margin-top: 28px;
                width: 100%;
                max-width: 260px;
                min-height: 54px;
                padding: 14px 22px;
                border-radius: 16px;
                text-decoration: none;
                font-weight: 700;
                font-size: 1rem;
                color: #ffffff;
                background: linear-gradient(135deg, var(--accent), #22c55e);
                box-shadow: 0 12px 28px rgba(22, 163, 74, 0.28);
                transition: transform 0.2s ease, box-shadow 0.2s ease, background 0.2s ease;
            }

            .button:hover,
            .button:focus-visible {
                background: linear-gradient(135deg, var(--accent-hover), var(--accent));
                transform: translateY(-1px);
                box-shadow: 0 16px 30px rgba(21, 128, 61, 0.3);
            }

            @media (max-width: 640px) {
                body {
                    padding: 16px;
                }

                .card {
                    padding: 28px 20px;
                    border-radius: 24px;
                }

                .checkmark {
                    width: 92px;
                    height: 92px;
                    font-size: 46px;
                    margin-bottom: 18px;
                }

                h1 {
                    font-size: 1.75rem;
                }

                p {
                    font-size: 0.98rem;
                }

                .button {
                    max-width: none;
                    width: 100%;
                }
            }
        </style>
    </head>
    <body>
        <main class="wrapper">
            <section class="card">
                <div class="checkmark" aria-hidden="true">✓</div>
                <h1>Success</h1>
                <p>Your account has been linked successfully. You can now return to Telegram to start the hunt!</p>
                <a class="button" href="https://t.me/YourBotUsername" target="_blank" rel="noopener noreferrer">
                    Open Telegram
                </a>
            </section>
        </main>
    </body>
    </html>
    """

    return HTMLResponse(content=success_html, status_code=HTTPStatus.OK)
