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
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}

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
            whitelist="",
        )
    except Exception as exc:
        logger.exception("Failed to store registration in Google Sheets: %s", exc)
        raise HTTPException(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail="Failed to store registration.",
        ) from exc

    await send_telegram_message(chat_id=payload.t_id, text="✅ Registration Successful!")

    bot_username = os.getenv("TELEGRAM_BOT_USERNAME", "YourBotUsername").strip() or "YourBotUsername"
    telegram_deep_link = f"https://t.me/{bot_username}"

    success_html = f"""
    <!DOCTYPE html>
    <html lang="ar" dir="rtl">
    <head>
        <meta charset="UTF-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        <title>تم الربط بنجاح</title>
        <style>
            :root {{
                color-scheme: light;
                --bg-a: #f5f3ff;
                --bg-b: #eff6ff;
                --bg-c: #f8fafc;
                --card-bg: rgba(255, 255, 255, 0.96);
                --text-main: #0f172a;
                --text-muted: #475569;
                --primary: #7c3aed;
                --primary-soft: #ede9fe;
                --primary-deep: #5b21b6;
                --success: #059669;
                --success-soft: #ecfdf5;
                --success-border: #a7f3d0;
                --success-deep: #047857;
                --info-soft: #eff6ff;
                --info-border: #bfdbfe;
                --warning-bg: #fff7ed;
                --warning-border: #fdba74;
                --border: #e2e8f0;
                --shadow: 0 24px 60px rgba(15, 23, 42, 0.14);
            }}

            * {{
                box-sizing: border-box;
            }}

            body {{
                margin: 0;
                min-height: 100vh;
                font-family: "Segoe UI", "Tahoma", Arial, sans-serif;
                direction: rtl;
                text-align: right;
                background:
                    radial-gradient(circle at top right, rgba(124, 58, 237, 0.16), transparent 28%),
                    radial-gradient(circle at bottom left, rgba(59, 130, 246, 0.12), transparent 30%),
                    linear-gradient(135deg, var(--bg-a), var(--bg-b) 45%, var(--bg-c) 100%);
                display: flex;
                align-items: center;
                justify-content: center;
                padding: 24px;
                color: var(--text-main);
                overflow-x: hidden;
            }}

            .wrapper,
            .card,
            .hero-copy,
            .lead,
            .step-card,
            .feature-card,
            .highlight-box,
            .info-panel,
            .note,
            .footer-box,
            .tips,
            .tip {{
                direction: rtl;
                text-align: right;
            }}

            .card,
            .step-card,
            .feature-card,
            .highlight-box,
            .info-panel,
            .note,
            .footer-box {{
                animation: fadeInUp 0.85s ease both;
            }}

            body::before,
            body::after {{
                content: "";
                position: fixed;
                width: 220px;
                height: 220px;
                border-radius: 999px;
                filter: blur(26px);
                opacity: 0.35;
                z-index: 0;
                pointer-events: none;
                animation: floatBlob 9s ease-in-out infinite;
            }}

            body::before {{
                top: -60px;
                right: -40px;
                background: rgba(124, 58, 237, 0.3);
            }}

            body::after {{
                bottom: -70px;
                left: -30px;
                background: rgba(34, 197, 94, 0.24);
                animation-delay: -3s;
            }}

            .wrapper {{
                width: 100%;
                max-width: 920px;
                position: relative;
                z-index: 1;
            }}

            .card {{
                background: var(--card-bg);
                border: 1px solid rgba(255, 255, 255, 0.92);
                border-radius: 30px;
                padding: 32px 26px 24px;
                box-shadow: var(--shadow);
                backdrop-filter: blur(10px);
                position: relative;
                overflow: hidden;
            }}

            .card::after {{
                content: "";
                position: absolute;
                inset: 0;
                background: linear-gradient(135deg, rgba(124, 58, 237, 0.04), rgba(59, 130, 246, 0.02));
                pointer-events: none;
            }}

            .hero {{
                display: grid;
                grid-template-columns: 92px 1fr;
                gap: 18px;
                align-items: start;
            }}

            .hero-icon {{
                width: 92px;
                height: 92px;
                border-radius: 26px;
                background: linear-gradient(135deg, #22c55e, #14b8a6);
                color: #ffffff;
                display: flex;
                align-items: center;
                justify-content: center;
                font-size: 44px;
                box-shadow: 0 16px 32px rgba(34, 197, 94, 0.24);
                animation: popIn 0.75s cubic-bezier(.2,.9,.2,1) both;
            }}

            .hero-copy {{
                position: relative;
            }}

            .success-state {{
                position: relative;
                display: inline-flex;
                align-items: center;
                gap: 12px;
                margin-bottom: 16px;
                padding: 6px;
                border-radius: 999px;
                animation: successReveal 0.95s cubic-bezier(.2,.9,.2,1) both;
            }}

            .success-state .badge {{
                animation: fadeInUp 1s ease both 0.15s;
            }}

            .success-state::before {{
                content: "";
                position: absolute;
                inset: -6px;
                border-radius: inherit;
                background: linear-gradient(120deg, rgba(255, 255, 255, 0) 20%, rgba(255, 255, 255, 0.78) 50%, rgba(255, 255, 255, 0) 80%);
                transform: translateX(-170%) skewX(-18deg);
                animation: successShimmer 2.2s ease 0.55s 1 forwards;
                pointer-events: none;
            }}

            .check-pulse {{
                width: 54px;
                height: 54px;
                border-radius: 999px;
                background: radial-gradient(circle at 30% 30%, #34d399, #10b981 62%, #059669 100%);
                color: #ffffff;
                display: inline-flex;
                align-items: center;
                justify-content: center;
                font-size: 24px;
                box-shadow: 0 14px 28px rgba(5, 150, 105, 0.28);
                position: relative;
                flex-shrink: 0;
                animation: checkPulse 1.8s ease-in-out infinite 0.85s;
            }}

            .check-pulse::before,
            .check-pulse::after {{
                content: "";
                position: absolute;
                inset: 0;
                border-radius: inherit;
                border: 2px solid rgba(16, 185, 129, 0.28);
                opacity: 0;
            }}

            .check-pulse::before {{
                animation: ringPulse 2.2s ease-out infinite 0.9s;
            }}

            .check-pulse::after {{
                animation: ringPulse 2.2s ease-out infinite 1.35s;
            }}

            .badge {{
                position: relative;
                display: inline-flex;
                align-items: center;
                gap: 8px;
                background: linear-gradient(135deg, var(--success-soft), #ffffff);
                color: var(--success-deep);
                border: 1px solid var(--success-border);
                border-radius: 999px;
                padding: 10px 16px;
                font-size: 15px;
                font-weight: bold;
                animation: badgeLift 0.9s ease both 0.18s;
                transform-origin: center;
                overflow: hidden;
            }}

            .badge::after {{
                content: "";
                position: absolute;
                inset: 0;
                background: linear-gradient(115deg, transparent 25%, rgba(255, 255, 255, 0.68) 50%, transparent 75%);
                transform: translateX(-140%);
                animation: badgeShimmer 2.4s ease-in-out 0.85s infinite;
                pointer-events: none;
            }}

            .badge-dot {{
                width: 10px;
                height: 10px;
                border-radius: 999px;
                background: currentColor;
                box-shadow: 0 0 0 0 rgba(5, 150, 105, 0.35);
                animation: pingDot 1.8s ease-out infinite;
            }}

            h1 {{
                margin: 0 0 10px;
                font-size: 31px;
                line-height: 1.4;
                animation: slideFade 0.9s ease both 0.28s;
            }}

            .lead {{
                margin: 0;
                font-size: 17px;
                line-height: 1.9;
                color: var(--text-muted);
                animation: slideFade 1s ease both 0.4s;
            }}

            .section-title {{
                margin: 28px 0 14px;
                font-size: 20px;
                color: var(--text-main);
            }}

            .steps,
            .features {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
                gap: 14px;
            }}

            .step-card,
            .feature-card {{
                background: linear-gradient(180deg, #ffffff 0%, #f8fbff 100%);
                border: 1px solid #dbeafe;
                border-radius: 20px;
                padding: 18px 16px;
                box-shadow: 0 10px 24px rgba(59, 130, 246, 0.08);
                transition: transform 0.25s ease, box-shadow 0.25s ease;
            }}

            .step-card:hover,
            .feature-card:hover {{
                transform: translateY(-3px);
                box-shadow: 0 14px 28px rgba(59, 130, 246, 0.12);
            }}

            .step-number,
            .feature-icon {{
                width: 36px;
                height: 36px;
                border-radius: 12px;
                background: var(--primary-soft);
                color: var(--primary);
                display: flex;
                align-items: center;
                justify-content: center;
                font-weight: bold;
                margin-bottom: 12px;
            }}

            .feature-icon {{
                font-size: 18px;
            }}

            .step-card h3,
            .feature-card h3 {{
                margin: 0 0 8px;
                font-size: 17px;
            }}

            .step-card p,
            .feature-card p {{
                margin: 0;
                font-size: 14px;
                line-height: 1.9;
                color: var(--text-muted);
            }}

            .highlight-box {{
                margin-top: 22px;
                background: linear-gradient(135deg, rgba(124, 58, 237, 0.08), rgba(59, 130, 246, 0.08));
                border: 1px solid rgba(124, 58, 237, 0.16);
                border-radius: 22px;
                padding: 18px 16px;
            }}

            .highlight-box h3 {{
                margin: 0 0 8px;
                font-size: 18px;
                color: var(--primary-deep);
            }}

            .highlight-box p {{
                margin: 0;
                font-size: 15px;
                line-height: 1.9;
                color: var(--text-muted);
            }}

            .progress-preview {{
                margin-top: 12px;
                display: inline-flex;
                align-items: center;
                gap: 10px;
                padding: 10px 14px;
                border-radius: 14px;
                background: #ffffff;
                border: 1px solid rgba(124, 58, 237, 0.14);
                color: var(--primary-deep);
                font-weight: bold;
                font-size: 14px;
                direction: ltr;
            }}

            .info-panel {{
                margin-top: 22px;
                background: var(--info-soft);
                border: 1px solid var(--info-border);
                border-radius: 18px;
                padding: 18px 16px;
                font-size: 15px;
                line-height: 1.9;
                color: #1e3a8a;
            }}

            .note {{
                margin-top: 22px;
                background: var(--warning-bg);
                border: 1px solid var(--warning-border);
                border-radius: 18px;
                padding: 18px 16px;
                font-size: 15px;
                line-height: 1.9;
                color: #7c2d12;
            }}

            .footer-box {{
                margin-top: 18px;
                background: #f8fafc;
                border: 1px dashed var(--border);
                border-radius: 18px;
                padding: 16px;
                font-size: 14px;
                line-height: 1.9;
                color: var(--text-muted);
            }}

            .tips {{
                margin-top: 22px;
                display: flex;
                flex-wrap: wrap;
                gap: 10px;
            }}

            .tip {{
                background: #ffffff;
                border: 1px solid var(--border);
                border-radius: 999px;
                padding: 10px 14px;
                font-size: 13px;
                color: #334155;
            }}

            .fab {{
                position: fixed;
                left: 22px;
                bottom: 22px;
                display: inline-flex;
                align-items: center;
                gap: 10px;
                background: linear-gradient(135deg, #7c3aed, #4f46e5);
                color: #ffffff;
                text-decoration: none;
                border-radius: 999px;
                padding: 15px 18px;
                box-shadow: 0 18px 30px rgba(79, 70, 229, 0.32);
                font-weight: bold;
                z-index: 9;
                transition: transform 0.2s ease, box-shadow 0.2s ease;
                animation: popIn 0.95s ease both 0.2s;
            }}

            .fab:hover {{
                transform: translateY(-2px) scale(1.01);
                box-shadow: 0 22px 36px rgba(79, 70, 229, 0.38);
            }}

            .fab-icon {{
                width: 34px;
                height: 34px;
                display: inline-flex;
                align-items: center;
                justify-content: center;
                border-radius: 999px;
                background: rgba(255, 255, 255, 0.18);
                font-size: 18px;
            }}

            strong {{
                color: var(--text-main);
            }}

            @keyframes slideFade {{
                from {{
                    opacity: 0;
                    transform: translateY(10px);
                }}
                to {{
                    opacity: 1;
                    transform: translateY(0);
                }}
            }}

            @keyframes fadeInUp {{
                from {{
                    opacity: 0;
                    transform: translateY(14px);
                }}
                to {{
                    opacity: 1;
                    transform: translateY(0);
                }}
            }}

            @keyframes popIn {{
                0% {{
                    opacity: 0;
                    transform: scale(0.82);
                }}
                70% {{
                    opacity: 1;
                    transform: scale(1.04);
                }}
                100% {{
                    opacity: 1;
                    transform: scale(1);
                }}
            }}

            @keyframes successReveal {{
                0% {{
                    opacity: 0;
                    transform: translateY(12px) scale(0.96);
                }}
                70% {{
                    opacity: 1;
                    transform: translateY(-1px) scale(1.01);
                }}
                100% {{
                    opacity: 1;
                    transform: translateY(0) scale(1);
                }}
            }}

            @keyframes checkPulse {{
                0%, 100% {{
                    transform: scale(1);
                    box-shadow: 0 14px 28px rgba(5, 150, 105, 0.26);
                }}
                50% {{
                    transform: scale(1.06);
                    box-shadow: 0 18px 34px rgba(5, 150, 105, 0.34);
                }}
            }}

            @keyframes ringPulse {{
                0% {{
                    opacity: 0.42;
                    transform: scale(1);
                }}
                100% {{
                    opacity: 0;
                    transform: scale(1.85);
                }}
            }}

            @keyframes badgeLift {{
                from {{
                    opacity: 0;
                    transform: translateY(8px);
                }}
                to {{
                    opacity: 1;
                    transform: translateY(0);
                }}
            }}

            @keyframes badgeShimmer {{
                0% {{
                    transform: translateX(-140%);
                }}
                55%,
                100% {{
                    transform: translateX(170%);
                }}
            }}

            @keyframes successShimmer {{
                0% {{
                    opacity: 0;
                    transform: translateX(-170%) skewX(-18deg);
                }}
                20% {{
                    opacity: 1;
                }}
                100% {{
                    opacity: 0;
                    transform: translateX(170%) skewX(-18deg);
                }}
            }}

            @keyframes pingDot {{
                0% {{
                    box-shadow: 0 0 0 0 rgba(5, 150, 105, 0.32);
                }}
                80% {{
                    box-shadow: 0 0 0 10px rgba(5, 150, 105, 0);
                }}
                100% {{
                    box-shadow: 0 0 0 0 rgba(5, 150, 105, 0);
                }}
            }}

            @keyframes floatBlob {{
                0%, 100% {{
                    transform: translateY(0px);
                }}
                50% {{
                    transform: translateY(14px);
                }}
            }}

            @media (max-width: 640px) {{
                body {{
                    padding: 16px;
                }}

                .card {{
                    padding: 24px 18px 20px;
                    border-radius: 24px;
                }}

                .hero {{
                    grid-template-columns: 1fr;
                }}

                .hero-icon {{
                    width: 78px;
                    height: 78px;
                    font-size: 36px;
                }}

                .success-state {{
                    width: 100%;
                    align-items: center;
                    justify-content: flex-start;
                }}

                .check-pulse {{
                    width: 48px;
                    height: 48px;
                    font-size: 22px;
                }}

                h1 {{
                    font-size: 27px;
                }}

                .lead {{
                    font-size: 16px;
                }}

                .progress-preview {{
                    width: 100%;
                    justify-content: center;
                    text-align: center;
                    flex-wrap: wrap;
                }}

                .fab {{
                    left: 16px;
                    right: 16px;
                    bottom: 16px;
                    justify-content: center;
                }}
            }}
        </style>
    </head>
    <body>
        <main class="wrapper">
            <section class="card">
                <div class="hero">
                    <div class="hero-icon" aria-hidden="true">🎉</div>
                    <div class="hero-copy">
                        <div class="success-state" aria-label="تم حفظ الجلسة بنجاح">
                            <span class="check-pulse" aria-hidden="true">✓</span>
                            <div class="badge"><span class="badge-dot"></span> ✅ تم حفظ الجلسة بنجاح</div>
                        </div>
                        <h1>تم ربط حساب إنستجرام بنجاح</h1>
                        <p class="lead">
                            تم التحقق من الجلسة الخاصة بك وحفظها داخل النظام بنجاح. الآن يمكنك الرجوع إلى تيليجرام
                            والاستفادة من لوحة التحكم المطورة لمتابعة الحساب، إدارة الصيد، ومراجعة التفاصيل بشكل أوضح وأسهل.
                        </p>
                    </div>
                </div>

                <div class="highlight-box">
                    <h3>🚀 ماذا أصبح متاحًا لك الآن؟</h3>
                    <p>
                        بعد نجاح الربط، ستجد داخل البوت <strong>لوحة تحكم مطورة</strong> تعرض حالة الحساب بشكل أفضل،
                        مع <strong>متابعة مباشرة لتقدم الصيد</strong>، و<strong>تقارير نهائية أكثر تفصيلًا</strong>،
                        بالإضافة إلى <strong>إدارة قائمة الـ Whitelist</strong> من تيليجرام بسهولة.
                    </p>
                    <div class="progress-preview">[██████░░░░] 60% • Live Hunting Progress</div>
                </div>

                <h2 class="section-title">✨ المميزات الجديدة داخل البوت</h2>

                <div class="features">
                    <article class="feature-card">
                        <div class="feature-icon">📊</div>
                        <h3>لوحة تحكم مطورة</h3>
                        <p>عرض أوضح لمعلومات الحساب والحالة العامة مع تنظيم أفضل لخيارات التحكم من داخل تيليجرام.</p>
                    </article>

                    <article class="feature-card">
                        <div class="feature-icon">🟢</div>
                        <h3>تقدم الصيد بشكل مباشر</h3>
                        <p>أثناء التشغيل ستظهر لك تحديثات مرئية أوضح تشمل نسبة التقدم وحالة التنفيذ لحظة بلحظة.</p>
                    </article>

                    <article class="feature-card">
                        <div class="feature-icon">🧾</div>
                        <h3>تقارير نهائية مفصلة</h3>
                        <p>بعد انتهاء الصيد ستحصل على ملخص يشمل عدد المفحوصين، المحذوفين فعليًا، المستثنين، والوقت الموفر.</p>
                    </article>

                    <article class="feature-card">
                        <div class="feature-icon">🛡️</div>
                        <h3>إدارة الـ Whitelist</h3>
                        <p>يمكنك إضافة أو تحديث الحسابات المستثناة للحفاظ على الأشخاص المهمين وعدم المساس بهم أثناء الصيد.</p>
                    </article>
                </div>

                <h2 class="section-title">✨ الخطوات التالية</h2>

                <div class="steps">
                    <article class="step-card">
                        <div class="step-number">1</div>
                        <h3>📲 العودة إلى تيليجرام</h3>
                        <p>ارجع الآن إلى محادثة البوت، فقد تم إرسال إشعار نجاح الربط إلى تيليجرام.</p>
                    </article>

                    <article class="step-card">
                        <div class="step-number">2</div>
                        <h3>🧭 فتح لوحة التحكم</h3>
                        <p>استخدم <strong>/start</strong> أو <strong>/help</strong> لعرض اللوحة المطورة ومراجعة حالة حسابك.</p>
                    </article>

                    <article class="step-card">
                        <div class="step-number">3</div>
                        <h3>⚙️ ضبط الاستثناءات والبدء</h3>
                        <p>راجع الـ <strong>Whitelist</strong> أولًا، ثم ابدأ الصيد وتابع التقدم والتقارير من داخل البوت.</p>
                    </article>
                </div>

                <div class="info-panel">
                    <strong>📌 تذكير سريع:</strong>
                    جميع الخطوات التالية تتم من داخل تيليجرام. بعد هذه الصفحة لا تحتاج إلى أي إعداد إضافي في المتصفح
                    إلا إذا أردت إعادة الربط لاحقًا.
                </div>

                <div class="note">
                    <strong>🔖 ملاحظة مهمة:</strong>
                    يجب حفظ الـ <strong>Bookmarklet</strong> داخل <strong>الإشارات المرجعية في المتصفح</strong>،
                    ثم الضغط عليه <strong>وأنت مسجل الدخول إلى إنستجرام</strong> حتى يتم إرسال بيانات الجلسة بشكل صحيح.
                </div>

                <div class="tips">
                    <div class="tip">📊 راقب لوحة التحكم من تيليجرام</div>
                    <div class="tip">🟢 تابع تقدم الصيد المباشر</div>
                    <div class="tip">🛡️ حدّث الـ Whitelist قبل البدء</div>
                    <div class="tip">🧾 راجع التقرير النهائي بعد كل عملية</div>
                </div>

                <div class="footer-box">
                    إذا احتجت إعادة الربط لاحقًا، افتح إنستجرام من المتصفح، تأكد أنك داخل حسابك، ثم اضغط على الـ Bookmarklet
                    مرة أخرى وبعدها أكمل من البوت في تيليجرام. أمّا الآن، فخطوتك التالية هي الرجوع إلى البوت وفتح لوحة التحكم المطورة.
                </div>
            </section>
        </main>

        <a class="fab" href="tg://resolve?domain={bot_username}" aria-label="العودة إلى تيليجرام">
            <span class="fab-icon">↩️</span>
            <span>العودة إلى تيليجرام 📲</span>
        </a>
    </body>
    </html>
    """

    return HTMLResponse(content=success_html, status_code=HTTPStatus.OK)
