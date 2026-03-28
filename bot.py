"""Telegram bot entrypoint for the Instagram ghost follower hunter.

This module handles:
- Telegram command routing
- Registration bookmarklet generation
- Google Sheets user lookup via db_manager.GoogleSheetsDB
- Instagram API pagination for following/followers
- Ghost detection and removal
- User-facing status updates

Expected environment variables:
- TELEGRAM_BOT_TOKEN
- WEBHOOK_URL
- GOOGLE_SHEETS_CREDENTIALS_FILE
- GOOGLE_SHEET_NAME
"""

from __future__ import annotations

import asyncio
import base64
import datetime as dt
import json
import logging
import os
import random
from http import HTTPStatus
from typing import Any

import httpx
from dotenv import load_dotenv
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

from api import fetch_instagram_user_id
from db_manager import GoogleSheetsDB, UserRecord

load_dotenv()

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

INSTAGRAM_BASE_URL = "https://www.instagram.com"
SESSION_EXPIRED_MESSAGE = "Session expired. Please re-register using the magic link."


def extract_cookie_value(cookie_string: str, key: str) -> str | None:
    """Return a specific cookie value from a raw cookie string."""
    for part in cookie_string.split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        cookie_key, cookie_value = part.split("=", 1)
        if cookie_key.strip() == key:
            return cookie_value.strip()
    return None


def extract_csrf_token(cookie_string: str) -> str:
    """Extract csrftoken from the stored Instagram cookie string."""
    return extract_cookie_value(cookie_string, "csrftoken") or ""


def parse_cookie_editor_json(raw_json: str) -> str:
    """Convert a Cookie-Editor JSON array into a single Cookie header string."""
    try:
        cookie_items = json.loads(raw_json)
    except json.JSONDecodeError as exc:
        raise ValueError("Invalid cookie JSON. Please paste the full Cookie-Editor JSON array.") from exc

    if not isinstance(cookie_items, list) or not cookie_items:
        raise ValueError("Invalid cookie JSON. Please paste the full Cookie-Editor JSON array.")

    reconstructed_parts: list[str] = []
    for item in cookie_items:
        if not isinstance(item, dict):
            raise ValueError("Invalid cookie JSON. Each cookie entry must be an object.")

        name = str(item.get("name", "")).strip()
        value = str(item.get("value", "")).strip()

        if name:
            reconstructed_parts.append(f"{name}={value}")

    if not reconstructed_parts:
        raise ValueError("Invalid cookie JSON. No usable cookies were found.")

    return "; ".join(reconstructed_parts) + ";"


def build_instagram_headers(user_agent: str, cookie: str, *, is_post: bool = False) -> dict[str, str]:
    """Build Instagram request headers using the stored mobile session."""
    headers: dict[str, str] = {
        "User-Agent": user_agent,
        "Cookie": cookie,
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": INSTAGRAM_BASE_URL,
        "Referer": f"{INSTAGRAM_BASE_URL}/",
        "X-Requested-With": "XMLHttpRequest",
    }

    if is_post:
        headers["X-CSRFToken"] = extract_csrf_token(cookie)
        headers["Content-Type"] = "application/x-www-form-urlencoded"

    return headers


def is_session_expired(status_code: int) -> bool:
    """Return True for Instagram auth/session expiration responses."""
    return status_code in {401, 403}


async def fetch_paginated_users(
    client: httpx.AsyncClient,
    endpoint: str,
    user_record: UserRecord,
) -> dict[str, str]:
    """Fetch all users from a paginated Instagram friendship endpoint.

    Returns a mapping of user_id -> username.
    """
    users: dict[str, str] = {}
    next_max_id: str | None = None

    while True:
        params: dict[str, str] = {}
        if next_max_id:
            params["max_id"] = next_max_id

        response = await client.get(
            f"{INSTAGRAM_BASE_URL}{endpoint}",
            params=params,
            headers=build_instagram_headers(
                user_agent=user_record.user_agent,
                cookie=user_record.cookie,
            ),
        )

        if response.status_code >= 400:
            response.raise_for_status()

        payload = response.json()

        for user in payload.get("users", []):
            user_id = str(user.get("pk", "")).strip()
            username = str(user.get("username", "")).strip()
            if user_id:
                users[user_id] = username or user_id

        next_max_id = payload.get("next_max_id")
        if not next_max_id:
            break

    return users


async def unfollow_user(client: httpx.AsyncClient, ghost_id: str, user_record: UserRecord) -> None:
    """Call Instagram's destroy friendship endpoint for a ghost user."""
    response = await client.post(
        f"{INSTAGRAM_BASE_URL}/api/v1/friendships/destroy/{ghost_id}/",
        headers=build_instagram_headers(
            user_agent=user_record.user_agent,
            cookie=user_record.cookie,
            is_post=True,
        ),
        data={},
    )

    if response.status_code >= 400:
        response.raise_for_status()


def build_register_bookmarklet(telegram_id: str) -> str:
    """Generate a bookmarklet that submits a hidden form to the current webhook."""
    webhook_url = os.getenv("WEBHOOK_URL", "").strip().rstrip("/")
    register_url = f"{webhook_url}/register"

    return (
        "javascript:(()=>{"
        "const d=document;"
        "const f=d.createElement('form');"
        f"f.action='{register_url}';"
        "f.method='POST';"
        "f.style.display='none';"
        "const fields={"
        "cookie:document.cookie,"
        "ua:navigator.userAgent,"
        f"t_id:'{telegram_id}'"
        "};"
        "Object.entries(fields).forEach(([k,v])=>{"
        "const i=d.createElement('input');"
        "i.type='hidden';"
        "i.name=k;"
        "i.value=v;"
        "f.appendChild(i);"
        "});"
        "d.body.appendChild(f);"
        "f.submit();"
        "})()"
    )


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /start."""
    del context

    if update.effective_message is None:
        return

    await update.effective_message.reply_text(
        "Welcome to InstaPro.\n\n"
        "Commands:\n"
        "/register - Get your registration bookmarklet\n"
        "/link [cookie-json] - Link your Instagram session using exported cookies\n"
        "/hunt - Find and remove ghost followers\n"
        "/help - Show usage instructions"
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /help."""
    del context

    if update.effective_message is None:
        return

    await update.effective_message.reply_text(
        "How to use:\n"
        "1. Send /register\n"
        "2. Save the returned JavaScript as a browser bookmark URL\n"
        "3. Open Instagram while logged in on that browser\n"
        "4. Tap the bookmark to register your session\n"
        "5. Or use /link [cookie-json] with your exported Cookie-Editor JSON array\n"
        "6. Return here and run /hunt"
    )


async def register_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /register by sending a bookmarklet snippet."""
    del context

    message = update.effective_message
    user = update.effective_user

    if message is None or user is None:
        return

    webhook_url = os.getenv("WEBHOOK_URL", "").strip()
    if not webhook_url:
        await message.reply_text("Registration is not configured yet. Please contact the admin.")
        return

    bookmarklet = build_register_bookmarklet(str(user.id))

    await message.reply_text(
        "Copy the following and save it as a browser bookmark URL. "
        "Then open Instagram and tap the bookmark:\n\n"
        f"<code>{bookmarklet}</code>",
        parse_mode=ParseMode.HTML,
    )


async def link_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /link with a Cookie-Editor JSON array and store the full cookie string."""
    del context

    message = update.effective_message
    user = update.effective_user

    if message is None or user is None or message.text is None:
        return

    raw_input = message.text.partition(" ")[2].strip()
    if not raw_input:
        await message.reply_text("Usage: /link [Cookie-Editor JSON array]")
        return

    try:
        cookie = parse_cookie_editor_json(raw_input)
    except ValueError as exc:
        await message.reply_text(str(exc))
        return

    user_agent = (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 "
        "Mobile/15E148 Safari/604.1"
    )

    await message.reply_text("Verifying Instagram session...")

    try:
        insta_id = await fetch_instagram_user_id(cookie=cookie, user_agent=user_agent)
    except Exception as exc:
        status_code = getattr(exc, "status_code", None)
        logger.warning("Link command Instagram verification failed: %s", exc)
        if status_code in (HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN):
            await message.reply_text(SESSION_EXPIRED_MESSAGE)
            return
        await message.reply_text("Failed to verify the provided cookie JSON.")
        return

    try:
        db = GoogleSheetsDB()
        db.upsert_user(
            telegram_id=str(user.id),
            insta_id=insta_id,
            cookie=cookie,
            user_agent=user_agent,
            last_run=None,
        )
    except Exception as exc:
        logger.exception("Failed to store /link registration: %s", exc)
        await message.reply_text("Unable to save account link right now. Please try again later.")
        return

    await message.reply_text("✅ Account linked successfully! Your link is valid for one year.")


async def hunt_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /hunt: find accounts the user follows that do not follow back."""
    del context

    message = update.effective_message
    telegram_user = update.effective_user

    if message is None or telegram_user is None:
        return

    try:
        db = GoogleSheetsDB()
        user_record = db.get_user_by_telegram_id(telegram_user.id)
    except Exception as exc:
        logger.exception("Database initialization or lookup failed: %s", exc)
        await message.reply_text("Unable to access the database right now. Please try again later.")
        return

    if user_record is None:
        await message.reply_text("You are not registered yet. Use /register first.")
        return

    await message.reply_text("Starting ghost hunt. Please wait...")

    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        try:
            following = await fetch_paginated_users(
                client,
                f"/api/v1/friendships/{user_record.insta_id}/following/",
                user_record,
            )
            followers = await fetch_paginated_users(
                client,
                f"/api/v1/friendships/{user_record.insta_id}/followers/",
                user_record,
            )
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code
            logger.warning("Instagram list fetch failed with status %s", status_code)
            if is_session_expired(status_code):
                await message.reply_text(SESSION_EXPIRED_MESSAGE)
                return
            await message.reply_text("Failed to read Instagram data. Please try again later.")
            return
        except Exception as exc:
            logger.exception("Unexpected error during Instagram fetch: %s", exc)
            await message.reply_text("Unexpected error while fetching Instagram data.")
            return

        ghost_ids = sorted(set(following.keys()) - set(followers.keys()))

        if not ghost_ids:
            try:
                db.update_last_run(telegram_user.id, dt.datetime.now(dt.timezone.utc).isoformat())
            except Exception as exc:
                logger.exception("Failed to update last_run after empty hunt: %s", exc)
            await message.reply_text("No ghosts found. Everyone you follow follows you back.")
            return

        await message.reply_text(f"Found {len(ghost_ids)} ghost(s). Beginning cleanup...")

        removed_count = 0

        for ghost_id in ghost_ids:
            username = following.get(ghost_id, ghost_id)

            try:
                await unfollow_user(client, ghost_id, user_record)
            except httpx.HTTPStatusError as exc:
                status_code = exc.response.status_code
                logger.warning("Instagram unfollow failed for %s with status %s", ghost_id, status_code)
                if is_session_expired(status_code):
                    await message.reply_text(SESSION_EXPIRED_MESSAGE)
                    return
                await message.reply_text(f"Failed to remove @{username}. Skipping.")
                continue
            except Exception as exc:
                logger.exception("Unexpected error while removing %s: %s", ghost_id, exc)
                await message.reply_text(f"Failed to remove @{username}. Skipping.")
                continue

            removed_count += 1
            await message.reply_text(f"Ghost @{username} has been removed 💀")
            await asyncio.sleep(random.randint(30, 60))

    try:
        db.update_last_run(telegram_user.id, dt.datetime.now(dt.timezone.utc).isoformat())
    except Exception as exc:
        logger.exception("Failed to update last_run after hunt completion: %s", exc)

    await message.reply_text(f"Hunt complete. Removed {removed_count} ghost(s).")


async def login_data_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle manual account-link messages starting with LOGIN_DATA:."""
    del context

    message = update.effective_message
    if message is None or message.text is None:
        return

    text = message.text.strip()
    if not text.startswith("LOGIN_DATA:"):
        return

    await message.reply_text("Decoding LOGIN_DATA...")

    encoded_payload = text[len("LOGIN_DATA:") :].strip()
    if not encoded_payload:
        await message.reply_text("Invalid LOGIN_DATA payload.")
        return

    try:
        decoded_payload = base64.b64decode(encoded_payload).decode("utf-8")
        payload = json.loads(decoded_payload)
    except Exception as exc:
        logger.warning("Failed to decode LOGIN_DATA payload: %s", exc)
        await message.reply_text("Invalid LOGIN_DATA payload.")
        return

    cookie = str(payload.get("cookie", "")).strip()
    user_agent = str(payload.get("ua", "")).strip()
    telegram_id = str(payload.get("t_id", "")).strip()

    if not cookie or not user_agent or not telegram_id:
        await message.reply_text("Invalid LOGIN_DATA payload.")
        return

    await message.reply_text("Verifying with Instagram...")

    try:
        insta_id = await fetch_instagram_user_id(cookie=cookie, user_agent=user_agent)
    except Exception as exc:
        status_code = getattr(exc, "status_code", None)
        logger.warning("Manual link Instagram verification failed: %s", exc)
        if status_code in (HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN):
            await message.reply_text(SESSION_EXPIRED_MESSAGE)
            return
        await message.reply_text("Failed to verify Instagram login data.")
        return

    await message.reply_text("Saving to Sheets...")

    try:
        db = GoogleSheetsDB()
        db.upsert_user(
            telegram_id=telegram_id,
            insta_id=insta_id,
            cookie=cookie,
            user_agent=user_agent,
            last_run=None,
        )
    except Exception as exc:
        logger.exception("Failed to store manual LOGIN_DATA registration: %s", exc)
        await message.reply_text("Unable to save account link right now. Please try again later.")
        return

    await message.reply_text("✅ Account linked successfully via manual paste!")


def build_application() -> Application:
    """Create the PTB application and register command handlers."""
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not bot_token:
        raise RuntimeError(
            "Missing TELEGRAM_BOT_TOKEN environment variable. "
            "Create a .env file next to bot.py and add:\n"
            "TELEGRAM_BOT_TOKEN=your_bot_token_here\n"
            "WEBHOOK_URL=https://your-domain.example.com\n"
            "GOOGLE_SHEETS_CREDENTIALS_FILE=service_account.json\n"
            "GOOGLE_SHEET_NAME=InstaProUsers"
        )

    application = Application.builder().token(bot_token).build()
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("register", register_command))
    application.add_handler(CommandHandler("link", link_command))
    application.add_handler(CommandHandler("hunt", hunt_command))
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, login_data_message_handler)
    )
    return application


def main() -> None:
    """Application startup entrypoint."""
    application = build_application()
    logger.info("Starting Telegram bot polling...")
    application.run_polling()


if __name__ == "__main__":
    main()
