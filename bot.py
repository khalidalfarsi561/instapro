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
import time
from http import HTTPStatus
from typing import Any, AsyncIterator

import httpx
from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

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
DEFAULT_MOBILE_USER_AGENT = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 "
    "Mobile/15E148 Safari/604.1"
)
HUNT_SESSION_KEY = "hunt_session"
HUNT_PAGE_SIZE = 8


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


def build_progress_bar(current: int, total: int, width: int = 8) -> str:
    """Build a compact text progress bar."""
    if total <= 0:
        return f"[{'░' * width}] 0%"
    ratio = max(0.0, min(1.0, current / total))
    filled = min(width, int(round(ratio * width)))
    empty = max(0, width - filled)
    return f"[{'█' * filled}{'░' * empty}] {int(ratio * 100)}%"


async def async_db_call(method_name: str, *args: Any, **kwargs: Any) -> Any:
    """Run blocking Google Sheets operations in a thread."""
    def _runner() -> Any:
        db = GoogleSheetsDB()
        method = getattr(db, method_name)
        return method(*args, **kwargs)

    return await asyncio.to_thread(_runner)


async def iter_paginated_users(
    client: httpx.AsyncClient,
    endpoint: str,
    user_record: UserRecord,
    *,
    human_delay_range: tuple[float, float] = (0.35, 1.1),
) -> AsyncIterator[tuple[str, str]]:
    """Yield paginated Instagram users one-by-one without keeping whole pages in memory."""
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
                yield user_id, username or user_id

        next_max_id = payload.get("next_max_id")
        if not next_max_id:
            break

        await asyncio.sleep(random.uniform(*human_delay_range))


async def fetch_paginated_users(
    client: httpx.AsyncClient,
    endpoint: str,
    user_record: UserRecord,
    *,
    as_set: bool = False,
) -> dict[str, str] | set[str]:
    """Fetch all users from a paginated Instagram friendship endpoint.

    Returns a mapping of user_id -> username by default.
    When as_set=True, returns only user IDs to reduce memory usage for large accounts.
    """
    if as_set:
        users_set: set[str] = set()
        async for user_id, _username in iter_paginated_users(client, endpoint, user_record):
            users_set.add(user_id)
        return users_set

    users: dict[str, str] = {}
    async for user_id, username in iter_paginated_users(client, endpoint, user_record):
        users[user_id] = username
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


def build_dashboard_keyboard() -> InlineKeyboardMarkup:
    """Return the main inline dashboard keyboard."""
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🚀 بدء الصيد", callback_data="dashboard:hunt")],
            [InlineKeyboardButton("🔗 ربط الحساب", callback_data="dashboard:link")],
            [InlineKeyboardButton("📋 قائمة الاستثناءات", callback_data="dashboard:whitelist")],
            [InlineKeyboardButton("📊 الإحصائيات", callback_data="dashboard:stats")],
        ]
    )


def build_hunt_preview_keyboard(session: dict[str, Any]) -> InlineKeyboardMarkup:
    """Build inline keyboard for whitelist toggling and cleanup actions."""
    ghosts: list[dict[str, str]] = session["ghosts"]
    page: int = session["page"]
    total_pages = max(1, (len(ghosts) + HUNT_PAGE_SIZE - 1) // HUNT_PAGE_SIZE)
    start = page * HUNT_PAGE_SIZE
    page_items = ghosts[start : start + HUNT_PAGE_SIZE]
    whitelist_ids: set[str] = set(session["whitelist_ids"])

    rows: list[list[InlineKeyboardButton]] = []
    for ghost in page_items:
        ghost_id = ghost["id"]
        username = ghost["username"]
        is_whitelisted = ghost_id in whitelist_ids
        prefix = "✅" if is_whitelisted else "▫️"
        label = f"{prefix} @{username}"
        rows.append([InlineKeyboardButton(label[:64], callback_data=f"hunt:toggle:{ghost_id}")])

    navigation_row: list[InlineKeyboardButton] = []
    if page > 0:
        navigation_row.append(InlineKeyboardButton("⬅️ السابق", callback_data="hunt:page:prev"))
    if page < total_pages - 1:
        navigation_row.append(InlineKeyboardButton("التالي ➡️", callback_data="hunt:page:next"))
    if navigation_row:
        rows.append(navigation_row)

    rows.append([InlineKeyboardButton("🧹 بدء الحذف", callback_data="hunt:confirm")])
    rows.append([InlineKeyboardButton("🔄 تحديث المعاينة", callback_data="hunt:refresh")])
    rows.append([InlineKeyboardButton("🏠 الرجوع للوحة التحكم", callback_data="dashboard:home")])

    return InlineKeyboardMarkup(rows)


def format_dashboard_text(user_record: UserRecord | None) -> str:
    """Return rich dashboard text for /start and /help."""
    if user_record is None:
        status_badge = "🔴 <b>غير مربوط</b>"
        action_hint = "اربط حسابك أولاً لفتح أدوات الصيد والتقارير."
        last_run_line = "⏱️ آخر تشغيل: <code>لم يتم بعد</code>"
        whitelist_line = "📋 الاستثناءات المحفوظة: <b>0</b>"
    else:
        whitelist_count = len(parse_whitelist_csv(getattr(user_record, "whitelist", "")))
        status_badge = "🟢 <b>جاهز للصيد</b>"
        action_hint = "كل شيء جاهز. ابدأ الفحص الآن أو راجع الاستثناءات والإحصائيات."
        last_run_line = f"⏱️ آخر تشغيل: <code>{user_record.last_run or 'لم يتم بعد'}</code>"
        whitelist_line = f"📋 الاستثناءات المحفوظة: <b>{whitelist_count}</b>"

    return (
        "✨ <b>لوحة تحكم InstaPro</b>\n"
        "━━━━━━━━━━━━━━\n\n"
        "🧭 <b>الحالة الحالية</b>\n"
        f"{status_badge}\n"
        f"{last_run_line}\n"
        f"{whitelist_line}\n\n"
        "⚡ <b>ماذا يمكنك أن تفعل الآن؟</b>\n"
        f"• {action_hint}\n"
        "• متابعة تقدم الصيد لحظة بلحظة\n"
        "• مراجعة الحسابات المستثناة قبل الحذف\n"
        "• الحصول على ملخص نهائي واضح بعد انتهاء المهمة\n\n"
        "👇 <b>اختر من الأزرار بالأسفل:</b>\n"
        "• <b>🚀 بدء الصيد</b>\n"
        "• <b>🔗 ربط الحساب</b>\n"
        "• <b>📋 قائمة الاستثناءات</b>\n"
        "• <b>📊 الإحصائيات</b>"
    )


def format_link_instructions(telegram_id: str) -> str:
    """Return rich text instructions for account linking."""
    bookmarklet = build_register_bookmarklet(telegram_id)
    return (
        "🔗 <b>ربط حساب Instagram</b>\n\n"
        "الطريقة الأسهل:\n"
        "1) أرسل /register أو انسخ الرابط التالي كإشارة مرجعية في المتصفح.\n"
        "2) افتح Instagram وأنت مسجل الدخول.\n"
        "3) اضغط الإشارة المرجعية ليتم حفظ الجلسة.\n\n"
        "أو استخدم الربط اليدوي:\n"
        "<code>/link [Cookie-Editor JSON array]</code>\n\n"
        "رابط الإشارة المرجعية:\n"
        f"<code>{bookmarklet}</code>"
    )


def parse_whitelist_csv(raw_whitelist: str) -> set[str]:
    """Parse a comma-separated whitelist string into a normalized set."""
    return {item.strip() for item in raw_whitelist.split(",") if item.strip()}


def build_whitelist_csv(whitelist_ids: set[str]) -> str:
    """Build a comma-separated whitelist string."""
    return ",".join(sorted(whitelist_ids, key=lambda value: int(value) if value.isdigit() else value))


def format_whitelist_text(user_record: UserRecord | None, session: dict[str, Any] | None = None) -> str:
    """Format the whitelist view text."""
    if user_record is None:
        return (
            "📋 <b>قائمة الاستثناءات</b>\n\n"
            "لا يوجد حساب مربوط حالياً.\n"
            "اضغط <b>🔗 ربط الحساب</b> أولاً."
        )

    stored_ids = parse_whitelist_csv(getattr(user_record, "whitelist", ""))
    preview_ids = set(session["whitelist_ids"]) if session else stored_ids

    if session and session.get("ghosts"):
        ghost_map = {ghost["id"]: ghost["username"] for ghost in session["ghosts"]}
        lines = []
        for ghost_id in sorted(preview_ids):
            username = ghost_map.get(ghost_id, ghost_id)
            lines.append(f"• @{username} <code>{ghost_id}</code>")
        body = "\n".join(lines) if lines else "لا توجد حسابات مستثناة في المعاينة الحالية."
        return (
            "📋 <b>قائمة الاستثناءات الحالية</b>\n\n"
            f"{body}\n\n"
            "يمكنك تعديلها أثناء معاينة الصيد قبل تنفيذ الحذف."
        )

    if not stored_ids:
        return (
            "📋 <b>قائمة الاستثناءات</b>\n\n"
            "لا توجد حسابات مستثناة محفوظة حالياً.\n"
            "ابدأ الصيد أولاً ثم اختر من تريد استثناءه."
        )

    lines = [f"• <code>{ghost_id}</code>" for ghost_id in sorted(stored_ids)]
    return "📋 <b>قائمة الاستثناءات</b>\n\n" + "\n".join(lines)


def format_stats_text(user_record: UserRecord | None) -> str:
    """Return a small statistics snapshot."""
    if user_record is None:
        return (
            "📊 <b>الإحصائيات</b>\n\n"
            "لا توجد بيانات بعد.\n"
            "قم بربط حسابك أولاً من خلال <b>🔗 ربط الحساب</b>."
        )

    whitelist_count = len(parse_whitelist_csv(getattr(user_record, "whitelist", "")))
    return (
        "📊 <b>الإحصائيات</b>\n\n"
        f"• Telegram ID: <code>{user_record.telegram_id}</code>\n"
        f"• Instagram ID: <code>{user_record.insta_id}</code>\n"
        f"• آخر تشغيل: <code>{user_record.last_run or 'لم يتم بعد'}</code>\n"
        f"• عدد الاستثناءات المحفوظة: <b>{whitelist_count}</b>"
    )


def format_ghost_preview_text(session: dict[str, Any]) -> str:
    """Format paginated ghost preview text."""
    ghosts: list[dict[str, str]] = session["ghosts"]
    whitelist_ids: set[str] = set(session["whitelist_ids"])
    page: int = session["page"]
    total_pages = max(1, (len(ghosts) + HUNT_PAGE_SIZE - 1) // HUNT_PAGE_SIZE)
    start = page * HUNT_PAGE_SIZE
    page_items = ghosts[start : start + HUNT_PAGE_SIZE]

    lines: list[str] = []
    for index, ghost in enumerate(page_items, start=start + 1):
        marker = "✅ مستثنى" if ghost["id"] in whitelist_ids else "🗑️ سيتم حذفه"
        lines.append(f"{index}. @{ghost['username']} — <code>{ghost['id']}</code> — {marker}")

    selected_skip = len(whitelist_ids.intersection({ghost["id"] for ghost in ghosts}))
    removable = len(ghosts) - selected_skip

    body = "\n".join(lines) if lines else "لا توجد عناصر في هذه الصفحة."
    return (
        "👻 <b>معاينة نتائج الصيد</b>\n"
        "━━━━━━━━━━━━━━\n\n"
        f"📌 إجمالي الحسابات الوهمية: <b>{len(ghosts)}</b>\n"
        f"✅ المستثنى حالياً: <b>{selected_skip}</b>\n"
        f"🧹 الجاهز للحذف: <b>{removable}</b>\n"
        f"📄 الصفحة: <b>{page + 1}/{total_pages}</b>\n\n"
        f"{body}\n\n"
        "اضغط على أي حساب لإضافته أو إزالته من قائمة الاستثناءات، ثم اختر <b>🧹 بدء الحذف</b>."
    )


async def send_or_edit_dashboard(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    user_record: UserRecord | None = None,
) -> None:
    """Render the inline dashboard either as a new message or callback edit."""
    telegram_user = update.effective_user
    if telegram_user is None:
        return

    if user_record is None:
        try:
            user_record = await async_db_call("get_user_by_telegram_id", telegram_user.id)
        except Exception as exc:
            logger.exception("Failed to load user for dashboard: %s", exc)
            user_record = None

    text = format_dashboard_text(user_record)
    keyboard = build_dashboard_keyboard()
    query = update.callback_query
    message = update.effective_message

    if query is not None and query.message is not None:
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
        return

    if message is not None:
        await message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /start."""
    await send_or_edit_dashboard(update, context)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /help."""
    await send_or_edit_dashboard(update, context)


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
        "🔗 <b>رابط الربط جاهز</b>\n\n"
        "انسخ النص التالي واحفظه كرابط إشارة مرجعية في المتصفح، ثم افتح Instagram واضغط عليه:\n\n"
        f"<code>{bookmarklet}</code>",
        parse_mode=ParseMode.HTML,
        reply_markup=build_dashboard_keyboard(),
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
        await message.reply_text(
            format_link_instructions(str(user.id)),
            parse_mode=ParseMode.HTML,
            reply_markup=build_dashboard_keyboard(),
        )
        return

    try:
        cookie = parse_cookie_editor_json(raw_input)
    except ValueError as exc:
        await message.reply_text(str(exc))
        return

    user_agent = DEFAULT_MOBILE_USER_AGENT

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
        await async_db_call(
            "upsert_user",
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

    await message.reply_text(
        "✅ <b>تم ربط الحساب بنجاح</b>\n\nالآن يمكنك الضغط على <b>🚀 بدء الصيد</b> من لوحة التحكم.",
        parse_mode=ParseMode.HTML,
        reply_markup=build_dashboard_keyboard(),
    )


async def start_hunt_preview(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    use_edit: bool,
) -> None:
    """Fetch ghost accounts and open the whitelist preview flow."""
    message = update.effective_message
    telegram_user = update.effective_user

    if message is None or telegram_user is None:
        return

    try:
        user_record = await async_db_call("get_user_by_telegram_id", telegram_user.id)
    except Exception as exc:
        logger.exception("Database initialization or lookup failed: %s", exc)
        if use_edit and update.callback_query is not None:
            await update.callback_query.edit_message_text("Unable to access the database right now. Please try again later.")
        else:
            await message.reply_text("Unable to access the database right now. Please try again later.")
        return

    if user_record is None:
        text = "أنت غير مسجل بعد. اضغط <b>🔗 ربط الحساب</b> أولاً."
        if use_edit and update.callback_query is not None:
            await update.callback_query.edit_message_text(
                text,
                parse_mode=ParseMode.HTML,
                reply_markup=build_dashboard_keyboard(),
            )
        else:
            await message.reply_text(
                text,
                parse_mode=ParseMode.HTML,
                reply_markup=build_dashboard_keyboard(),
            )
        return

    status_message = None
    if use_edit and update.callback_query is not None:
        status_message = update.callback_query.message
        await update.callback_query.edit_message_text("🔎 جاري فحص المتابعين والحسابات التي تتابعها...")
    else:
        status_message = await message.reply_text("🔎 جاري فحص المتابعين والحسابات التي تتابعها...")

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
                as_set=True,
            )
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code
            logger.warning("Instagram list fetch failed with status %s", status_code)
            if is_session_expired(status_code):
                await status_message.edit_text(SESSION_EXPIRED_MESSAGE)
                return
            await status_message.edit_text("Failed to read Instagram data. Please try again later.")
            return
        except Exception as exc:
            logger.exception("Unexpected error during Instagram fetch: %s", exc)
            await status_message.edit_text("Unexpected error while fetching Instagram data.")
            return

    if not isinstance(following, dict) or not isinstance(followers, set):
        await status_message.edit_text("Unexpected Instagram response format.")
        return

    ghost_ids = sorted(set(following.keys()) - followers)
    if not ghost_ids:
        try:
            await async_db_call("update_last_run", telegram_user.id, dt.datetime.now(dt.timezone.utc).isoformat())
        except Exception as exc:
            logger.exception("Failed to update last_run after empty hunt: %s", exc)
        await status_message.edit_text(
            "✅ لا توجد حسابات وهمية حالياً. كل من تتابعهم يتابعونك أيضاً.",
            reply_markup=build_dashboard_keyboard(),
        )
        return

    ghosts = [{"id": ghost_id, "username": following.get(ghost_id, ghost_id)} for ghost_id in ghost_ids]
    whitelist_ids = parse_whitelist_csv(getattr(user_record, "whitelist", ""))
    session = {
        "ghosts": ghosts,
        "whitelist_ids": whitelist_ids,
        "page": 0,
        "started_at": time.perf_counter(),
    }
    context.user_data[HUNT_SESSION_KEY] = session

    await status_message.edit_text(
        format_ghost_preview_text(session),
        parse_mode=ParseMode.HTML,
        reply_markup=build_hunt_preview_keyboard(session),
    )


async def hunt_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /hunt: open preview and whitelist flow before cleanup."""
    await start_hunt_preview(update, context, use_edit=False)


async def execute_cleanup(
    query_message: Any,
    telegram_id: int,
    user_record: UserRecord,
    session: dict[str, Any],
) -> tuple[int, int, int, float, bool]:
    """Execute cleanup and update a single status message."""
    ghost_entries: list[dict[str, str]] = session["ghosts"]
    whitelist_ids: set[str] = set(session["whitelist_ids"])
    removable = [ghost for ghost in ghost_entries if ghost["id"] not in whitelist_ids]
    skipped_count = len(ghost_entries) - len(removable)
    removed_count = 0
    session_expired = False
    cleanup_started_at = time.perf_counter()

    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        total = len(removable)
        for index, ghost in enumerate(removable, start=1):
            username = ghost["username"]
            ghost_id = ghost["id"]
            progress_bar = build_progress_bar(index - 1, total)
            estimated_remaining = max(0, total - index + 1)

            await query_message.edit_text(
                "🧹 <b>عملية الحذف جارية</b>\n\n"
                f"📊 التقدم: <code>{progress_bar}</code>\n"
                f"🔢 العنصر الحالي: <b>{index}/{total}</b>\n"
                f"👤 الآن يتم حذف: <b>@{username}</b>\n"
                f"⏳ متبقٍ تقريبي: <b>{estimated_remaining}</b> حساب",
                parse_mode=ParseMode.HTML,
            )

            try:
                await unfollow_user(client, ghost_id, user_record)
            except httpx.HTTPStatusError as exc:
                status_code = exc.response.status_code
                logger.warning("Instagram unfollow failed for %s with status %s", ghost_id, status_code)
                if is_session_expired(status_code):
                    session_expired = True
                    break
            except Exception as exc:
                logger.exception("Unexpected error while removing %s: %s", ghost_id, exc)

            if not session_expired:
                removed_count += 1
                if index < total:
                    await asyncio.sleep(random.uniform(45, 90))

        if total > 0 and not session_expired:
            await query_message.edit_text(
                "🧹 <b>اللمسات الأخيرة...</b>\n\n"
                f"📊 التقدم: <code>{build_progress_bar(total, total)}</code>\n"
                "يتم الآن حفظ التقرير وتحديث البيانات.",
                parse_mode=ParseMode.HTML,
            )

    elapsed = time.perf_counter() - cleanup_started_at

    try:
        await async_db_call("update_last_run", telegram_id, dt.datetime.now(dt.timezone.utc).isoformat())
        try:
            await async_db_call("update_whitelist", telegram_id, build_whitelist_csv(whitelist_ids))
        except AttributeError:
            pass
    except Exception as exc:
        logger.exception("Failed to update post-hunt metadata: %s", exc)

    return removed_count, skipped_count, len(ghost_entries), elapsed, session_expired


async def dashboard_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle dashboard and hunt callback interactions."""
    query = update.callback_query
    telegram_user = update.effective_user
    if query is None or telegram_user is None:
        return

    await query.answer()

    data = query.data or ""
    try:
        user_record = await async_db_call("get_user_by_telegram_id", telegram_user.id)
    except Exception as exc:
        logger.exception("Failed to initialize DB during callback: %s", exc)
        await query.edit_message_text("Unable to access the database right now. Please try again later.")
        return

    if data == "dashboard:home":
        await send_or_edit_dashboard(update, context, user_record=user_record)
        return

    if data == "dashboard:link":
        await query.edit_message_text(
            format_link_instructions(str(telegram_user.id)),
            parse_mode=ParseMode.HTML,
            reply_markup=build_dashboard_keyboard(),
        )
        return

    if data == "dashboard:whitelist":
        session = context.user_data.get(HUNT_SESSION_KEY)
        await query.edit_message_text(
            format_whitelist_text(user_record, session if isinstance(session, dict) else None),
            parse_mode=ParseMode.HTML,
            reply_markup=build_dashboard_keyboard(),
        )
        return

    if data == "dashboard:stats":
        await query.edit_message_text(
            format_stats_text(user_record),
            parse_mode=ParseMode.HTML,
            reply_markup=build_dashboard_keyboard(),
        )
        return

    if data == "dashboard:hunt":
        await start_hunt_preview(update, context, use_edit=True)
        return

    session = context.user_data.get(HUNT_SESSION_KEY)
    if not isinstance(session, dict):
        await query.edit_message_text(
            "انتهت جلسة المعاينة. اضغط <b>🚀 بدء الصيد</b> لبدء جلسة جديدة.",
            parse_mode=ParseMode.HTML,
            reply_markup=build_dashboard_keyboard(),
        )
        return

    if data.startswith("hunt:toggle:"):
        ghost_id = data.split(":")[-1]
        whitelist_ids: set[str] = set(session["whitelist_ids"])
        if ghost_id in whitelist_ids:
            whitelist_ids.remove(ghost_id)
        else:
            whitelist_ids.add(ghost_id)
        session["whitelist_ids"] = whitelist_ids
        context.user_data[HUNT_SESSION_KEY] = session
        await query.edit_message_text(
            format_ghost_preview_text(session),
            parse_mode=ParseMode.HTML,
            reply_markup=build_hunt_preview_keyboard(session),
        )
        return

    if data == "hunt:page:prev":
        session["page"] = max(0, int(session["page"]) - 1)
        context.user_data[HUNT_SESSION_KEY] = session
        await query.edit_message_text(
            format_ghost_preview_text(session),
            parse_mode=ParseMode.HTML,
            reply_markup=build_hunt_preview_keyboard(session),
        )
        return

    if data == "hunt:page:next":
        max_page = max(0, (len(session["ghosts"]) - 1) // HUNT_PAGE_SIZE)
        session["page"] = min(max_page, int(session["page"]) + 1)
        context.user_data[HUNT_SESSION_KEY] = session
        await query.edit_message_text(
            format_ghost_preview_text(session),
            parse_mode=ParseMode.HTML,
            reply_markup=build_hunt_preview_keyboard(session),
        )
        return

    if data == "hunt:refresh":
        await query.edit_message_text(
            format_ghost_preview_text(session),
            parse_mode=ParseMode.HTML,
            reply_markup=build_hunt_preview_keyboard(session),
        )
        return

    if data == "hunt:confirm":
        if user_record is None:
            await query.edit_message_text(
                "أنت غير مسجل بعد. اضغط <b>🔗 ربط الحساب</b> أولاً.",
                parse_mode=ParseMode.HTML,
                reply_markup=build_dashboard_keyboard(),
            )
            return

        whitelist_ids = set(session["whitelist_ids"])
        try:
            await async_db_call("update_whitelist", telegram_user.id, build_whitelist_csv(whitelist_ids))
        except AttributeError:
            pass
        except Exception as exc:
            logger.exception("Failed to persist whitelist before cleanup: %s", exc)

        removable_count = len([ghost for ghost in session["ghosts"] if ghost["id"] not in whitelist_ids])
        if removable_count == 0:
            await query.edit_message_text(
                "📋 جميع الحسابات الوهمية الحالية ضمن قائمة الاستثناءات، لذلك لن يتم حذف أي حساب.\n\n"
                "يمكنك إزالة بعض الحسابات من الاستثناءات ثم إعادة المحاولة.",
                reply_markup=build_dashboard_keyboard(),
            )
            return

        removed_count, skipped_count, total_ghosts, elapsed, session_expired = await execute_cleanup(
            query.message,
            telegram_user.id,
            user_record,
            session,
        )

        context.user_data.pop(HUNT_SESSION_KEY, None)

        if session_expired:
            await query.message.edit_text(SESSION_EXPIRED_MESSAGE, reply_markup=build_dashboard_keyboard())
            return

        started_at = float(session.get("started_at", time.perf_counter()))
        total_session_elapsed = max(elapsed, time.perf_counter() - started_at)
        time_saved_seconds = skipped_count * 60

        await query.message.edit_text("✅ تم الانتهاء من عملية الصيد.", reply_markup=build_dashboard_keyboard())
        await query.message.reply_text(
            "📄 <b>تقرير الصيد النهائي</b>\n"
            "━━━━━━━━━━━━━━\n\n"
            f"🔎 Total Scanned: <b>{total_ghosts}</b>\n"
            f"🗑️ Actually Removed: <b>{removed_count}</b>\n"
            f"✅ Whitelisted: <b>{skipped_count}</b>\n"
            f"⚡ Time Saved: <b>{time_saved_seconds // 60}</b> دقيقة و <b>{time_saved_seconds % 60}</b> ثانية\n"
            f"⏱️ الزمن المستغرق: <b>{total_session_elapsed:.1f}</b> ثانية",
            parse_mode=ParseMode.HTML,
            reply_markup=build_dashboard_keyboard(),
        )
        return


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
        await async_db_call(
            "upsert_user",
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

    await message.reply_text(
        "✅ Account linked successfully via manual paste!",
        reply_markup=build_dashboard_keyboard(),
    )


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
    application.add_handler(CallbackQueryHandler(dashboard_callback_handler))
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