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
import html
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
from db_manager import UserRecord

load_dotenv()

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

INSTAGRAM_BASE_URL = "https://www.instagram.com"
SESSION_EXPIRED_MESSAGE = (
    "⚠️ <b>انتهت الجلسة</b>\n\n"
    "جلسة Instagram الحالية لم تعد صالحة. يرجى إعادة الربط عبر الرابط السحري ثم المحاولة مرة أخرى."
)
DEFAULT_MOBILE_USER_AGENT = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 "
    "Mobile/15E148 Safari/604.1"
)
HUNT_SESSION_KEY = "hunt_session"
HUNT_PAGE_SIZE = 8
DEFAULT_SPEED_MODE = "normal"
VERIFIED_AUTO_PROTECT_FOLLOWERS_THRESHOLD = 50000
SPEED_MODE_CONFIG: dict[str, dict[str, Any]] = {
    "safe": {
        "label": "🐢 الوضع الآمن",
        "description": "أبطأ وأكثر حذرًا لتقليل المخاطر",
        "delay_range": (120, 300),
        "eta_multiplier": 210,
    },
    "normal": {
        "label": "⚖️ الوضع المتوازن",
        "description": "السرعة الحالية بتوازن جيد",
        "delay_range": (45, 90),
        "eta_multiplier": 67.5,
    },
}


def escape_html(value: Any) -> str:
    """Escape dynamic text for Telegram HTML parse mode."""
    return html.escape(str(value), quote=False)


def bool_from_sheet_flag(value: str | bool | None) -> bool:
    """Convert flexible sheet flag values to bool."""
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


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
        raise ValueError(
            "⚠️ <b>JSON غير صالح</b>\n\nيرجى لصق مصفوفة Cookie-Editor JSON كاملة."
        ) from exc

    if not isinstance(cookie_items, list) or not cookie_items:
        raise ValueError(
            "⚠️ <b>المدخلات غير صالحة</b>\n\nيرجى لصق مصفوفة Cookie-Editor JSON كاملة."
        )

    reconstructed_parts: list[str] = []
    for item in cookie_items:
        if not isinstance(item, dict):
            raise ValueError(
                "⚠️ <b>تنسيق غير صالح</b>\n\nكل عنصر Cookie يجب أن يكون كائن JSON مستقل."
            )

        name = str(item.get("name", "")).strip()
        value = str(item.get("value", "")).strip()

        if name:
            reconstructed_parts.append(f"{name}={value}")

    if not reconstructed_parts:
        raise ValueError(
            "⚠️ <b>لم يتم العثور على كوكيز صالحة</b>\n\nتأكد من نسخ البيانات كاملة من Cookie-Editor."
        )

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


def build_progress_bar(current: int, total: int, width: int = 10) -> str:
    """Build an expressive Telegram-safe progress bar with changing activity emojis."""
    if total <= 0:
        return "🌑 ░░░░░░░░░░ 0%"

    ratio = max(0.0, min(1.0, current / total))
    percentage = int(ratio * 100)
    filled = min(width, int(round(ratio * width)))
    empty = max(0, width - filled)

    if percentage <= 0:
        mood = "🌑"
        trail = "بدء"
    elif percentage < 34:
        mood = "🌓"
        trail = "يبدأ"
    elif percentage < 67:
        mood = "🌗"
        trail = "يتقدّم"
    elif percentage < 100:
        mood = "🌕"
        trail = "قريب من الاكتمال"
    else:
        mood = "🌕"
        trail = "اكتمل"

    return f"{mood} {'█' * filled}{'░' * empty} {percentage}% • {trail}"


async def async_db_call(method_name: str, *args: Any, **kwargs: Any) -> Any:
    """Run blocking Google Sheets operations in a thread."""
    from db_manager import GoogleSheetsDB

    def _runner() -> Any:
        db = GoogleSheetsDB()
        method = getattr(db, method_name)
        return method(*args, **kwargs)

    return await asyncio.to_thread(_runner)


async def persist_user_record(user_record: UserRecord) -> UserRecord:
    """Persist the full user record with new schema fields preserved."""
    return await async_db_call(
        "upsert_user",
        telegram_id=user_record.telegram_id,
        insta_id=user_record.insta_id,
        cookie=user_record.cookie,
        user_agent=user_record.user_agent,
        last_run=user_record.last_run,
        whitelist=user_record.whitelist,
        speed_mode=user_record.speed_mode,
        is_hunting=user_record.is_hunting,
    )


def get_speed_mode_config(speed_mode: str | None) -> dict[str, Any]:
    """Return a normalized speed mode config."""
    normalized = (speed_mode or DEFAULT_SPEED_MODE).strip().lower()
    return SPEED_MODE_CONFIG.get(normalized, SPEED_MODE_CONFIG[DEFAULT_SPEED_MODE])


def estimate_cleanup_seconds(removable_count: int, speed_mode: str) -> int:
    """Estimate runtime for cleanup based on selected speed mode."""
    config = get_speed_mode_config(speed_mode)
    per_account = int(config["eta_multiplier"])
    return max(60, removable_count * per_account) if removable_count > 0 else 0


def format_duration_ar(seconds: int | float) -> str:
    """Format a duration in Arabic-friendly units."""
    total_seconds = max(0, int(round(seconds)))
    minutes, secs = divmod(total_seconds, 60)
    hours, minutes = divmod(minutes, 60)

    parts: list[str] = []
    if hours:
        parts.append(f"{hours} ساعة")
    if minutes:
        parts.append(f"{minutes} دقيقة")
    if secs or not parts:
        parts.append(f"{secs} ثانية")
    return " و ".join(parts)


def format_modern_notice(title: str, body_lines: list[str], accent_emoji: str = "ℹ️") -> str:
    """Build a modern Arabic-first HTML notice block."""
    body = "\n".join(f"• {line}" for line in body_lines if line)
    return (
        f"{accent_emoji} <b>{title}</b>\n"
        "━━━━━━━━━━━━━━\n\n"
        f"{body}"
    )


def format_active_hunt_warning(user_record: UserRecord | None) -> str:
    """Return a clear warning when a hunt is already active."""
    speed_label = (
        escape_html(get_speed_mode_config(getattr(user_record, "speed_mode", DEFAULT_SPEED_MODE))["label"])
        if user_record
        else escape_html(get_speed_mode_config(DEFAULT_SPEED_MODE)["label"])
    )
    last_run = escape_html(getattr(user_record, "last_run", "") or "قيد التحديث")
    return (
        "🚫 <b>عملية صيد نشطة بالفعل</b>\n"
        "━━━━━━━━━━━━━━\n\n"
        "هناك مهمة حذف/تنظيف تعمل الآن لهذا الحساب، لذلك تم منع بدء مهمة جديدة تلقائياً لحماية الجلسة والنتائج.\n\n"
        f"⚙️ وضع السرعة الحالي: <b>{speed_label}</b>\n"
        f"⏱️ آخر تحديث محفوظ: <code>{last_run}</code>\n\n"
        "💡 <b>ماذا تفعل الآن؟</b>\n"
        "• انتظر اكتمال العملية الجارية.\n"
        "• أو استخدم زر <b>🛑 إيقاف العملية فوراً</b> من رسالة التقدّم الحالية.\n"
        "• بعد الإيقاف أو الانتهاء، يمكنك بدء صيد جديد بأمان."
    )


async def ensure_user_not_hunting(
    telegram_id: int,
    *,
    update: Update | None = None,
    user_record: UserRecord | None = None,
    reply_markup: InlineKeyboardMarkup | None = None,
    edit_if_callback: bool = False,
) -> tuple[bool, UserRecord | None]:
    """Load user state and warn if a concurrent hunt is already active."""
    record = user_record
    if record is None:
        try:
            record = await async_db_call("get_user_by_telegram_id", telegram_id)
        except Exception as exc:
            logger.exception("Failed to load user while checking hunt lock: %s", exc)
            return False, None

    if record is None:
        return True, None

    if not bool_from_sheet_flag(getattr(record, "is_hunting", "false")):
        return True, record

    if update is not None:
        warning_text = format_active_hunt_warning(record)
        query = update.callback_query
        message = update.effective_message
        keyboard = reply_markup or build_dashboard_keyboard(getattr(record, "speed_mode", DEFAULT_SPEED_MODE))

        if edit_if_callback and query is not None and query.message is not None:
            await query.edit_message_text(
                warning_text,
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard,
            )
        elif message is not None:
            await message.reply_text(
                warning_text,
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard,
            )

    return False, record


async def iter_paginated_users(
    client: httpx.AsyncClient,
    endpoint: str,
    user_record: UserRecord,
    *,
    human_delay_range: tuple[float, float] = (0.35, 1.1),
) -> AsyncIterator[dict[str, str]]:
    """Yield paginated Instagram users one-by-one with safety metadata."""
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
                yield {
                    "id": user_id,
                    "username": username or user_id,
                    "is_verified": "true" if bool(user.get("is_verified", False)) else "false",
                    "follower_count": str(user.get("follower_count") or 0),
                }

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
) -> dict[str, dict[str, str]] | set[str]:
    """Fetch all users from a paginated Instagram friendship endpoint."""
    if as_set:
        users_set: set[str] = set()
        async for user in iter_paginated_users(client, endpoint, user_record):
            users_set.add(user["id"])
        return users_set

    users: dict[str, dict[str, str]] = {}
    async for user in iter_paginated_users(client, endpoint, user_record):
        users[user["id"]] = user
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


def build_dashboard_keyboard(selected_speed_mode: str | None = None) -> InlineKeyboardMarkup:
    """Return the main inline dashboard keyboard with speed selection."""
    selected_speed_mode = (selected_speed_mode or DEFAULT_SPEED_MODE).strip().lower()
    safe_prefix = "✅ " if selected_speed_mode == "safe" else ""
    normal_prefix = "✅ " if selected_speed_mode == "normal" else ""

    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🚀 بدء الصيد", callback_data="dashboard:hunt")],
            [
                InlineKeyboardButton(
                    f"{safe_prefix}🐢 الوضع الآمن",
                    callback_data="speed:set:safe",
                ),
                InlineKeyboardButton(
                    f"{normal_prefix}⚖️ الوضع المتوازن",
                    callback_data="speed:set:normal",
                ),
            ],
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
        shield = "🛡️ " if ghost.get("auto_protected") == "true" else ""
        label = f"{prefix} {shield}@{username}"
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


def build_stop_keyboard() -> InlineKeyboardMarkup:
    """Inline keyboard used during cleanup execution."""
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("🛑 إيقاف العملية فوراً", callback_data="hunt:stop")]]
    )


def parse_whitelist_csv(raw_whitelist: str) -> set[str]:
    """Parse a comma-separated whitelist string into a normalized set."""
    return {item.strip() for item in raw_whitelist.split(",") if item.strip()}


def build_whitelist_csv(whitelist_ids: set[str]) -> str:
    """Build a comma-separated whitelist string."""
    return ",".join(sorted(whitelist_ids, key=lambda value: int(value) if value.isdigit() else value))


def format_dashboard_text(user_record: UserRecord | None) -> str:
    """Return rich dashboard text for /start and /help."""
    if user_record is None:
        status_badge = "🔴 <b>غير مربوط</b>"
        action_hint = "اربط حسابك أولاً لفتح أدوات الصيد والتقارير."
        last_run_line = "⏱️ آخر تشغيل: <code>لم يتم بعد</code>"
        whitelist_line = "📋 الاستثناءات المحفوظة: <b>0</b>"
        speed_line = "⚙️ وضع السرعة: <b>⚖️ الوضع المتوازن</b>"
        hunt_line = "🎯 حالة الصيد: <b>لا توجد عملية جارية</b>"
    else:
        whitelist_count = len(parse_whitelist_csv(getattr(user_record, "whitelist", "")))
        speed_mode = get_speed_mode_config(getattr(user_record, "speed_mode", DEFAULT_SPEED_MODE))
        is_hunting = bool_from_sheet_flag(getattr(user_record, "is_hunting", "false"))
        status_badge = "🟢 <b>جاهز للصيد</b>" if not is_hunting else "🟠 <b>عملية جارية الآن</b>"
        action_hint = (
            "كل شيء جاهز. ابدأ الفحص الآن أو راجع الاستثناءات والإحصائيات."
            if not is_hunting
            else "هناك مهمة صيد نشطة بالفعل. يمكنك متابعتها أو إيقافها قبل بدء مهمة جديدة."
        )
        last_run_line = f"⏱️ آخر تشغيل: <code>{escape_html(user_record.last_run or 'لم يتم بعد')}</code>"
        whitelist_line = f"📋 الاستثناءات المحفوظة: <b>{whitelist_count}</b>"
        speed_line = f"⚙️ وضع السرعة: <b>{escape_html(speed_mode['label'])}</b>"
        hunt_line = (
            "🎯 حالة الصيد: <b>قيد التنفيذ الآن</b>"
            if is_hunting
            else "🎯 حالة الصيد: <b>لا توجد عملية جارية</b>"
        )

    return (
        "✨ <b>لوحة تحكم InstaPro</b>\n"
        "━━━━━━━━━━━━━━\n\n"
        "🧭 <b>الحالة الحالية</b>\n"
        f"{status_badge}\n"
        f"{last_run_line}\n"
        f"{whitelist_line}\n"
        f"{speed_line}\n"
        f"{hunt_line}\n\n"
        "⚡ <b>ماذا يمكنك أن تفعل الآن؟</b>\n"
        f"• {action_hint}\n"
        "• متابعة تقدم الصيد لحظة بلحظة\n"
        "• مراجعة الحسابات المستثناة قبل الحذف\n"
        "• الحصول على ملخص نهائي واضح بعد انتهاء المهمة\n\n"
        "👇 <b>اختر من الأزرار بالأسفل:</b>\n"
        "• <b>🚀 بدء الصيد</b>\n"
        "• <b>🐢 / ⚖️ أوضاع السرعة</b>\n"
        "• <b>🔗 ربط الحساب</b>\n"
        "• <b>📋 قائمة الاستثناءات</b>\n"
        "• <b>📊 الإحصائيات</b>"
    )


def format_link_instructions(telegram_id: str) -> str:
    """Return rich text instructions for account linking."""
    bookmarklet = build_register_bookmarklet(telegram_id)
    return (
        "🔗 <b>ربط حساب Instagram</b>\n\n"
        "1) أرسل <b>/register</b> أو انسخ الرابط التالي كإشارة مرجعية في المتصفح.\n"
        "2) افتح Instagram وأنت مسجل الدخول.\n"
        "3) اضغط الإشارة المرجعية ليتم حفظ الجلسة.\n\n"
        "أو استخدم الربط اليدوي:\n"
        "<code>/link [Cookie-Editor JSON array]</code>\n\n"
        "رابط الإشارة المرجعية:\n"
        f"<code>{escape_html(bookmarklet)}</code>"
    )


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
        ghost_map = {ghost["id"]: ghost for ghost in session["ghosts"]}
        lines = []
        for ghost_id in sorted(preview_ids):
            ghost = ghost_map.get(ghost_id, {"username": ghost_id, "auto_protected": "false"})
            shield = " 🛡️" if ghost.get("auto_protected") == "true" else ""
            lines.append(
                f"• @{escape_html(ghost['username'])}{shield} <code>{escape_html(ghost_id)}</code>"
            )
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

    lines = [f"• <code>{escape_html(ghost_id)}</code>" for ghost_id in sorted(stored_ids)]
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
    speed_mode = get_speed_mode_config(getattr(user_record, "speed_mode", DEFAULT_SPEED_MODE))
    is_hunting = bool_from_sheet_flag(getattr(user_record, "is_hunting", "false"))
    return (
        "📊 <b>الإحصائيات</b>\n"
        "━━━━━━━━━━━━━━\n\n"
        f"• Telegram ID: <code>{escape_html(user_record.telegram_id)}</code>\n"
        f"• Instagram ID: <code>{escape_html(user_record.insta_id)}</code>\n"
        f"• آخر تشغيل: <code>{escape_html(user_record.last_run or 'لم يتم بعد')}</code>\n"
        f"• عدد الاستثناءات المحفوظة: <b>{whitelist_count}</b>\n"
        f"• وضع السرعة الحالي: <b>{escape_html(speed_mode['label'])}</b>\n"
        f"• حالة الصيد: <b>{'جارية' if is_hunting else 'متوقفة'}</b>"
    )


def format_ghost_preview_text(session: dict[str, Any]) -> str:
    """Format paginated ghost preview text."""
    ghosts: list[dict[str, str]] = session["ghosts"]
    whitelist_ids: set[str] = set(session["whitelist_ids"])
    page: int = session["page"]
    total_pages = max(1, (len(ghosts) + HUNT_PAGE_SIZE - 1) // HUNT_PAGE_SIZE)
    start = page * HUNT_PAGE_SIZE
    page_items = ghosts[start : start + HUNT_PAGE_SIZE]
    auto_protected_count = int(session.get("auto_protected_count", 0))
    speed_mode = get_speed_mode_config(session.get("speed_mode", DEFAULT_SPEED_MODE))

    lines: list[str] = []
    for index, ghost in enumerate(page_items, start=start + 1):
        status_bits: list[str] = []
        if ghost["id"] in whitelist_ids:
            status_bits.append("✅ مستثنى")
        else:
            status_bits.append("🗑️ سيتم حذفه")
        if ghost.get("auto_protected") == "true":
            status_bits.append("🛡️ حماية تلقائية")
        if ghost.get("is_verified") == "true":
            status_bits.append("✔️ موثق")
        follower_count = int(ghost.get("follower_count", "0") or 0)
        if follower_count >= VERIFIED_AUTO_PROTECT_FOLLOWERS_THRESHOLD:
            status_bits.append(f"👥 {follower_count:,} متابع")

        lines.append(
            f"{index}. @{escape_html(ghost['username'])} — <code>{escape_html(ghost['id'])}</code> — "
            + " • ".join(status_bits)
        )

    selected_skip = len(whitelist_ids.intersection({ghost["id"] for ghost in ghosts}))
    removable = len(ghosts) - selected_skip
    body = "\n".join(lines) if lines else "لا توجد عناصر في هذه الصفحة."

    return (
        "👻 <b>معاينة نتائج الصيد</b>\n"
        "━━━━━━━━━━━━━━\n\n"
        f"📌 إجمالي الحسابات الوهمية: <b>{len(ghosts)}</b>\n"
        f"🛡️ تم تأمينها تلقائياً: <b>{auto_protected_count}</b>\n"
        f"✅ المستثنى حالياً: <b>{selected_skip}</b>\n"
        f"🧹 الجاهز للحذف: <b>{removable}</b>\n"
        f"⚙️ وضع السرعة: <b>{escape_html(speed_mode['label'])}</b>\n"
        f"📄 الصفحة: <b>{page + 1}/{total_pages}</b>\n\n"
        f"{body}\n\n"
        "اضغط على أي حساب لإضافته أو إزالته من قائمة الاستثناءات، ثم اختر <b>🧹 بدء الحذف</b>."
    )


def format_pre_hunt_report(session: dict[str, Any]) -> str:
    """Render the pre-hunt summary report."""
    ghosts: list[dict[str, str]] = session["ghosts"]
    whitelist_ids: set[str] = set(session["whitelist_ids"])
    removable_count = len([ghost for ghost in ghosts if ghost["id"] not in whitelist_ids])
    auto_protected_count = int(session.get("auto_protected_count", 0))
    speed_mode = get_speed_mode_config(session.get("speed_mode", DEFAULT_SPEED_MODE))
    estimated_seconds = estimate_cleanup_seconds(removable_count, session.get("speed_mode", DEFAULT_SPEED_MODE))

    return (
        "🧾 <b>تقرير ما قبل التنفيذ</b>\n"
        "━━━━━━━━━━━━━━\n\n"
        f"👻 إجمالي الوهميين المكتشفين: <b>{len(ghosts)}</b>\n"
        f"🛡️ الحسابات المؤمنة تلقائياً: <b>{auto_protected_count}</b>\n"
        f"✅ العناصر الموجودة في الاستثناءات: <b>{len(whitelist_ids)}</b>\n"
        f"🧹 الجاهز للحذف فعلياً: <b>{removable_count}</b>\n"
        f"⚙️ وضع السرعة المختار: <b>{escape_html(speed_mode['label'])}</b>\n"
        f"⏳ الوقت المتوقع للعملية: <b>{escape_html(format_duration_ar(estimated_seconds))}</b>\n\n"
        f"💡 <i>{escape_html(speed_mode['description'])}</i>\n"
        "• سيتم حفظ النتائج الحالية حتى لو تم الضغط على زر الإيقاف أثناء التنفيذ."
    )


def format_progress_text(
    *,
    progress_bar: str,
    current_index: int,
    total: int,
    username: str,
    speed_mode: str,
    estimated_remaining_seconds: int,
    removed_count: int,
    skipped_count: int,
) -> str:
    """Render a polished progress update."""
    return (
        "🧹 <b>عملية التنظيف قيد التنفيذ</b>\n"
        "━━━━━━━━━━━━━━\n\n"
        f"📊 التقدّم: <code>{escape_html(progress_bar)}</code>\n"
        f"🔢 العنصر الحالي: <b>{current_index}/{total}</b>\n"
        f"👤 الآن تتم معالجة: <b>@{escape_html(username)}</b>\n"
        f"🗑️ تم حذفهم حتى الآن: <b>{removed_count}</b>\n"
        f"✅ تم تجاوزهم / استثناؤهم: <b>{skipped_count}</b>\n"
        f"⚙️ وضع السرعة: <b>{escape_html(get_speed_mode_config(speed_mode)['label'])}</b>\n"
        f"⏳ المتبقي تقريباً: <b>{escape_html(format_duration_ar(estimated_remaining_seconds))}</b>\n\n"
        "💡 يمكنك الضغط على <b>🛑 إيقاف العملية فوراً</b> وسيتم حفظ ما تم إنجازه."
    )


def format_final_report(
    *,
    total_ghosts: int,
    removed_count: int,
    skipped_count: int,
    whitelist_count: int,
    auto_protected_count: int,
    time_saved_minutes: int,
    speed_mode: str,
    total_session_elapsed: float,
    stopped_by_user: bool,
) -> str:
    """Render the final cleanup report."""
    title = "📄 <b>تقرير الإيقاف النهائي</b>" if stopped_by_user else "📄 <b>تقرير الصيد النهائي</b>"
    return (
        f"{title}\n"
        "━━━━━━━━━━━━━━\n\n"
        f"🔎 إجمالي الحسابات التي تم فحصها: <b>{total_ghosts}</b>\n"
        f"🗑️ عدد الحسابات المحذوفة فعلياً: <b>{removed_count}</b>\n"
        f"✅ عدد الحسابات المستثناة (Whitelist): <b>{whitelist_count}</b>\n"
        f"🛡️ حماية تلقائية: <b>{auto_protected_count}</b>\n"
        f"⏱️ الوقت الإجمالي الموفر: <b>{time_saved_minutes}</b> دقيقة\n"
        f"⚙️ وضع السرعة: <b>{escape_html(get_speed_mode_config(speed_mode)['label'])}</b>\n"
        f"⏳ الزمن المستغرق: <b>{escape_html(format_duration_ar(total_session_elapsed))}</b>\n\n"
        f"{'🛑 <i>تم إيقاف العملية بناءً على طلبك مع الاحتفاظ بالنتائج الحالية.</i>' if stopped_by_user else '✅ <i>اكتملت العملية وتم حفظ النتائج بنجاح.</i>'}"
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

    selected_speed_mode = getattr(user_record, "speed_mode", DEFAULT_SPEED_MODE) if user_record else DEFAULT_SPEED_MODE
    text = format_dashboard_text(user_record)
    keyboard = build_dashboard_keyboard(selected_speed_mode)
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
        await message.reply_text(
            "⚠️ <b>الربط غير مهيأ</b>\n\nيرجى التواصل مع المشرف لإعداد رابط الخدمة أولاً.",
            parse_mode=ParseMode.HTML,
        )
        return

    bookmarklet = build_register_bookmarklet(str(user.id))

    await message.reply_text(
        "🔗 <b>رابط الربط جاهز</b>\n"
        "━━━━━━━━━━━━━━\n\n"
        "انسخ النص التالي واحفظه كرابط إشارة مرجعية في المتصفح، ثم افتح Instagram واضغط عليه:\n\n"
        f"<code>{escape_html(bookmarklet)}</code>",
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
        await message.reply_text(str(exc), parse_mode=ParseMode.HTML)
        return

    user_agent = DEFAULT_MOBILE_USER_AGENT

    await message.reply_text(
        "🔎 <b>جارٍ التحقق من جلسة Instagram...</b>",
        parse_mode=ParseMode.HTML,
    )

    try:
        insta_id = await fetch_instagram_user_id(cookie=cookie, user_agent=user_agent)
    except Exception as exc:
        status_code = getattr(exc, "status_code", None)
        logger.warning("Link command Instagram verification failed: %s", exc)
        if status_code in (HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN):
            await message.reply_text(SESSION_EXPIRED_MESSAGE, parse_mode=ParseMode.HTML)
            return
        await message.reply_text(
            "❌ <b>فشل التحقق من الكوكيز</b>\n\nتأكد من صحة Cookie-Editor JSON ثم أعد المحاولة.",
            parse_mode=ParseMode.HTML,
        )
        return

    try:
        await async_db_call(
            "upsert_user",
            telegram_id=str(user.id),
            insta_id=insta_id,
            cookie=cookie,
            user_agent=user_agent,
            last_run=None,
            speed_mode=DEFAULT_SPEED_MODE,
            is_hunting="false",
        )
    except Exception as exc:
        logger.exception("Failed to store /link registration: %s", exc)
        await message.reply_text(
            "⚠️ <b>تعذر حفظ الربط حالياً</b>\n\nيرجى المحاولة مرة أخرى بعد قليل.",
            parse_mode=ParseMode.HTML,
        )
        return

    await message.reply_text(
        "✅ <b>تم ربط الحساب بنجاح</b>\n\nالآن يمكنك الضغط على <b>🚀 بدء الصيد</b> من لوحة التحكم.",
        parse_mode=ParseMode.HTML,
        reply_markup=build_dashboard_keyboard(DEFAULT_SPEED_MODE),
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
        error_text = (
            "⚠️ <b>تعذر الوصول لقاعدة البيانات</b>\n\nيرجى المحاولة مرة أخرى بعد قليل."
        )
        if use_edit and update.callback_query is not None:
            await update.callback_query.edit_message_text(error_text, parse_mode=ParseMode.HTML)
        else:
            await message.reply_text(error_text, parse_mode=ParseMode.HTML)
        return

    if user_record is None:
        text = "🔗 <b>أنت غير مسجل بعد</b>\n\nاضغط <b>🔗 ربط الحساب</b> أولاً."
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

    allowed, _ = await ensure_user_not_hunting(
        telegram_user.id,
        update=update,
        user_record=user_record,
        reply_markup=build_dashboard_keyboard(getattr(user_record, "speed_mode", DEFAULT_SPEED_MODE)),
        edit_if_callback=use_edit,
    )
    if not allowed:
        return

    if context.user_data.get("cleanup_running"):
        warning = (
            "🚫 <b>عملية حذف جارية حالياً</b>\n"
            "━━━━━━━━━━━━━━\n\n"
            "لا يمكن بدء صيد جديد الآن لأن مهمة تنظيف سابقة ما زالت تعمل.\n"
            "يرجى الانتظار حتى اكتمالها أو إيقافها أولاً."
        )
        if use_edit and update.callback_query is not None:
            await update.callback_query.edit_message_text(
                warning,
                parse_mode=ParseMode.HTML,
                reply_markup=build_dashboard_keyboard(getattr(user_record, "speed_mode", DEFAULT_SPEED_MODE)),
            )
        else:
            await message.reply_text(
                warning,
                parse_mode=ParseMode.HTML,
                reply_markup=build_dashboard_keyboard(getattr(user_record, "speed_mode", DEFAULT_SPEED_MODE)),
            )
        return

    status_message = None
    loading_text = (
        "🔎 <b>جارٍ فحص المتابعين والحسابات التي تتابعها...</b>\n"
        "━━━━━━━━━━━━━━\n\n"
        "• يتم الآن جلب القوائم وتحليل الحسابات الوهمية.\n"
        "• سيتم تجهيز معاينة حديثة قبل أي حذف."
    )
    if use_edit and update.callback_query is not None:
        status_message = update.callback_query.message
        await update.callback_query.edit_message_text(loading_text, parse_mode=ParseMode.HTML)
    else:
        status_message = await message.reply_text(loading_text, parse_mode=ParseMode.HTML)

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
                await status_message.edit_text(SESSION_EXPIRED_MESSAGE, parse_mode=ParseMode.HTML)
                return
            await status_message.edit_text(
                "❌ <b>فشل جلب بيانات Instagram</b>\n\nيرجى المحاولة لاحقاً.",
                parse_mode=ParseMode.HTML,
            )
            return
        except Exception as exc:
            logger.exception("Unexpected error during Instagram fetch: %s", exc)
            await status_message.edit_text(
                "❌ <b>حدث خطأ غير متوقع أثناء الفحص</b>\n\nيرجى المحاولة لاحقاً.",
                parse_mode=ParseMode.HTML,
            )
            return

    if not isinstance(following, dict) or not isinstance(followers, set):
        await status_message.edit_text(
            "❌ <b>وصلت استجابة غير متوقعة من Instagram</b>",
            parse_mode=ParseMode.HTML,
        )
        return

    ghost_ids = sorted(set(following.keys()) - followers)
    if not ghost_ids:
        try:
            user_record.last_run = dt.datetime.now(dt.timezone.utc).isoformat()
            user_record.is_hunting = "false"
            await persist_user_record(user_record)
        except Exception as exc:
            logger.exception("Failed to update last_run after empty hunt: %s", exc)
        await status_message.edit_text(
            "✅ <b>لا توجد حسابات وهمية حالياً</b>\n"
            "━━━━━━━━━━━━━━\n\n"
            "كل من تتابعهم يتابعونك أيضاً، لذلك لا يوجد شيء للحذف الآن.",
            parse_mode=ParseMode.HTML,
            reply_markup=build_dashboard_keyboard(getattr(user_record, "speed_mode", DEFAULT_SPEED_MODE)),
        )
        return

    ghosts: list[dict[str, str]] = []
    whitelist_ids = parse_whitelist_csv(getattr(user_record, "whitelist", ""))
    auto_protected_ids: set[str] = set()
    auto_protected_lines: list[str] = []

    for ghost_id in ghost_ids:
        ghost_info = following.get(ghost_id, {})
        username = str(ghost_info.get("username", ghost_id)).strip() or ghost_id
        is_verified = str(ghost_info.get("is_verified", "false")).strip().lower()
        follower_count = int(str(ghost_info.get("follower_count", "0")).strip() or 0)
        auto_protected = False

        if is_verified == "true" or follower_count >= VERIFIED_AUTO_PROTECT_FOLLOWERS_THRESHOLD:
            whitelist_ids.add(ghost_id)
            auto_protected_ids.add(ghost_id)
            auto_protected = True
            reason = "✔️ موثق" if is_verified == "true" else f"👥 {follower_count:,} متابع"
            auto_protected_lines.append(f"• @{escape_html(username)} — {reason}")

        ghosts.append(
            {
                "id": ghost_id,
                "username": username,
                "is_verified": is_verified,
                "follower_count": str(follower_count),
                "auto_protected": "true" if auto_protected else "false",
            }
        )

    speed_mode = getattr(user_record, "speed_mode", DEFAULT_SPEED_MODE) or DEFAULT_SPEED_MODE
    session = {
        "ghosts": ghosts,
        "whitelist_ids": whitelist_ids,
        "page": 0,
        "started_at": time.perf_counter(),
        "speed_mode": speed_mode,
        "auto_protected_count": len(auto_protected_ids),
        "stop_requested": False,
        "active_message_id": getattr(status_message, "message_id", None),
        "chat_id": telegram_user.id,
        "removed_count": 0,
        "processed_count": 0,
        "total_ghosts": len(ghosts),
    }
    context.user_data[HUNT_SESSION_KEY] = session

    preview_text = format_ghost_preview_text(session)
    if auto_protected_lines:
        preview_text += (
            "\n\n🛡️ <b>تمت إضافة حسابات قوية إلى القائمة البيضاء المؤقتة تلقائياً:</b>\n"
            + "\n".join(auto_protected_lines[:8])
        )
        if len(auto_protected_lines) > 8:
            preview_text += f"\n• ... و <b>{len(auto_protected_lines) - 8}</b> حسابات أخرى"

    await status_message.edit_text(
        preview_text,
        parse_mode=ParseMode.HTML,
        reply_markup=build_hunt_preview_keyboard(session),
    )


async def hunt_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /hunt: open preview and whitelist flow before cleanup."""
    await start_hunt_preview(update, context, use_edit=False)


async def execute_cleanup(
    context: ContextTypes.DEFAULT_TYPE,
    query_message: Any,
    telegram_id: int,
    user_record: UserRecord,
    session: dict[str, Any],
) -> tuple[int, int, int, float, bool, bool]:
    """Execute cleanup and update a single status message."""
    del telegram_id

    ghost_entries: list[dict[str, str]] = session["ghosts"]
    whitelist_ids: set[str] = set(session["whitelist_ids"])
    removable = [ghost for ghost in ghost_entries if ghost["id"] not in whitelist_ids]
    skipped_count = len(ghost_entries) - len(removable)
    removed_count = 0
    session_expired = False
    stopped_by_user = False
    cleanup_started_at = time.perf_counter()
    speed_mode = session.get("speed_mode", DEFAULT_SPEED_MODE)
    delay_min, delay_max = get_speed_mode_config(speed_mode)["delay_range"]
    context.user_data["cleanup_running"] = True

    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        total = len(removable)
        for index, ghost in enumerate(removable, start=1):
            current_session = context.user_data.get(HUNT_SESSION_KEY, {})
            if isinstance(current_session, dict) and current_session.get("stop_requested"):
                stopped_by_user = True
                break

            username = ghost["username"]
            ghost_id = ghost["id"]
            progress_bar = build_progress_bar(index - 1, total)
            estimated_remaining_accounts = max(0, total - index + 1)
            estimated_remaining_seconds = estimate_cleanup_seconds(estimated_remaining_accounts, speed_mode)

            if isinstance(current_session, dict):
                current_session["processed_count"] = index - 1
                current_session["removed_count"] = removed_count
                context.user_data[HUNT_SESSION_KEY] = current_session

            await query_message.edit_text(
                format_progress_text(
                    progress_bar=progress_bar,
                    current_index=index,
                    total=total,
                    username=username,
                    speed_mode=speed_mode,
                    estimated_remaining_seconds=estimated_remaining_seconds,
                    removed_count=removed_count,
                    skipped_count=skipped_count,
                ),
                parse_mode=ParseMode.HTML,
                reply_markup=build_stop_keyboard(),
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
                current_session = context.user_data.get(HUNT_SESSION_KEY, {})
                if isinstance(current_session, dict):
                    current_session["processed_count"] = index
                    current_session["removed_count"] = removed_count
                    context.user_data[HUNT_SESSION_KEY] = current_session

                if index < total:
                    wait_seconds = random.uniform(delay_min, delay_max)
                    end_wait = time.perf_counter() + wait_seconds
                    while time.perf_counter() < end_wait:
                        current_session = context.user_data.get(HUNT_SESSION_KEY, {})
                        if isinstance(current_session, dict) and current_session.get("stop_requested"):
                            stopped_by_user = True
                            break
                        await asyncio.sleep(1)
                    if stopped_by_user:
                        break

        if total > 0 and not session_expired and not stopped_by_user:
            await query_message.edit_text(
                "🧹 <b>اللمسات الأخيرة...</b>\n"
                "━━━━━━━━━━━━━━\n\n"
                f"📊 التقدّم: <code>{escape_html(build_progress_bar(total, total))}</code>\n"
                "💾 يتم الآن حفظ التقرير وتحديث البيانات النهائية.",
                parse_mode=ParseMode.HTML,
                reply_markup=build_stop_keyboard(),
            )

    elapsed = time.perf_counter() - cleanup_started_at

    try:
        user_record.last_run = dt.datetime.now(dt.timezone.utc).isoformat()
        user_record.whitelist = build_whitelist_csv(whitelist_ids)
        user_record.is_hunting = "false"
        await persist_user_record(user_record)
    except Exception as exc:
        logger.exception("Failed to update post-hunt metadata: %s", exc)
    finally:
        context.user_data["cleanup_running"] = False
        context.user_data.pop(HUNT_SESSION_KEY, None)

    return removed_count, skipped_count, len(ghost_entries), elapsed, session_expired, stopped_by_user


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
        await query.edit_message_text(
            "⚠️ <b>تعذر الوصول لقاعدة البيانات</b>\n\nيرجى المحاولة مرة أخرى بعد قليل.",
            parse_mode=ParseMode.HTML,
        )
        return

    if data == "dashboard:home":
        await send_or_edit_dashboard(update, context, user_record=user_record)
        return

    if data == "dashboard:link":
        await query.edit_message_text(
            format_link_instructions(str(telegram_user.id)),
            parse_mode=ParseMode.HTML,
            reply_markup=build_dashboard_keyboard(getattr(user_record, "speed_mode", DEFAULT_SPEED_MODE) if user_record else DEFAULT_SPEED_MODE),
        )
        return

    if data == "dashboard:whitelist":
        session = context.user_data.get(HUNT_SESSION_KEY)
        await query.edit_message_text(
            format_whitelist_text(user_record, session if isinstance(session, dict) else None),
            parse_mode=ParseMode.HTML,
            reply_markup=build_dashboard_keyboard(getattr(user_record, "speed_mode", DEFAULT_SPEED_MODE) if user_record else DEFAULT_SPEED_MODE),
        )
        return

    if data == "dashboard:stats":
        await query.edit_message_text(
            format_stats_text(user_record),
            parse_mode=ParseMode.HTML,
            reply_markup=build_dashboard_keyboard(getattr(user_record, "speed_mode", DEFAULT_SPEED_MODE) if user_record else DEFAULT_SPEED_MODE),
        )
        return

    if data.startswith("speed:set:"):
        if user_record is None:
            await query.edit_message_text(
                "🔗 <b>أنت غير مسجل بعد</b>\n\nاضغط <b>🔗 ربط الحساب</b> أولاً.",
                parse_mode=ParseMode.HTML,
                reply_markup=build_dashboard_keyboard(),
            )
            return

        if bool_from_sheet_flag(getattr(user_record, "is_hunting", "false")):
            await query.edit_message_text(
                format_active_hunt_warning(user_record),
                parse_mode=ParseMode.HTML,
                reply_markup=build_dashboard_keyboard(getattr(user_record, "speed_mode", DEFAULT_SPEED_MODE)),
            )
            return

        requested_mode = data.split(":")[-1].strip().lower()
        if requested_mode not in SPEED_MODE_CONFIG:
            requested_mode = DEFAULT_SPEED_MODE

        user_record.speed_mode = requested_mode
        try:
            await persist_user_record(user_record)
        except Exception as exc:
            logger.exception("Failed to persist speed mode: %s", exc)
            await query.edit_message_text(
                "⚠️ <b>تعذر حفظ وضع السرعة</b>\n\nيرجى المحاولة مرة أخرى.",
                parse_mode=ParseMode.HTML,
                reply_markup=build_dashboard_keyboard(getattr(user_record, "speed_mode", DEFAULT_SPEED_MODE)),
            )
            return

        await query.edit_message_text(
            "✅ <b>تم تحديث وضع السرعة</b>\n"
            "━━━━━━━━━━━━━━\n\n"
            f"⚙️ الوضع الحالي: <b>{escape_html(get_speed_mode_config(requested_mode)['label'])}</b>\n"
            f"💡 الوصف: <i>{escape_html(get_speed_mode_config(requested_mode)['description'])}</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=build_dashboard_keyboard(requested_mode),
        )
        return

    if data == "dashboard:hunt":
        await start_hunt_preview(update, context, use_edit=True)
        return

    session = context.user_data.get(HUNT_SESSION_KEY)
    if not isinstance(session, dict):
        await query.edit_message_text(
            "⌛ <b>انتهت جلسة المعاينة</b>\n\nاضغط <b>🚀 بدء الصيد</b> لبدء جلسة جديدة.",
            parse_mode=ParseMode.HTML,
            reply_markup=build_dashboard_keyboard(getattr(user_record, "speed_mode", DEFAULT_SPEED_MODE) if user_record else DEFAULT_SPEED_MODE),
        )
        return

    if data == "hunt:stop":
        session["stop_requested"] = True
        context.user_data[HUNT_SESSION_KEY] = session
        removed_so_far = int(session.get("removed_count", 0) or 0)
        total_ghosts = int(session.get("total_ghosts", len(session.get("ghosts", []))) or 0)
        skipped_count = len(session.get("ghosts", [])) - len(
            [ghost for ghost in session.get("ghosts", []) if ghost.get("id") not in set(session.get("whitelist_ids", set()))]
        )
        await query.edit_message_text(
            "🛑 <b>تم استلام طلب الإيقاف بنجاح</b>\n"
            "━━━━━━━━━━━━━━\n\n"
            f"🗂️ النتائج المحفوظة حتى الآن: <b>{removed_so_far}</b> حذف فعلي\n"
            f"📌 إجمالي العناصر في الجلسة: <b>{total_ghosts}</b>\n"
            f"✅ العناصر المستثناة: <b>{skipped_count}</b>\n\n"
            "سيتم إنهاء الحلقة الحالية بأمان ثم حفظ التقرير النهائي دون فقدان ما تم إنجازه.",
            parse_mode=ParseMode.HTML,
        )
        return

    if data.startswith("hunt:toggle:"):
        if bool_from_sheet_flag(getattr(user_record, "is_hunting", "false")):
            await query.edit_message_text(
                format_active_hunt_warning(user_record),
                parse_mode=ParseMode.HTML,
                reply_markup=build_dashboard_keyboard(getattr(user_record, "speed_mode", DEFAULT_SPEED_MODE)),
            )
            return

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
        if bool_from_sheet_flag(getattr(user_record, "is_hunting", "false")):
            await query.edit_message_text(
                format_active_hunt_warning(user_record),
                parse_mode=ParseMode.HTML,
                reply_markup=build_dashboard_keyboard(getattr(user_record, "speed_mode", DEFAULT_SPEED_MODE)),
            )
            return

        await query.edit_message_text(
            format_ghost_preview_text(session),
            parse_mode=ParseMode.HTML,
            reply_markup=build_hunt_preview_keyboard(session),
        )
        return

    if data == "hunt:confirm":
        if user_record is None:
            await query.edit_message_text(
                "🔗 <b>أنت غير مسجل بعد</b>\n\nاضغط <b>🔗 ربط الحساب</b> أولاً.",
                parse_mode=ParseMode.HTML,
                reply_markup=build_dashboard_keyboard(),
            )
            return

        latest_allowed, latest_record = await ensure_user_not_hunting(
            telegram_user.id,
            user_record=user_record,
        )
        if not latest_allowed:
            await query.edit_message_text(
                format_active_hunt_warning(latest_record or user_record),
                parse_mode=ParseMode.HTML,
                reply_markup=build_dashboard_keyboard(getattr(user_record, "speed_mode", DEFAULT_SPEED_MODE)),
            )
            return
        if latest_record is not None:
            user_record = latest_record

        whitelist_ids = set(session["whitelist_ids"])
        user_record.whitelist = build_whitelist_csv(whitelist_ids)
        user_record.is_hunting = "true"
        session["speed_mode"] = getattr(user_record, "speed_mode", DEFAULT_SPEED_MODE) or DEFAULT_SPEED_MODE
        context.user_data[HUNT_SESSION_KEY] = session

        try:
            await persist_user_record(user_record)
        except Exception as exc:
            logger.exception("Failed to persist whitelist before cleanup: %s", exc)

        removable_count = len([ghost for ghost in session["ghosts"] if ghost["id"] not in whitelist_ids])
        if removable_count == 0:
            user_record.is_hunting = "false"
            try:
                await persist_user_record(user_record)
            except Exception as exc:
                logger.exception("Failed to clear is_hunting after zero-removable result: %s", exc)
            await query.edit_message_text(
                "📋 <b>جميع الحسابات الوهمية ضمن قائمة الاستثناءات</b>\n"
                "━━━━━━━━━━━━━━\n\n"
                "لن يتم حذف أي حساب حالياً. يمكنك إزالة بعض الحسابات من الاستثناءات ثم إعادة المحاولة.",
                parse_mode=ParseMode.HTML,
                reply_markup=build_dashboard_keyboard(getattr(user_record, "speed_mode", DEFAULT_SPEED_MODE)),
            )
            return

        await query.edit_message_text(
            format_pre_hunt_report(session),
            parse_mode=ParseMode.HTML,
            reply_markup=build_stop_keyboard(),
        )

        removed_count, skipped_count, total_ghosts, elapsed, session_expired, stopped_by_user = await execute_cleanup(
            context,
            query.message,
            telegram_user.id,
            user_record,
            session,
        )

        if session_expired:
            await query.message.edit_text(
                SESSION_EXPIRED_MESSAGE,
                parse_mode=ParseMode.HTML,
                reply_markup=build_dashboard_keyboard(getattr(user_record, "speed_mode", DEFAULT_SPEED_MODE)),
            )
            return

        started_at = float(session.get("started_at", time.perf_counter()))
        total_session_elapsed = max(elapsed, time.perf_counter() - started_at)
        time_saved_seconds = skipped_count * 60
        finish_title = (
            "🛑 <b>تم إيقاف العملية وحفظ النتائج الحالية</b>"
            if stopped_by_user
            else "✅ <b>تم الانتهاء من عملية الصيد</b>"
        )

        await query.message.edit_text(
            f"{finish_title}\n\n"
            f"{'تم حفظ كل ما تم حذفه قبل الإيقاف ويمكنك مراجعة التقرير النهائي بالأسفل.' if stopped_by_user else 'تم حفظ النتائج وتحديث الحالة بنجاح.'}",
            parse_mode=ParseMode.HTML,
            reply_markup=build_dashboard_keyboard(getattr(user_record, "speed_mode", DEFAULT_SPEED_MODE)),
        )
        await query.message.reply_text(
            format_final_report(
                total_ghosts=total_ghosts,
                removed_count=removed_count,
                skipped_count=skipped_count,
                auto_protected_count=int(session.get("auto_protected_count", 0)),
                time_saved_seconds=time_saved_seconds,
                speed_mode=session.get("speed_mode", DEFAULT_SPEED_MODE),
                total_session_elapsed=total_session_elapsed,
                stopped_by_user=stopped_by_user,
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=build_dashboard_keyboard(getattr(user_record, "speed_mode", DEFAULT_SPEED_MODE)),
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

    await message.reply_text(
        "🔓 <b>جارٍ فك تشفير LOGIN_DATA...</b>",
        parse_mode=ParseMode.HTML,
    )

    encoded_payload = text[len("LOGIN_DATA:") :].strip()
    if not encoded_payload:
        await message.reply_text(
            "❌ <b>بيانات LOGIN_DATA غير صالحة</b>",
            parse_mode=ParseMode.HTML,
        )
        return

    try:
        decoded_payload = base64.b64decode(encoded_payload).decode("utf-8")
        payload = json.loads(decoded_payload)
    except Exception as exc:
        logger.warning("Failed to decode LOGIN_DATA payload: %s", exc)
        await message.reply_text(
            "❌ <b>تعذر قراءة LOGIN_DATA</b>\n\nتحقق من النص ثم أعد المحاولة.",
            parse_mode=ParseMode.HTML,
        )
        return

    cookie = str(payload.get("cookie", "")).strip()
    user_agent = str(payload.get("ua", "")).strip()
    telegram_id = str(payload.get("t_id", "")).strip()

    if not cookie or not user_agent or not telegram_id:
        await message.reply_text(
            "❌ <b>بيانات LOGIN_DATA ناقصة</b>",
            parse_mode=ParseMode.HTML,
        )
        return

    await message.reply_text(
        "🔎 <b>جارٍ التحقق من Instagram...</b>",
        parse_mode=ParseMode.HTML,
    )

    try:
        insta_id = await fetch_instagram_user_id(cookie=cookie, user_agent=user_agent)
    except Exception as exc:
        status_code = getattr(exc, "status_code", None)
        logger.warning("Manual link Instagram verification failed: %s", exc)
        if status_code in (HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN):
            await message.reply_text(SESSION_EXPIRED_MESSAGE, parse_mode=ParseMode.HTML)
            return
        await message.reply_text(
            "❌ <b>فشل التحقق من بيانات Instagram</b>",
            parse_mode=ParseMode.HTML,
        )
        return

    await message.reply_text(
        "💾 <b>جارٍ الحفظ في Google Sheets...</b>",
        parse_mode=ParseMode.HTML,
    )

    try:
        await async_db_call(
            "upsert_user",
            telegram_id=telegram_id,
            insta_id=insta_id,
            cookie=cookie,
            user_agent=user_agent,
            last_run=None,
            speed_mode=DEFAULT_SPEED_MODE,
            is_hunting="false",
        )
    except Exception as exc:
        logger.exception("Failed to store manual LOGIN_DATA registration: %s", exc)
        await message.reply_text(
            "⚠️ <b>تعذر حفظ الربط حالياً</b>\n\nيرجى المحاولة مرة أخرى بعد قليل.",
            parse_mode=ParseMode.HTML,
        )
        return

    await message.reply_text(
        "✅ <b>تم ربط الحساب بنجاح عبر اللصق اليدوي</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=build_dashboard_keyboard(DEFAULT_SPEED_MODE),
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
