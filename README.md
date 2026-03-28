# InstaPro

Python-based Telegram bot and FastAPI webhook for identifying and removing Instagram "ghosts" — accounts you follow that do not follow you back.

## Project Structure

- `requirements.txt` - Python dependencies
- `db_manager.py` - Google Sheets persistence layer
- `api.py` - FastAPI registration webhook
- `bot.py` - Telegram bot logic and ghost-hunting workflow
- `.env.example` - Environment variable template

## Features

- Zero-password registration via JavaScript bookmarklet
- FastAPI `/register` endpoint for session intake
- Google Sheets storage with upsert support
- Telegram notifications for successful registration and each removal
- Instagram session reuse with stored mobile `User-Agent`
- CSRF token extraction from cookie string for POST requests
- Session expiry handling for `401`/`403` responses
- Randomized delay between unfollow actions to reduce automation risk

## Google Sheet Schema

The first worksheet must use these exact headers in this exact order:

- `telegram_id`
- `insta_id`
- `cookie`
- `user_agent`
- `last_run`

`db_manager.py` will auto-create the header row if the sheet is blank.

## Environment Variables

Copy `.env.example` to `.env` and fill in the values:

- `TELEGRAM_BOT_TOKEN` - Telegram bot token
- `WEBHOOK_URL` - Public base URL where the FastAPI app is hosted
- `REGISTER_API_KEY` - Optional shared secret required by `/register`
- `GOOGLE_SHEETS_CREDENTIALS_FILE` - Path to your Google service account JSON file
- `GOOGLE_SHEET_NAME` - Name of the Google Sheet to use

## Installation

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Running the FastAPI Webhook

```bash
uvicorn api:app --host 0.0.0.0 --port 8000
```

## Running the Telegram Bot

```bash
python bot.py
```

## Registration Flow

1. User sends `/register` to the Telegram bot.
2. The bot returns a JavaScript bookmarklet.
3. The user saves it as a mobile browser bookmark URL.
4. While logged into Instagram in that browser, the user taps the bookmark.
5. The bookmarklet sends:

```json
{
  "cookie": "...",
  "ua": "...",
  "t_id": "..."
}
```

to:

```text
{WEBHOOK_URL}/register
```

6. The FastAPI backend validates the Instagram session using:
   - `https://www.instagram.com/api/v1/accounts/current_user/`
7. The backend extracts the Instagram `pk`, upserts the record into Google Sheets, and sends:
   - `✅ Registration Successful!`

## Ghost Hunt Flow

When the user sends `/hunt`:

1. The bot loads the user's session from Google Sheets using `telegram_id`.
2. It fetches:
   - Following: `/api/v1/friendships/{insta_id}/following/`
   - Followers: `/api/v1/friendships/{insta_id}/followers/`
3. It compares them with Python sets:

```python
ghosts = following_ids - follower_ids
```

4. For each ghost, it calls:

```text
POST /api/v1/friendships/destroy/{ghost_id}/
```

5. After each successful removal, the bot sends:

```text
Ghost @username has been removed 💀
```

6. It waits a random delay between 30 and 60 seconds before the next removal.

## Safety Logic

- Every Instagram request uses the stored per-user `User-Agent`
- Every Instagram POST includes `X-CSRFToken` extracted from the cookie string
- If Instagram returns `401` or `403`, the user is notified with:

```text
Session expired. Please re-register using the magic link.
```

## Notes

- This project depends on Instagram private endpoints, which may change without notice.
- Mobile browser bookmarklets can behave differently depending on the browser.
- The cookie captured by `document.cookie` must include the needed Instagram session values for the flow to work.
- Use responsibly and ensure compliance with all platform terms and applicable laws.
