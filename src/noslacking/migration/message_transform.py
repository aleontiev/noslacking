"""Transform Slack message format to Google Chat format."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from noslacking.db.models import User

logger = logging.getLogger(__name__)

# Common Slack emoji name → Unicode mapping
SLACK_EMOJI: dict[str, str] = {
    "smile": "😄", "laughing": "😆", "blush": "😊", "smiley": "😃",
    "relaxed": "☺️", "smirk": "😏", "heart_eyes": "😍", "kissing_heart": "😘",
    "kissing_closed_eyes": "😚", "flushed": "😳", "relieved": "😌",
    "satisfied": "😆", "grin": "😁", "wink": "😉", "stuck_out_tongue_winking_eye": "😜",
    "stuck_out_tongue_closed_eyes": "😝", "grinning": "😀", "kissing": "😗",
    "stuck_out_tongue": "😛", "sleeping": "😴", "worried": "😟",
    "frowning": "😦", "anguished": "😧", "open_mouth": "😮",
    "grimacing": "😬", "confused": "😕", "hushed": "😯", "expressionless": "😑",
    "unamused": "😒", "sweat_smile": "😅", "sweat": "😓",
    "disappointed_relieved": "😥", "weary": "😩", "pensive": "😔",
    "disappointed": "😞", "confounded": "😖", "fearful": "😨",
    "cold_sweat": "😰", "persevere": "😣", "cry": "😢", "sob": "😭",
    "joy": "😂", "astonished": "😲", "scream": "😱",
    "tired_face": "😫", "angry": "😠", "rage": "😡", "triumph": "😤",
    "sleepy": "😪", "yum": "😋", "mask": "😷", "sunglasses": "😎",
    "dizzy_face": "😵", "imp": "👿", "smiling_imp": "😈",
    "neutral_face": "😐", "no_mouth": "😶", "innocent": "😇",
    "alien": "👽", "yellow_heart": "💛", "blue_heart": "💙",
    "purple_heart": "💜", "heart": "❤️", "green_heart": "💚",
    "broken_heart": "💔", "heartbeat": "💓", "heartpulse": "💗",
    "two_hearts": "💕", "revolving_hearts": "💞", "cupid": "💘",
    "sparkling_heart": "💖", "sparkles": "✨", "star": "⭐", "star2": "🌟",
    "dizzy": "💫", "boom": "💥", "collision": "💥", "anger": "💢",
    "exclamation": "❗", "question": "❓", "grey_exclamation": "❕",
    "grey_question": "❔", "zzz": "💤", "dash": "💨", "sweat_drops": "💦",
    "notes": "🎶", "musical_note": "🎵", "fire": "🔥", "poop": "💩",
    "+1": "👍", "thumbsup": "👍", "-1": "👎", "thumbsdown": "👎",
    "ok_hand": "👌", "punch": "👊", "fist": "✊", "v": "✌️",
    "wave": "👋", "hand": "✋", "raised_hand": "✋",
    "open_hands": "👐", "point_up": "☝️", "point_down": "👇",
    "point_left": "👈", "point_right": "👉", "raised_hands": "🙌",
    "pray": "🙏", "point_up_2": "👆", "clap": "👏", "muscle": "💪",
    "walking": "🚶", "runner": "🏃", "running": "🏃",
    "couple": "👫", "family": "👪", "two_men_holding_hands": "👬",
    "two_women_holding_hands": "👭", "dancer": "💃",
    "bow": "🙇", "couplekiss": "💏", "couple_with_heart": "💑",
    "massage": "💆", "haircut": "💇", "nail_care": "💅",
    "boy": "👦", "girl": "👧", "woman": "👩", "man": "👨",
    "baby": "👶", "older_woman": "👵", "older_man": "👴",
    "eyes": "👀", "eye": "👁️", "ear": "👂", "nose": "👃",
    "lips": "👄", "tongue": "👅",
    "100": "💯", "money_with_wings": "💸", "moneybag": "💰",
    "rocket": "🚀", "tada": "🎉", "party_popper": "🎉",
    "thinking_face": "🤔", "thinking": "🤔", "face_with_monocle": "🧐",
    "white_check_mark": "✅", "heavy_check_mark": "✔️",
    "x": "❌", "negative_squared_cross_mark": "❎",
    "warning": "⚠️", "no_entry": "⛔",
    "lock": "🔒", "unlock": "🔓", "key": "🔑",
    "bulb": "💡", "wrench": "🔧", "hammer": "🔨",
    "gear": "⚙️", "link": "🔗", "pushpin": "📌",
    "memo": "📝", "pencil": "✏️", "pencil2": "✏️",
    "book": "📖", "books": "📚", "clipboard": "📋",
    "calendar": "📅", "date": "📅",
    "email": "📧", "envelope": "✉️", "mailbox": "📫",
    "phone": "☎️", "telephone_receiver": "📞",
    "computer": "💻", "desktop_computer": "🖥️",
    "globe_with_meridians": "🌐", "earth_americas": "🌎",
    "sunny": "☀️", "cloud": "☁️", "umbrella": "☂️",
    "snowflake": "❄️", "zap": "⚡",
    "dog": "🐶", "cat": "🐱", "mouse": "🐭",
    "hamster": "🐹", "rabbit": "🐰", "bear": "🐻",
    "pig": "🐷", "cow": "🐮", "chicken": "🐔",
    "monkey_face": "🐵", "see_no_evil": "🙈",
    "hear_no_evil": "🙉", "speak_no_evil": "🙊",
    "coffee": "☕", "beer": "🍺", "beers": "🍻",
    "wine_glass": "🍷", "cocktail": "🍸",
    "pizza": "🍕", "hamburger": "🍔", "fries": "🍟",
    "poultry_leg": "🍗", "rice": "🍚", "sushi": "🍣",
    "apple": "🍎", "green_apple": "🍏", "banana": "🍌",
    "trophy": "🏆", "medal": "🏅", "crown": "👑",
    "gem": "💎", "ribbon": "🎀", "gift": "🎁",
    "christmas_tree": "🎄", "jack_o_lantern": "🎃",
    "ghost": "👻", "skull": "💀",
    "slightly_smiling_face": "🙂", "upside_down_face": "🙃",
    "rolling_on_the_floor_laughing": "🤣", "rofl": "🤣",
    "hugging_face": "🤗", "hugs": "🤗",
    "nerd_face": "🤓", "face_with_rolling_eyes": "🙄",
    "shrug": "🤷", "facepalm": "🤦", "face_palm": "🤦",
    "raised_hand_with_fingers_splayed": "🖐️",
    "middle_finger": "🖕", "crossed_fingers": "🤞",
    "handshake": "🤝", "writing_hand": "✍️",
    "heavy_plus_sign": "➕", "heavy_minus_sign": "➖",
    "heavy_division_sign": "➗", "heavy_multiplication_x": "✖️",
    "arrow_right": "➡️", "arrow_left": "⬅️",
    "arrow_up": "⬆️", "arrow_down": "⬇️",
    "white_large_square": "⬜", "black_large_square": "⬛",
    "red_circle": "🔴", "blue_circle": "🔵",
    "large_green_circle": "🟢", "large_orange_circle": "🟠",
    "rotating_light": "🚨", "bell": "🔔",
    "loudspeaker": "📢", "mega": "📣",
    "speech_balloon": "💬", "thought_balloon": "💭",
    "hourglass": "⌛", "stopwatch": "⏱️",
    "chart_with_upwards_trend": "📈", "chart_with_downwards_trend": "📉",
    "bar_chart": "📊",
}

# Regex for Slack emoji syntax :emoji_name:
_SLACK_EMOJI = re.compile(r":([a-zA-Z0-9_\-+]+):")

# Slack mrkdwn patterns
_USER_MENTION = re.compile(r"<@(U[A-Z0-9]+)(?:\|([^>]+))?>")
_CHANNEL_MENTION = re.compile(r"<#(C[A-Z0-9]+)(?:\|([^>]+))?>")
_URL_PATTERN = re.compile(r"<(https?://[^|>]+)(?:\|([^>]+))?>")
_BOLD = re.compile(r"\*([^*]+)\*")
_ITALIC = re.compile(r"_([^_]+)_")
_STRIKE = re.compile(r"~([^~]+)~")
_CODE_BLOCK = re.compile(r"```(.*?)```", re.DOTALL)
_INLINE_CODE = re.compile(r"`([^`]+)`")


def slack_ts_to_datetime(ts: str) -> datetime:
    """Convert Slack timestamp (e.g., '1234567890.123456') to datetime."""
    return datetime.fromtimestamp(float(ts), tz=timezone.utc)


def transform_message_text(
    text: str,
    session: Session,
    channel_names: dict[str, str] | None = None,
) -> str:
    """Convert Slack mrkdwn message text to Google Chat formatted text.

    Google Chat supports a subset of formatting:
    - Bold: *text*
    - Italic: _text_
    - Strikethrough: ~text~
    - Code: `code` and ```code blocks```
    - Links: <url|text> -> text (url)
    - User mentions: converted to display names
    """
    if not text:
        return ""

    result = text

    # Replace user mentions with Google Chat mention syntax: <users/email>
    def replace_user_mention(match: re.Match) -> str:
        user_id = match.group(1)
        user = session.get(User, user_id)
        if user and user.google_email:
            return f"<users/{user.google_email}>"
        # Fallback to display name if no Google email
        if user:
            name = user.slack_display_name or user.slack_real_name or user_id
            return f"@{name}"
        label = match.group(2)
        return f"@{label}" if label else f"@{user_id}"

    result = _USER_MENTION.sub(replace_user_mention, result)

    # Replace channel mentions
    def replace_channel_mention(match: re.Match) -> str:
        label = match.group(2)
        if label:
            return f"#{label}"
        return f"#{match.group(1)}"

    if channel_names:
        def replace_channel_with_name(match: re.Match) -> str:
            channel_id = match.group(1)
            label = match.group(2)
            name = label or channel_names.get(channel_id, channel_id)
            return f"#{name}"
        result = _CHANNEL_MENTION.sub(replace_channel_with_name, result)
    else:
        result = _CHANNEL_MENTION.sub(replace_channel_mention, result)

    # Replace URLs — Slack wraps them as <url|label>
    def replace_url(match: re.Match) -> str:
        url = match.group(1)
        label = match.group(2)
        if label and label != url:
            return f"{label} ({url})"
        return url

    result = _URL_PATTERN.sub(replace_url, result)

    # Slack special tokens → Google Chat @all mention
    result = result.replace("<!here>", "<users/all>")
    result = result.replace("<!channel>", "<users/all>")
    result = result.replace("<!everyone>", "<users/all>")

    # Convert Slack emoji :name: to Unicode
    def replace_emoji(match: re.Match) -> str:
        name = match.group(1)
        return SLACK_EMOJI.get(name, f":{name}:")  # Keep original if unknown

    result = _SLACK_EMOJI.sub(replace_emoji, result)

    return result


def build_attribution_text(
    original_text: str,
    user_name: str,
    timestamp: datetime,
) -> str:
    """Build message text with attribution for unmapped users."""
    return f"*{user_name}:*\n{original_text}"


def build_file_card(filename: str, url: str | None = None) -> dict:
    """Build a simple card widget for a file attachment reference."""
    widgets = [{"textParagraph": {"text": f"📎 {filename}"}}]
    if url:
        widgets.append({
            "buttonList": {
                "buttons": [{
                    "text": "Open file",
                    "onClick": {"openLink": {"url": url}},
                }]
            }
        })

    return {
        "cardId": f"file-{filename}",
        "card": {
            "sections": [{"widgets": widgets}],
        },
    }
