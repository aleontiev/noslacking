"""Pydantic models for Slack entities."""

from __future__ import annotations

from pydantic import BaseModel, Field


class SlackUser(BaseModel):
    id: str
    name: str = ""
    real_name: str = ""
    display_name: str = ""
    email: str = ""
    is_bot: bool = False
    is_deleted: bool = False
    is_admin: bool = False
    tz: str = ""

    @classmethod
    def from_api(cls, data: dict) -> SlackUser:
        profile = data.get("profile", {})
        return cls(
            id=data["id"],
            name=data.get("name", ""),
            real_name=data.get("real_name", profile.get("real_name", "")),
            display_name=profile.get("display_name", ""),
            email=profile.get("email", ""),
            is_bot=data.get("is_bot", False),
            is_deleted=data.get("deleted", False),
            is_admin=data.get("is_admin", False),
            tz=data.get("tz", ""),
        )


class SlackChannel(BaseModel):
    id: str
    name: str
    channel_type: str = "public_channel"
    is_archived: bool = False
    topic: str = ""
    purpose: str = ""
    num_members: int = 0

    @classmethod
    def from_api(cls, data: dict) -> SlackChannel:
        # Determine type
        if data.get("is_im"):
            ctype = "im"
        elif data.get("is_mpim"):
            ctype = "mpim"
        elif data.get("is_private"):
            ctype = "private_channel"
        else:
            ctype = "public_channel"

        return cls(
            id=data["id"],
            name=data.get("name", data.get("id", "")),
            channel_type=ctype,
            is_archived=data.get("is_archived", False),
            topic=data.get("topic", {}).get("value", ""),
            purpose=data.get("purpose", {}).get("value", ""),
            num_members=data.get("num_members", 0),
        )


class SlackReaction(BaseModel):
    name: str
    count: int = 0
    users: list[str] = Field(default_factory=list)


class SlackFile(BaseModel):
    id: str
    name: str = ""
    mimetype: str = ""
    size: int = 0
    url_private: str = ""
    url_private_download: str = ""

    @classmethod
    def from_api(cls, data: dict) -> SlackFile:
        return cls(
            id=data["id"],
            name=data.get("name", ""),
            mimetype=data.get("mimetype", ""),
            size=data.get("size", 0),
            url_private=data.get("url_private", ""),
            url_private_download=data.get("url_private_download", ""),
        )


class SlackMessage(BaseModel):
    ts: str
    thread_ts: str | None = None
    user: str = ""
    text: str = ""
    msg_type: str = "message"
    subtype: str | None = None
    reactions: list[SlackReaction] = Field(default_factory=list)
    files: list[SlackFile] = Field(default_factory=list)
    reply_count: int = 0
    raw: dict = Field(default_factory=dict)

    @property
    def is_thread_parent(self) -> bool:
        return self.reply_count > 0 or (
            self.thread_ts is not None and self.thread_ts == self.ts
        )

    @property
    def is_thread_reply(self) -> bool:
        return self.thread_ts is not None and self.thread_ts != self.ts

    @classmethod
    def from_api(cls, data: dict) -> SlackMessage:
        reactions = [
            SlackReaction(name=r["name"], count=r.get("count", 0), users=r.get("users", []))
            for r in data.get("reactions", [])
        ]
        files = [SlackFile.from_api(f) for f in data.get("files", [])]

        return cls(
            ts=data["ts"],
            thread_ts=data.get("thread_ts"),
            user=data.get("user", data.get("bot_id", "")),
            text=data.get("text", ""),
            msg_type=data.get("type", "message"),
            subtype=data.get("subtype"),
            reactions=reactions,
            files=files,
            reply_count=data.get("reply_count", 0),
            raw=data,
        )
