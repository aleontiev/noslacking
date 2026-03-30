"""Pydantic models for Google entities."""

from __future__ import annotations

from pydantic import BaseModel


class GoogleUser(BaseModel):
    id: str = ""
    primary_email: str
    full_name: str = ""
    is_suspended: bool = False

    @classmethod
    def from_api(cls, data: dict) -> GoogleUser:
        name = data.get("name", {})
        return cls(
            id=data.get("id", ""),
            primary_email=data.get("primaryEmail", ""),
            full_name=name.get("fullName", ""),
            is_suspended=data.get("suspended", False),
        )


class GoogleSpace(BaseModel):
    name: str  # Resource name, e.g., "spaces/ABC123"
    display_name: str = ""
    space_type: str = ""

    @classmethod
    def from_api(cls, data: dict) -> GoogleSpace:
        return cls(
            name=data.get("name", ""),
            display_name=data.get("displayName", ""),
            space_type=data.get("spaceType", ""),
        )
