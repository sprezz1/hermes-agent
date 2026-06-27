"""Zulip platform adapter for Hermes Agent.

Uses Zulip's HTTP API directly through httpx. This avoids a hard dependency on
the `zulip` Python SDK while using the same bot email/API-key credentials.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

try:
    import httpx

    HTTPX_AVAILABLE = True
except ImportError:
    httpx = None  # type: ignore[assignment]
    HTTPX_AVAILABLE = False

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, MessageEvent, MessageType, SendResult

logger = logging.getLogger(__name__)

MAX_MESSAGE_LENGTH = 10000
EVENT_TYPES = ["message"]


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _verify_ssl(extra: dict[str, Any]) -> bool:
    if "verify_ssl" in extra:
        return bool(extra.get("verify_ssl"))
    raw = os.getenv("ZULIP_VERIFY_SSL", "true")
    return str(raw).strip().lower() not in {"0", "false", "no", "off"}


def _site(extra: dict[str, Any]) -> str:
    return (os.getenv("ZULIP_SITE") or extra.get("site") or "").rstrip("/")


def _email(extra: dict[str, Any]) -> str:
    return os.getenv("ZULIP_EMAIL") or extra.get("email") or ""


def _api_key(config: PlatformConfig, extra: dict[str, Any]) -> str:
    return os.getenv("ZULIP_API_KEY") or config.api_key or extra.get("api_key") or ""


def _home_channel(extra: dict[str, Any]) -> str:
    return os.getenv("ZULIP_HOME_CHANNEL") or extra.get("home_channel") or ""


def _home_topic(extra: dict[str, Any]) -> str:
    return os.getenv("ZULIP_HOME_CHANNEL_NAME") or extra.get("home_topic") or "general"


def check_requirements() -> bool:
    return HTTPX_AVAILABLE and bool(os.getenv("ZULIP_SITE") and os.getenv("ZULIP_EMAIL") and os.getenv("ZULIP_API_KEY"))


def validate_config(config: PlatformConfig) -> bool:
    extra = config.extra or {}
    return HTTPX_AVAILABLE and bool(_site(extra) and _email(extra) and _api_key(config, extra))


def is_connected(config: PlatformConfig) -> bool:
    return validate_config(config)


def _env_enablement() -> dict | None:
    site = os.getenv("ZULIP_SITE", "").strip().rstrip("/")
    email = os.getenv("ZULIP_EMAIL", "").strip()
    api_key = os.getenv("ZULIP_API_KEY", "").strip()
    if not (site and email and api_key):
        return None

    seed: dict[str, Any] = {
        "site": site,
        "email": email,
        "api_key": api_key,
        "verify_ssl": _verify_ssl({}),
    }
    home = os.getenv("ZULIP_HOME_CHANNEL", "").strip()
    if home:
        seed["home_channel"] = {
            "chat_id": home,
            "name": os.getenv("ZULIP_HOME_CHANNEL_NAME", "general"),
        }
    return seed


def _apply_yaml_config(_yaml_cfg: dict, platform_cfg: dict) -> dict:
    extra = platform_cfg.get("extra") if isinstance(platform_cfg.get("extra"), dict) else {}

    def cfg_value(*names: str) -> Any:
        for name in names:
            if name in platform_cfg:
                return platform_cfg[name]
            if name in extra:
                return extra[name]
        return None

    site = cfg_value("site")
    email = cfg_value("email")
    api_key = cfg_value("api_key")
    home = cfg_value("home_channel")
    topic = cfg_value("home_topic", "topic")
    verify_ssl = cfg_value("verify_ssl")

    if site and not os.getenv("ZULIP_SITE"):
        os.environ["ZULIP_SITE"] = str(site)
    if email and not os.getenv("ZULIP_EMAIL"):
        os.environ["ZULIP_EMAIL"] = str(email)
    if api_key and not os.getenv("ZULIP_API_KEY"):
        os.environ["ZULIP_API_KEY"] = str(api_key)
    if home and not os.getenv("ZULIP_HOME_CHANNEL"):
        os.environ["ZULIP_HOME_CHANNEL"] = str(home)
    if topic and not os.getenv("ZULIP_HOME_CHANNEL_NAME"):
        os.environ["ZULIP_HOME_CHANNEL_NAME"] = str(topic)
    if verify_ssl is not None and not os.getenv("ZULIP_VERIFY_SSL"):
        os.environ["ZULIP_VERIFY_SSL"] = "true" if bool(verify_ssl) else "false"

    seeded = _env_enablement() or {}
    if site:
        seeded["site"] = str(site).rstrip("/")
    if email:
        seeded["email"] = str(email)
    if api_key:
        seeded["api_key"] = str(api_key)
    if verify_ssl is not None:
        seeded["verify_ssl"] = bool(verify_ssl)
    return seeded


class ZulipAdapter(BasePlatformAdapter):
    MAX_MESSAGE_LENGTH = MAX_MESSAGE_LENGTH

    def __init__(self, config: PlatformConfig):
        super().__init__(config=config, platform=Platform("zulip"))
        self._extra = config.extra or {}
        self._site = _site(self._extra)
        self._email = _email(self._extra)
        self._api_key = _api_key(config, self._extra)
        self._verify_ssl = _verify_ssl(self._extra)
        self._client: Optional["httpx.AsyncClient"] = None
        self._event_task: Optional[asyncio.Task] = None
        self._queue_id: Optional[str] = None
        self._last_event_id: Optional[int] = None
        self._own_user_id: Optional[int] = None

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        if not HTTPX_AVAILABLE:
            logger.warning("Zulip: httpx is not installed")
            return False
        if not (self._site and self._email and self._api_key):
            logger.warning("Zulip: ZULIP_SITE, ZULIP_EMAIL, and ZULIP_API_KEY are required")
            return False

        self._client = httpx.AsyncClient(
            base_url=self._site,
            auth=(self._email, self._api_key),
            verify=self._verify_ssl,
            timeout=httpx.Timeout(connect=15.0, read=90.0, write=15.0, pool=15.0),
        )
        try:
            me = await self._request("GET", "/api/v1/users/me")
            self._own_user_id = int(me.get("user_id"))
            registered = await self._request("POST", "/api/v1/register", data={"event_types": json.dumps(EVENT_TYPES)})
            self._queue_id = registered["queue_id"]
            self._last_event_id = int(registered["last_event_id"])
        except Exception as e:
            logger.error("Zulip: failed to connect: %s", e)
            await self.disconnect()
            return False

        self._mark_connected()
        self._event_task = asyncio.create_task(self._event_loop())
        logger.info("Zulip: connected to %s as %s", self._site, self._email)
        return True

    async def disconnect(self) -> None:
        self._running = False
        self._mark_disconnected()
        if self._event_task:
            self._event_task.cancel()
            try:
                await self._event_task
            except asyncio.CancelledError:
                pass
            self._event_task = None
        if self._client:
            if self._queue_id:
                try:
                    await self._client.delete(
                        "/api/v1/events",
                        params={"queue_id": self._queue_id},
                    )
                except Exception:
                    pass
            await self._client.aclose()
            self._client = None

    async def _request(self, method: str, path: str, **kwargs: Any) -> dict:
        if self._client is None:
            raise RuntimeError("Zulip client is not connected")
        response = await self._client.request(method, path, **kwargs)
        response.raise_for_status()
        data = response.json()
        if data.get("result") != "success":
            raise RuntimeError(data.get("msg") or data)
        return data

    async def _event_loop(self) -> None:
        while self._running and self._queue_id is not None and self._last_event_id is not None:
            try:
                data = await self._request(
                    "GET",
                    "/api/v1/events",
                    params={
                        "queue_id": self._queue_id,
                        "last_event_id": self._last_event_id,
                        "dont_block": "false",
                    },
                )
                for event in data.get("events", []):
                    self._last_event_id = int(event.get("id", self._last_event_id))
                    await self._handle_event(event)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("Zulip: event loop error: %s", e)
                await asyncio.sleep(5)

    async def _handle_event(self, event: dict[str, Any]) -> None:
        if event.get("type") != "message":
            return
        message = event.get("message") or {}
        if message.get("sender_id") == self._own_user_id:
            return
        content = (message.get("content") or "").strip()
        if not content:
            return

        msg_type = message.get("type")
        sender_email = message.get("sender_email") or str(message.get("sender_id", ""))
        sender_name = message.get("sender_full_name") or sender_email
        topic = message.get("subject") or message.get("topic") or "general"

        if msg_type == "private":
            chat_id = f"dm:{message.get('sender_id')}"
            chat_name = sender_name
            chat_type = "dm"
        else:
            stream_id = message.get("stream_id")
            chat_id = str(stream_id or message.get("display_recipient") or "")
            chat_name = str(message.get("display_recipient") or chat_id)
            chat_type = "group"

        source = self.build_source(
            chat_id=chat_id,
            chat_name=chat_name,
            chat_type=chat_type,
            user_id=sender_email,
            user_name=sender_name,
            thread_id=str(topic) if topic else None,
        )
        gateway_event = MessageEvent(
            text=content,
            message_type=MessageType.TEXT,
            source=source,
            message_id=str(message.get("id") or int(time.time() * 1000)),
            timestamp=datetime.fromtimestamp(message.get("timestamp", time.time()), tz=timezone.utc),
        )
        await self.handle_message(gateway_event)

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        try:
            result = await _send_with_config(
                self.config,
                chat_id,
                content,
                thread_id=(metadata or {}).get("thread_id") if metadata else None,
            )
            if result.get("success"):
                return SendResult(success=True, message_id=str(result.get("message_id", "")))
            return SendResult(success=False, error=str(result.get("error", "Zulip send failed")))
        except Exception as e:
            return SendResult(success=False, error=str(e))

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        return None

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        return {"name": chat_id, "type": "dm" if str(chat_id).startswith("dm:") else "group"}


async def _send_with_config(
    pconfig: PlatformConfig,
    chat_id: str,
    message: str,
    *,
    thread_id: Optional[str] = None,
) -> dict[str, Any]:
    extra = pconfig.extra or {}
    site = _site(extra)
    email = _email(extra)
    api_key = _api_key(pconfig, extra)
    verify_ssl = _verify_ssl(extra)
    if not (site and email and api_key):
        return {"error": "Zulip send: ZULIP_SITE, ZULIP_EMAIL, and ZULIP_API_KEY are required"}

    target = chat_id or _home_channel(extra)
    if not target:
        return {"error": "Zulip send: no target chat_id or ZULIP_HOME_CHANNEL configured"}

    payload: dict[str, Any]
    if str(target).startswith("dm:"):
        recipient = str(target)[3:]
        if recipient.isdigit():
            to_value = json.dumps([int(recipient)])
        else:
            to_value = json.dumps([recipient])
        payload = {
            "type": "private",
            "to": to_value,
            "content": message,
        }
    else:
        payload = {
            "type": "stream",
            "to": str(target),
            "topic": thread_id or _home_topic(extra),
            "content": message,
        }

    try:
        async with httpx.AsyncClient(
            base_url=site,
            auth=(email, api_key),
            verify=verify_ssl,
            timeout=15.0,
        ) as client:
            response = await client.post("/api/v1/messages", data=payload)
        if response.status_code >= 300:
            return {"error": f"Zulip HTTP {response.status_code}: {response.text[:200]}"}
        data = response.json()
        if data.get("result") != "success":
            return {"error": data.get("msg") or str(data)}
        return {"success": True, "platform": "zulip", "chat_id": target, "message_id": data.get("id")}
    except Exception as e:
        return {"error": f"Zulip send failed: {e}"}


async def _standalone_send(
    pconfig: PlatformConfig,
    chat_id: str,
    message: str,
    *,
    thread_id: Optional[str] = None,
    media_files: Optional[list[str]] = None,
    force_document: bool = False,
) -> dict[str, Any]:
    return await _send_with_config(pconfig, chat_id, message, thread_id=thread_id)


def register(ctx) -> None:
    ctx.register_platform(
        name="zulip",
        label="Zulip",
        adapter_factory=lambda cfg: ZulipAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=["ZULIP_SITE", "ZULIP_EMAIL", "ZULIP_API_KEY"],
        install_hint="httpx is required and already included with Hermes",
        env_enablement_fn=_env_enablement,
        apply_yaml_config_fn=_apply_yaml_config,
        cron_deliver_env_var="ZULIP_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        allowed_users_env="ZULIP_ALLOWED_USERS",
        allow_all_env="ZULIP_ALLOW_ALL_USERS",
        max_message_length=MAX_MESSAGE_LENGTH,
        emoji="Z",
        pii_safe=False,
        allow_update_command=True,
        platform_hint=(
            "You are communicating via Zulip. Channel messages are organized "
            "by topic; keep replies relevant to the current topic. Markdown is supported."
        ),
    )
