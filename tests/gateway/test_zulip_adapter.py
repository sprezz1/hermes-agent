from __future__ import annotations

import asyncio
import json

import pytest

from gateway.config import PlatformConfig
from tests.gateway._plugin_adapter_loader import load_plugin_adapter


_zulip = load_plugin_adapter("zulip")


@pytest.mark.asyncio
async def test_edit_message_patches_zulip_content() -> None:
    adapter = _zulip.ZulipAdapter(PlatformConfig(enabled=True, extra={"site": "https://zulip.test"}))
    calls = []

    async def fake_request(method, path, **kwargs):
        calls.append((method, path, kwargs))
        return {"result": "success"}

    adapter._request = fake_request

    result = await adapter.edit_message("8", "123", "updated progress")

    assert result.success is True
    assert result.message_id == "123"
    assert calls == [
        (
            "PATCH",
            "/api/v1/messages/123",
            {"data": {"content": "updated progress"}},
        )
    ]


@pytest.mark.asyncio
async def test_edit_message_requires_message_id() -> None:
    adapter = _zulip.ZulipAdapter(PlatformConfig(enabled=True, extra={"site": "https://zulip.test"}))

    result = await adapter.edit_message("8", "", "updated progress")

    assert result.success is False
    assert "message_id is required" in (result.error or "")


def test_zulip_adapter_supports_markdown_code_blocks() -> None:
    adapter = _zulip.ZulipAdapter(PlatformConfig(enabled=True, extra={"site": "https://zulip.test"}))

    assert adapter.supports_code_blocks is True


@pytest.mark.asyncio
async def test_send_typing_posts_stream_start_and_stop_for_topic() -> None:
    adapter = _zulip.ZulipAdapter(PlatformConfig(enabled=True, extra={"site": "https://zulip.test"}))
    calls = []

    async def fake_request(method, path, **kwargs):
        calls.append((method, path, kwargs))
        return {"result": "success"}

    adapter._request = fake_request

    await adapter.send_typing("8", metadata={"thread_id": "Zulip-Native Astra"})
    await adapter.stop_typing("8")

    assert calls == [
        (
            "POST",
            "/api/v1/typing",
            {"data": {"op": "start", "type": "stream", "stream_id": 8, "topic": "Zulip-Native Astra"}},
        ),
        (
            "POST",
            "/api/v1/typing",
            {"data": {"op": "stop", "type": "stream", "stream_id": 8, "topic": "Zulip-Native Astra"}},
        ),
    ]
    assert adapter._typing_targets == {}


@pytest.mark.asyncio
async def test_send_typing_posts_private_target() -> None:
    adapter = _zulip.ZulipAdapter(PlatformConfig(enabled=True, extra={"site": "https://zulip.test"}))
    calls = []

    async def fake_request(method, path, **kwargs):
        calls.append((method, path, kwargs))
        return {"result": "success"}

    adapter._request = fake_request

    await adapter.send_typing("dm:11")

    assert calls == [
        (
            "POST",
            "/api/v1/typing",
            {"data": {"op": "start", "type": "direct", "to": json.dumps([11])}},
        )
    ]


@pytest.mark.asyncio
async def test_update_presence_posts_active_status() -> None:
    adapter = _zulip.ZulipAdapter(PlatformConfig(enabled=True, extra={"site": "https://zulip.test"}))
    calls = []

    async def fake_request(method, path, **kwargs):
        calls.append((method, path, kwargs))
        return {"result": "success"}

    adapter._request = fake_request

    await adapter._update_presence("active")

    assert calls == [
        (
            "POST",
            "/api/v1/users/me/presence",
            {"data": {"status": "active", "ping_only": "true", "new_user_input": "false"}},
        )
    ]


@pytest.mark.asyncio
async def test_disconnect_cancels_presence_task() -> None:
    class FakeClient:
        async def aclose(self) -> None:
            return None

    adapter = _zulip.ZulipAdapter(PlatformConfig(enabled=True, extra={"site": "https://zulip.test"}))
    task = asyncio.create_task(asyncio.sleep(60))
    adapter._presence_task = task
    adapter._client = FakeClient()

    await adapter.disconnect()

    assert adapter._presence_task is None
    assert task.cancelled()
