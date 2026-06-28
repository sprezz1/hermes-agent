from __future__ import annotations

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
