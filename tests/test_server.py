from __future__ import annotations

import asyncio
import logging

import pytest
from histserv.server import Server, ServerOptions


def test_ephemeral_port_is_reported() -> None:
    async def run() -> None:
        server = Server(options=ServerOptions(port=0))
        await server.start()
        try:
            assert server.port != 0
            assert server.address == f"[::]:{server.port}"
        finally:
            await server.stop(grace=0)

    asyncio.run(run())


@pytest.mark.parametrize(
    "error", [ModuleNotFoundError("uvicorn"), RuntimeError("startup failed")]
)
def test_dashboard_failure_is_logged(monkeypatch, caplog, error: Exception) -> None:
    async def fail(self, port: int) -> None:
        raise error

    monkeypatch.setattr(Server, "_start_dashboard", fail)

    async def run() -> None:
        server = Server(options=ServerOptions(port=0, dashboard_port=8789))
        await server.start()
        try:
            dashboard = next(
                task for task in server._callbacks if task.get_name() == "dashboard"
            )
            await asyncio.wait([dashboard])
            # The task's done callback runs on the next event-loop turn.
            await asyncio.sleep(0)
        finally:
            await server.stop(grace=0)

    with caplog.at_level(logging.ERROR, logger="histserv.server"):
        asyncio.run(run())

    failures = [
        record for record in caplog.records if record.message == "Dashboard task failed"
    ]
    assert len(failures) == 1
    assert failures[0].exc_info is not None
    assert failures[0].exc_info[1] is error


@pytest.mark.parametrize("complete", [False, True])
def test_dashboard_normal_completion_and_cancellation_are_not_logged(
    monkeypatch, caplog, complete: bool
) -> None:
    async def dashboard(self, port: int) -> None:
        if not complete:
            await asyncio.Future()

    monkeypatch.setattr(Server, "_start_dashboard", dashboard)

    async def run() -> None:
        server = Server(options=ServerOptions(port=0, dashboard_port=8789))
        await server.start()
        await asyncio.sleep(0)
        await server.stop(grace=0)

    with caplog.at_level(logging.ERROR, logger="histserv.server"):
        asyncio.run(run())

    assert not [record for record in caplog.records if record.name == "histserv.server"]


def test_explicit_port_is_kept() -> None:
    async def run() -> None:
        server = Server(options=ServerOptions(port=0))
        await server.start()
        try:
            # Grab the port the first server got, release it, and rebind
            # explicitly to show port/address reflect the requested port.
            port = server.port
        finally:
            await server.stop(grace=0)

        explicit = Server(options=ServerOptions(port=port))
        await explicit.start()
        try:
            assert explicit.port == port
            assert explicit.address == f"[::]:{port}"
        finally:
            await explicit.stop(grace=0)

    asyncio.run(run())
