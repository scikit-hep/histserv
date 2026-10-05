from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

import grpc

from histserv.callbacks import print_hists_stats, prune_old_hists
from histserv.protos import hist_pb2_grpc
from histserv.service import Histogrammer, LoggingInterceptor


@dataclass(frozen=True)
class ServerOptions:
    port: int = 50051
    prune_after: timedelta = timedelta(days=1)
    prune_interval: timedelta = timedelta(minutes=5)
    stats_interval: timedelta = timedelta(seconds=5)
    # If set, start the observability dashboard on this HTTP port alongside gRPC.
    dashboard_port: int | None = None

    def __post_init__(self) -> None:
        if not 0 <= self.port <= 0xFFFF:
            raise ValueError("port must be between 0 and 65535")
        if self.prune_after.total_seconds() < 0:
            raise ValueError("prune_after must be non-negative")
        if self.prune_interval.total_seconds() <= 0:
            raise ValueError("prune_interval must be positive")
        if self.stats_interval.total_seconds() <= 0:
            raise ValueError("stats_interval must be positive")
        if self.dashboard_port is not None and not 0 <= self.dashboard_port <= 0xFFFF:
            raise ValueError("dashboard_port must be between 0 and 65535")
        if self.dashboard_port == self.port:
            raise ValueError("dashboard_port and port can't be the same")


class Server:
    def __init__(
        self,
        *,
        options: ServerOptions,
    ) -> None:
        self.options = options
        self._started = False

        self.server = grpc.aio.server(
            interceptors=[LoggingInterceptor()],
            compression=grpc.Compression.NoCompression,
            options=[
                ("grpc.max_receive_message_length", 1 << 29),  # 0.5GB
                ("grpc.max_send_message_length", -1),  # .snapshot
            ],
        )

        self.histogrammer = Histogrammer()
        hist_pb2_grpc.add_HistogrammerServiceServicer_to_server(
            self.histogrammer, self.server
        )

        self._callbacks: list[asyncio.Task[None]] = []

        # add_insecure_port returns the actually bound port, which differs
        # from options.port when it is 0 ("pick any free port").
        self._port = self.server.add_insecure_port(f"[::]:{self.options.port}")
        if self._port == 0:
            raise RuntimeError(
                f"failed to bind gRPC server to port {self.options.port}"
            )

    @property
    def port(self) -> int:
        return self._port

    @property
    def address(self) -> str:
        return f"[::]:{self._port}"

    async def start(self) -> None:
        if self._started:
            return

        await self.server.start()
        self._callbacks = [
            asyncio.create_task(
                prune_old_hists(
                    histogrammer=self.histogrammer,
                    delta=self.options.prune_after,
                    interval=self.options.prune_interval,
                ),
                name="prune_old_hists",
            ),
            asyncio.create_task(
                print_hists_stats(
                    histogrammer=self.histogrammer,
                    interval=self.options.stats_interval,
                ),
                name="print_hists_stats",
            ),
        ]

        if self.options.dashboard_port is not None:
            dashboard = asyncio.create_task(
                self._start_dashboard(self.options.dashboard_port),
                name="dashboard",
            )
            dashboard.add_done_callback(self._dashboard_done)
            self._callbacks.append(dashboard)

        self._started = True

    @staticmethod
    def _dashboard_done(task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            logging.getLogger(__name__).error(
                "Dashboard task failed",
                exc_info=(type(error), error, error.__traceback__),
            )

    async def _start_dashboard(self, port: int) -> None:
        import uvicorn

        from histserv.dashboard import create_app

        # Works for both editable installs (src/histserv/dashboard/ui/dist) and
        # wheel installs (histserv/dashboard/ui/dist inside site-packages).
        _dist = Path(__file__).resolve().parent / "dashboard" / "ui" / "dist"
        static_dir = _dist if _dist.is_dir() else None
        if static_dir is None:
            logging.getLogger(__name__).warning(
                "Dashboard UI not found at %s — serving API/WS only. "
                "Run 'pixi run -e dashboard dashboard-build' to build the frontend.",
                _dist,
            )
        app = create_app(self.histogrammer, static_dir=static_dir)
        config = uvicorn.Config(
            app=app,
            host="0.0.0.0",
            port=port,
            log_level="warning",
        )
        server = uvicorn.Server(config)
        await server.serve()

    async def stop(self, grace: float | None = None) -> None:
        if not self._started:
            return

        for task in self._callbacks:
            task.cancel()
        if self._callbacks:
            await asyncio.gather(*self._callbacks, return_exceptions=True)
            self._callbacks.clear()
        await self.server.stop(grace=grace)
        self._started = False

    async def wait_for_termination(self, timeout: float | None = None) -> None:
        await self.server.wait_for_termination(timeout=timeout)
