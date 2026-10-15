from __future__ import annotations

import asyncio
import logging
import os
import time
import typing as tp
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import Final

import grpc

from histserv import __version__
from histserv.chunked_hist import ChunkedHist
from histserv.logging import (
    fmt_rpc_logger_msg,
    fmt_rpc_logger_msg_no_hist_id,
    get_logger,
)
from histserv.protos import hist_pb2, hist_pb2_grpc
from histserv.serialize import (
    deserialize_chunk_key,
    deserialize_chunk_scalar,
    deserialize_chunked_hist_payload,
    deserialize_dense_view_bytes,
    merge_chunk_payloads,
    serialize_chunked_hist_payload,
)
from histserv.util import bytes_repr, duration_repr

logger = get_logger("histserv")

RPC_INIT: Final = "Init"
RPC_DESCRIBE: Final = "Describe"
RPC_EXISTS: Final = "Exists"
RPC_WAS_FILLED_WITH_UNIQUE_ID: Final = "WasFilledWithUniqueId"
RPC_FILL: Final = "Fill"
RPC_FILL_MANY: Final = "FillMany"
RPC_SNAPSHOT: Final = "Snapshot"
RPC_DELETE: Final = "Delete"
RPC_RESET: Final = "Reset"
RPC_FLUSH: Final = "Flush"
RPC_STATS: Final = "Stats"


class LoggingInterceptor(grpc.aio.ServerInterceptor):
    async def intercept_service(self, continuation, handler_call_details):
        handler = await continuation(handler_call_details)
        if handler is None or handler.unary_unary is None:
            return handler

        rpc_method = handler_call_details.method.rsplit("/", 1)[-1]

        async def logging_wrapper(request, context):
            started = time.perf_counter()
            debug_enabled = logger.isEnabledFor(logging.DEBUG)
            error_enabled = logger.isEnabledFor(logging.ERROR)
            request_size = request.ByteSize() if (debug_enabled or error_enabled) else 0

            def _log(level: int, msg: str) -> None:
                hist_id = getattr(request, "hist_id", None)
                if isinstance(hist_id, str) and hist_id:
                    logger.log(
                        level,
                        fmt_rpc_logger_msg(
                            rpc_method=rpc_method, hist_id=hist_id, msg=msg
                        ),
                    )
                else:
                    logger.log(
                        level,
                        fmt_rpc_logger_msg_no_hist_id(
                            rpc_method=rpc_method, msg=msg
                        ),
                    )

            try:
                response = await handler.unary_unary(request, context)
            except Exception:
                if error_enabled:
                    duration = time.perf_counter() - started
                    _log(
                        logging.ERROR,
                        f"request={bytes_repr(request_size)}, "
                        f"duration={duration_repr(duration)}, "
                        "response=<error>",
                    )
                raise

            if debug_enabled:
                duration = time.perf_counter() - started
                _log(
                    logging.DEBUG,
                    f"request={bytes_repr(request_size)}, "
                    f"response={bytes_repr(response.ByteSize())}, "
                    f"duration={duration_repr(duration)}",
                )
            return response

        return grpc.unary_unary_rpc_method_handler(
            logging_wrapper,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )


@dataclass
class HistogramEntry:
    hist: ChunkedHist
    token: str | None
    last_access: datetime
    unique_ids: set[bytes]


@dataclass(frozen=True)
class TokenScopedStatsSnapshot:
    histogram_count: int
    histogram_bytes: int
    rpc_calls_total: dict[str, int]


@dataclass(frozen=True)
class StatsSnapshot:
    histogram_count: int
    histogram_bytes: int
    active_rpcs: int
    version: str
    uptime_seconds: int
    user_cpu_seconds: float
    system_cpu_seconds: float
    rpc_calls_total: dict[str, int]
    observed_at: datetime
    token_scoped: TokenScopedStatsSnapshot | None


class Histogrammer(hist_pb2_grpc.HistogrammerServiceServicer):
    def __init__(self) -> None:
        super().__init__()
        self._entries: dict[str, HistogramEntry] = {}
        self._active_rpcs = 0
        self._started_at = datetime.now(timezone.utc)
        self._rpc_calls_total: Counter[str] = Counter()
        self._rpc_calls_by_token: Counter[tuple[str, str]] = Counter()

    def _rpc_started(self, rpc_name: str, token: str | None) -> None:
        self._active_rpcs += 1
        self._rpc_calls_total[rpc_name] += 1
        if token is not None:
            self._rpc_calls_by_token[(token, rpc_name)] += 1

    def _rpc_finished(self) -> None:
        self._active_rpcs -= 1

    def _request_token(self, context: grpc.ServicerContext) -> str | None:
        for key, value in context.invocation_metadata():
            if key == "x-histserv-token":
                return value
        return None

    async def _abort_internal(
        self,
        *,
        context: grpc.ServicerContext,
        rpc_method: str,
        hist_id: str,
        exc: Exception,
    ) -> None:
        error_msg = f"unexpected server error during {rpc_method}: {exc!r}"
        logger.error(
            fmt_rpc_logger_msg(
                rpc_method=rpc_method,
                hist_id=hist_id,
                msg=error_msg,
            ),
            exc_info=exc,
        )
        await context.abort(grpc.StatusCode.INTERNAL, error_msg)

    @staticmethod
    async def _abort(
        context: grpc.ServicerContext,
        code: grpc.StatusCode,
        details: str,
    ) -> tp.NoReturn:
        await context.abort(code, details)
        raise RuntimeError("grpc context.abort unexpectedly returned")

    def _get_entry(
        self,
        *,
        hist_id: str,
        request_token: str | None,
        touch: bool = True,
    ) -> HistogramEntry | None:
        entry = self._entries.get(hist_id)
        if entry is None:
            return None
        if entry.token is not None and entry.token != request_token:
            return None
        if touch:
            entry.last_access = datetime.now(timezone.utc)
        return entry

    async def _require_entry(
        self,
        *,
        context: grpc.ServicerContext,
        rpc_method: str,
        hist_id: str,
        request_token: str | None,
    ) -> HistogramEntry:
        entry = self._get_entry(hist_id=hist_id, request_token=request_token)
        if entry is not None:
            return entry
        await self._abort(
            context, grpc.StatusCode.NOT_FOUND, f"unknown histogram id: {hist_id}"
        )

    def _histogram_bytes(self, entries: list[HistogramEntry]) -> int:
        return sum(entry.hist.histogram_bytes() for entry in entries)

    async def _reject_duplicate_unique_id(
        self,
        *,
        context: grpc.ServicerContext,
        hist_id: str,
        unique_id: bytes | None,
        entry: HistogramEntry,
    ) -> None:
        if unique_id is not None and unique_id in entry.unique_ids:
            await self._abort(
                context,
                grpc.StatusCode.ALREADY_EXISTS,
                f"rejected fill of histogram id {hist_id}, because request.unique_id={unique_id!r} already exists",
            )

    @staticmethod
    def _remember_unique_id(entry: HistogramEntry, unique_id: bytes | None) -> None:
        if unique_id is not None:
            entry.unique_ids.add(unique_id)

    @staticmethod
    def _request_unique_id(request: tp.Any) -> bytes | None:
        return request.unique_id if request.HasField("unique_id") else None

    @staticmethod
    def _request_dense_view_codec(request: tp.Any) -> str | None:
        return (
            request.dense_view_codec if request.HasField("dense_view_codec") else None
        )

    def _compute_stats(self, *, token: str | None) -> StatsSnapshot:
        entries = list(self._entries.values())
        token_scoped = None
        if token is not None:
            token_entries = [entry for entry in entries if entry.token == token]
            token_scoped = TokenScopedStatsSnapshot(
                histogram_count=len(token_entries),
                histogram_bytes=self._histogram_bytes(token_entries),
                rpc_calls_total={
                    rpc_name: count
                    for (token_key, rpc_name), count in self._rpc_calls_by_token.items()
                    if token_key == token
                },
            )

        times = os.times()
        observed_at = datetime.now(timezone.utc)
        return StatsSnapshot(
            histogram_count=len(entries),
            histogram_bytes=self._histogram_bytes(entries),
            active_rpcs=self._active_rpcs,
            version=__version__,
            uptime_seconds=int((observed_at - self._started_at).total_seconds()),
            user_cpu_seconds=times.user,
            system_cpu_seconds=times.system,
            rpc_calls_total=dict(self._rpc_calls_total),
            observed_at=observed_at,
            token_scoped=token_scoped,
        )

    def entries_snapshot(self) -> tuple[HistogramEntry, ...]:
        return tuple(self._entries.values())

    def _remove_entry(self, hist_id: str) -> None:
        """Remove a histogram and, if its token is no longer used by any
        entry, the token's RPC counters — otherwise the per-token Counter
        grows without bound on a long-lived server. A token with no
        histograms is unreachable through Stats anyway (NOT_FOUND)."""
        entry = self._entries.pop(hist_id, None)
        if entry is None or entry.token is None:
            return
        token = entry.token
        if any(e.token == token for e in self._entries.values()):
            return
        for key in [k for k in self._rpc_calls_by_token if k[0] == token]:
            del self._rpc_calls_by_token[key]

    def prune_entries_older_than(self, *, now: datetime, age: timedelta) -> list[str]:
        removed: list[str] = []
        for hist_id, entry in tuple(self._entries.items()):
            if (now - entry.last_access) >= age:
                self._remove_entry(hist_id)
                removed.append(hist_id)
        return removed

    async def Init(
        self, request: hist_pb2.InitRequest, context: grpc.ServicerContext
    ) -> hist_pb2.InitResponse:
        request_token = self._request_token(context)
        self._rpc_started(RPC_INIT, request_token)
        try:
            hist_id = request.hist_id or uuid.uuid4().hex
            fmt_rpc_msg = partial(
                fmt_rpc_logger_msg, rpc_method=RPC_INIT, hist_id=hist_id
            )

            try:
                hist_obj = deserialize_chunked_hist_payload(request.payload)
            except (TypeError, ValueError) as exc:
                logger.error(fmt_rpc_msg(msg=f"invalid init payload: {exc!r}"))
                await self._abort(
                    context,
                    grpc.StatusCode.INVALID_ARGUMENT,
                    f"invalid init payload: {exc!r}",
                )
            except Exception as exc:
                await self._abort_internal(
                    context=context,
                    rpc_method=RPC_INIT,
                    hist_id=hist_id,
                    exc=exc,
                )

            self._entries[hist_id] = HistogramEntry(
                hist=hist_obj,
                token=request_token,
                last_access=datetime.now(timezone.utc),
                unique_ids=set(),
            )
            logger.debug(fmt_rpc_msg(msg="initialized histogram"))
            return hist_pb2.InitResponse(hist_id=hist_id)
        finally:
            self._rpc_finished()

    async def Describe(
        self, request: hist_pb2.DescribeRequest, context: grpc.ServicerContext
    ) -> hist_pb2.DescribeResponse:
        request_token = self._request_token(context)
        self._rpc_started(RPC_DESCRIBE, request_token)
        try:
            entry = await self._require_entry(
                context=context,
                rpc_method=RPC_DESCRIBE,
                hist_id=request.hist_id,
                request_token=request_token,
            )
            return hist_pb2.DescribeResponse(hist_json=entry.hist.metadata_json())
        finally:
            self._rpc_finished()

    async def Exists(
        self, request: hist_pb2.ExistsRequest, context: grpc.ServicerContext
    ) -> hist_pb2.ExistsResponse:
        request_token = self._request_token(context)
        self._rpc_started(RPC_EXISTS, request_token)
        try:
            return hist_pb2.ExistsResponse(
                exists=self._get_entry(
                    hist_id=request.hist_id,
                    request_token=request_token,
                    touch=False,
                )
                is not None
            )
        finally:
            self._rpc_finished()

    async def WasFilledWithUniqueId(
        self,
        request: hist_pb2.WasFilledWithUniqueIdRequest,
        context: grpc.ServicerContext,
    ) -> hist_pb2.WasFilledWithUniqueIdResponse:
        request_token = self._request_token(context)
        self._rpc_started(RPC_WAS_FILLED_WITH_UNIQUE_ID, request_token)
        try:
            entry = await self._require_entry(
                context=context,
                rpc_method=RPC_WAS_FILLED_WITH_UNIQUE_ID,
                hist_id=request.hist_id,
                request_token=request_token,
            )
            return hist_pb2.WasFilledWithUniqueIdResponse(
                was_filled=request.unique_id in entry.unique_ids
            )
        finally:
            self._rpc_finished()

    async def Fill(
        self, request: hist_pb2.FillRequest, context: grpc.ServicerContext
    ) -> hist_pb2.FillResponse:
        request_token = self._request_token(context)
        self._rpc_started(RPC_FILL, request_token)
        try:
            hist_id = request.hist_id
            fmt_rpc_msg = partial(
                fmt_rpc_logger_msg, rpc_method=RPC_FILL, hist_id=hist_id
            )
            entry = await self._require_entry(
                context=context,
                rpc_method=RPC_FILL,
                hist_id=hist_id,
                request_token=request_token,
            )

            unique_id = self._request_unique_id(request)
            await self._reject_duplicate_unique_id(
                context=context,
                hist_id=hist_id,
                unique_id=unique_id,
                entry=entry,
            )

            try:
                chunk_key = deserialize_chunk_key(
                    request.chunk_key,
                    axis_count=len(entry.hist.chunk_axis_names),
                )
                dense_view = deserialize_dense_view_bytes(
                    request.dense_view,
                    shape=entry.hist.dense_view_shape,
                    dtype=entry.hist.dense_view_dtype,
                    expected_nbytes=entry.hist._dense_view_nbytes,
                    codec=self._request_dense_view_codec(request),
                )
                entry.hist.add_dense_view(chunk_key, dense_view)
                self._remember_unique_id(entry, unique_id)
                logger.debug(fmt_rpc_msg(msg="merged one dense fill payload"))
            except (TypeError, ValueError) as exc:
                logger.error(fmt_rpc_msg(msg=f"invalid fill request: {exc!r}"))
                await self._abort(
                    context,
                    grpc.StatusCode.INVALID_ARGUMENT,
                    f"invalid fill request: {exc!r}",
                )
            except Exception as exc:
                await self._abort_internal(
                    context=context,
                    rpc_method=RPC_FILL,
                    hist_id=hist_id,
                    exc=exc,
                )

            return hist_pb2.FillResponse()
        finally:
            self._rpc_finished()

    async def FillMany(
        self, request: hist_pb2.FillManyRequest, context: grpc.ServicerContext
    ) -> hist_pb2.FillResponse:
        request_token = self._request_token(context)
        self._rpc_started(RPC_FILL_MANY, request_token)
        try:
            hist_id = request.hist_id
            fmt_rpc_msg = partial(
                fmt_rpc_logger_msg,
                rpc_method=RPC_FILL_MANY,
                hist_id=hist_id,
            )
            entry = await self._require_entry(
                context=context,
                rpc_method=RPC_FILL_MANY,
                hist_id=hist_id,
                request_token=request_token,
            )

            unique_id = self._request_unique_id(request)
            await self._reject_duplicate_unique_id(
                context=context,
                hist_id=hist_id,
                unique_id=unique_id,
                entry=entry,
            )

            try:
                merged_bytes = merge_chunk_payloads(
                    entry.hist,
                    request.chunks,
                    codec=self._request_dense_view_codec(request),
                )
                self._remember_unique_id(entry, unique_id)
                logger.debug(
                    fmt_rpc_msg(
                        msg=f"merged {bytes_repr(merged_bytes)} of decoded dense payload"
                    )
                )
            except (TypeError, ValueError) as exc:
                logger.error(fmt_rpc_msg(msg=f"invalid fill-many request: {exc!r}"))
                await self._abort(
                    context,
                    grpc.StatusCode.INVALID_ARGUMENT,
                    f"invalid fill-many request: {exc!r}",
                )
            except Exception as exc:
                await self._abort_internal(
                    context=context,
                    rpc_method=RPC_FILL_MANY,
                    hist_id=hist_id,
                    exc=exc,
                )

            return hist_pb2.FillResponse()
        finally:
            self._rpc_finished()

    async def Snapshot(
        self, request: hist_pb2.SnapshotRequest, context: grpc.ServicerContext
    ) -> hist_pb2.SnapshotResponse:
        request_token = self._request_token(context)
        self._rpc_started(RPC_SNAPSHOT, request_token)
        try:
            hist_id = request.hist_id
            entry = await self._require_entry(
                context=context,
                rpc_method=RPC_SNAPSHOT,
                hist_id=hist_id,
                request_token=request_token,
            )

            try:
                if request.delete_from_server and request.chunk_selectors:
                    await self._abort(
                        context,
                        grpc.StatusCode.INVALID_ARGUMENT,
                        "delete_from_server is not allowed for partial snapshots",
                    )

                # Capture an atomic copy of the chunk state synchronously on
                # the event loop (gRPC fills cannot interleave with synchronous
                # code here), so the heavy serialization below can run in a
                # worker thread without racing concurrent mutations. The entry
                # is only removed after serialization succeeds, so a failed
                # snapshot never loses data.
                if request.delete_from_server:
                    snapshot = entry.hist.copy()
                elif request.chunk_selectors:
                    selection: dict[str, list[str | int]] = {}
                    for selector in request.chunk_selectors:
                        if not selector.axis:
                            raise ValueError("chunk selector axis must be non-empty")
                        values: list[str | int] = []
                        for value in selector.values:
                            values.append(deserialize_chunk_scalar(value))
                        if not values:
                            raise ValueError(
                                f"chunk selector for axis {selector.axis!r} must be non-empty"
                            )
                        selection[selector.axis] = values
                    # __getitem__ already returns a fresh ChunkedHist with
                    # copied chunk arrays, so it is detached from live storage.
                    snapshot = entry.hist[selection]
                else:
                    snapshot = entry.hist.copy()

                payload = await asyncio.to_thread(
                    serialize_chunked_hist_payload,
                    snapshot,
                    codec=self._request_dense_view_codec(request),
                )
                if request.delete_from_server:
                    self._remove_entry(hist_id)
                logger.debug(
                    fmt_rpc_logger_msg(
                        rpc_method=RPC_SNAPSHOT,
                        hist_id=hist_id,
                        msg="created snapshot",
                    )
                )
                return hist_pb2.SnapshotResponse(payload=payload)
            except (TypeError, ValueError) as exc:
                logger.error(
                    fmt_rpc_logger_msg(
                        rpc_method=RPC_SNAPSHOT,
                        hist_id=hist_id,
                        msg=f"invalid snapshot state: {exc!r}",
                    )
                )
                await self._abort(
                    context,
                    grpc.StatusCode.FAILED_PRECONDITION,
                    f"invalid snapshot state: {exc!r}",
                )
            except Exception as exc:
                await self._abort_internal(
                    context=context,
                    rpc_method=RPC_SNAPSHOT,
                    hist_id=hist_id,
                    exc=exc,
                )
        finally:
            self._rpc_finished()

        raise AssertionError("unreachable")

    async def Delete(
        self, request: hist_pb2.DeleteRequest, context: grpc.ServicerContext
    ) -> hist_pb2.DeleteResponse:
        request_token = self._request_token(context)
        self._rpc_started(RPC_DELETE, request_token)
        try:
            hist_id = request.hist_id
            await self._require_entry(
                context=context,
                rpc_method=RPC_DELETE,
                hist_id=hist_id,
                request_token=request_token,
            )
            self._remove_entry(hist_id)
            logger.debug(
                fmt_rpc_logger_msg(
                    rpc_method=RPC_DELETE,
                    hist_id=hist_id,
                    msg="deleted histogram",
                )
            )
            return hist_pb2.DeleteResponse()
        finally:
            self._rpc_finished()

    async def Reset(
        self, request: hist_pb2.ResetRequest, context: grpc.ServicerContext
    ) -> hist_pb2.ResetResponse:
        request_token = self._request_token(context)
        self._rpc_started(RPC_RESET, request_token)
        try:
            hist_id = request.hist_id
            entry = await self._require_entry(
                context=context,
                rpc_method=RPC_RESET,
                hist_id=hist_id,
                request_token=request_token,
            )
            entry.hist.reset()
            entry.unique_ids.clear()
            logger.debug(
                fmt_rpc_logger_msg(
                    rpc_method=RPC_RESET,
                    hist_id=hist_id,
                    msg="reset histogram",
                )
            )
            return hist_pb2.ResetResponse()
        finally:
            self._rpc_finished()

    async def Flush(
        self, request: hist_pb2.FlushRequest, context: grpc.ServicerContext
    ) -> hist_pb2.FlushResponse:
        request_token = self._request_token(context)
        self._rpc_started(RPC_FLUSH, request_token)
        try:
            try:
                import h5py
                import uhi.io.hdf5
            except ImportError:
                await self._abort(
                    context,
                    grpc.StatusCode.FAILED_PRECONDITION,
                    "Flush requires h5py on the server; "
                    "install the optional extra 'histserv[hdf5]'",
                )

            hist_id = request.hist_id
            destination = request.destination
            if not destination.endswith((".h5", ".hdf5")):
                await self._abort(
                    context,
                    grpc.StatusCode.INVALID_ARGUMENT,
                    f"invalid destination: {destination}, needs to be a hdf5 file",
                )

            entry = await self._require_entry(
                context=context,
                rpc_method=RPC_FLUSH,
                hist_id=hist_id,
                request_token=request_token,
            )
            # Capture an atomic copy on the event loop (same pattern as
            # Snapshot), then materialize and write the file in a worker
            # thread so the blocking I/O does not stall other RPCs. The
            # entry is removed only after the write succeeds.
            snapshot = entry.hist.copy()

            def _write_hdf5() -> None:
                with h5py.File(destination, "w") as h5_file:
                    uhi.io.hdf5.write(
                        h5_file.create_group(hist_id), snapshot.to_hist()
                    )

            await asyncio.to_thread(_write_hdf5)
            self._remove_entry(hist_id)
            logger.debug(
                fmt_rpc_logger_msg(
                    rpc_method=RPC_FLUSH,
                    hist_id=hist_id,
                    msg=f"flushed histogram to {destination}",
                )
            )
            return hist_pb2.FlushResponse()
        finally:
            self._rpc_finished()

    async def Stats(
        self, request: hist_pb2.StatsRequest, context: grpc.ServicerContext
    ) -> hist_pb2.StatsResponse:
        del request
        request_token = self._request_token(context)
        self._rpc_started(RPC_STATS, request_token)
        try:
            if request_token is not None and not any(
                entry.token == request_token for entry in self._entries.values()
            ):
                await self._abort(
                    context,
                    grpc.StatusCode.NOT_FOUND,
                    f"unknown token: {request_token}",
                )

            stats = self._compute_stats(token=request_token)
            token_scoped = None
            if stats.token_scoped is not None:
                token_scoped = hist_pb2.TokenScopedStats(
                    histogram_count=stats.token_scoped.histogram_count,
                    histogram_bytes=stats.token_scoped.histogram_bytes,
                    rpc_calls_total=stats.token_scoped.rpc_calls_total,
                )
            return hist_pb2.StatsResponse(
                histogram_count=stats.histogram_count,
                histogram_bytes=stats.histogram_bytes,
                active_rpcs=stats.active_rpcs,
                version=stats.version,
                uptime_seconds=stats.uptime_seconds,
                user_cpu_seconds=stats.user_cpu_seconds,
                system_cpu_seconds=stats.system_cpu_seconds,
                rpc_calls_total=stats.rpc_calls_total,
                observed_at=stats.observed_at,
                token_scoped=token_scoped,
            )
        finally:
            self._rpc_finished()
