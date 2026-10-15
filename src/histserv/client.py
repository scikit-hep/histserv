from __future__ import annotations

import typing as tp
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from typing import Literal, TypedDict

import grpc
from hist import Hist

from histserv.chunked_hist import (
    ChunkKey,
    ChunkScalar,
    ChunkedHist,
    _zero_dense_view,
    normalize_chunk_selection,
)
from histserv.protos import hist_pb2, hist_pb2_grpc
from histserv.serialize import (
    deserialize_chunked_hist_payload,
    serialize_chunk_payload,
    serialize_chunk_scalar,
    serialize_chunked_hist_payload,
    serialize_unique_id,
)


class TokenScopedStatsDict(TypedDict):
    histogram_count: int
    histogram_bytes: int
    rpc_calls_total: dict[str, int]


class _StatsBaseDict(TypedDict):
    histogram_count: int
    histogram_bytes: int
    active_rpcs: int
    version: str
    uptime_seconds: int
    user_cpu_seconds: float
    system_cpu_seconds: float
    rpc_calls_total: dict[str, int]
    observed_at: datetime


class StatsDict(_StatsBaseDict, total=False):
    token_scoped: TokenScopedStatsDict


class ConnectionInfoDict(TypedDict):
    address: str
    hist_id: str
    token: str | None


FillCompression = Literal["zstd", "lz4"]


def _validate_fill_compression(
    compression: FillCompression | None,
) -> FillCompression | None:
    if compression is None or compression in {"zstd", "lz4"}:
        return compression
    raise ValueError(f"unsupported fill compression: {compression!r}")


def _set_dense_view_codec(message: tp.Any, codec: FillCompression | None) -> None:
    if codec is not None:
        message.dense_view_codec = codec


class Client:
    def __init__(
        self,
        address: str,
    ) -> None:
        """Create a client connected to a histserv gRPC endpoint.

        Args:
            address: Server address in `host:port` form.

        Example:
            >>> client = Client("localhost:50051")
        """
        self.address = address

    def __getstate__(self):
        state = dict(self.__dict__)
        state.pop("channel", None)
        state.pop("stub", None)
        return state

    def __enter__(self) -> Client:
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        del exc_type, exc_value, traceback
        # Only close a channel that was actually created; accessing the
        # cached_property here would open one just to close it.
        if "channel" in self.__dict__:
            self.channel.close()

    @cached_property
    def channel(self) -> grpc.Channel:
        return grpc.insecure_channel(
            self.address,
            compression=grpc.Compression.NoCompression,
            options=[
                ("grpc.max_send_message_length", 1 << 29),  # 0.5GB
                ("grpc.max_receive_message_length", -1),  # .snapshot
            ],
        )

    @cached_property
    def stub(self) -> hist_pb2_grpc.HistogrammerServiceStub:
        return hist_pb2_grpc.HistogrammerServiceStub(self.channel)

    @staticmethod
    def _metadata(token: str | None) -> tuple[tuple[str, str], ...] | None:
        return None if token is None else (("x-histserv-token", token),)

    def init(
        self,
        hist: Hist | ChunkedHist,
        *,
        hist_id: str | None = None,
        token: str | None = None,
        timeout: int = 10,
        compression: FillCompression | None = None,
    ) -> RemoteHist:
        """Initialize a histogram on the server.

        Args:
            hist: Local `hist.Hist` or `ChunkedHist` to upload.
            hist_id: Optional server-side histogram identifier. When omitted,
                the server generates a UUIDv4 identifier.
            token: Optional access token associated with the histogram.
            timeout: RPC timeout in seconds.
            compression: Optional dense payload compression for this RPC.
                Use `None` (default) for no compression. Supported codecs are
                `"zstd"` and `"lz4"`.

        Returns:
            A `RemoteHist` handle for the created server-side histogram.

        Example:
            >>> local = hist.Hist(hist.axis.Regular(10, 0, 1, name="x"))
            >>> remote = client.init(local)
        """
        if isinstance(hist, ChunkedHist):
            chunked = hist
        elif isinstance(hist, Hist):
            chunked = ChunkedHist.from_hist(hist)
        else:
            raise ValueError(
                f"`hist` must be `hist.Hist` or `histserv.ChunkedHist`, got {type(hist)=}"
            )
        if not all(axis.name for axis in chunked.axes):
            raise ValueError("all axes must be named")
        compression = _validate_fill_compression(compression)
        payload = serialize_chunked_hist_payload(
            chunked,
            codec=compression,
        )
        request = hist_pb2.InitRequest(payload=payload)
        if hist_id is not None:
            request.hist_id = hist_id
        response = self.stub.Init(
            request,
            timeout=timeout,
            metadata=self._metadata(token),
        )
        return RemoteHist(
            client=self,
            hist_id=response.hist_id,
            token=token,
            template=ChunkedHist.from_metadata_json(chunked.metadata_json()),
        )

    def connect(
        self,
        hist_id: str,
        *,
        token: str | None = None,
        timeout: int = 10,
    ) -> RemoteHist:
        """Reconnect to an existing remote histogram.

        Args:
            hist_id: Histogram identifier returned by `init()`.
            token: Optional access token for the histogram.
            timeout: RPC timeout in seconds.

        Returns:
            A `RemoteHist` handle bound to the existing histogram.

        Example:
            >>> remote = client.connect("abc123", token="secret")
        """
        response = self.stub.Describe(
            hist_pb2.DescribeRequest(hist_id=hist_id),
            timeout=timeout,
            metadata=self._metadata(token),
        )
        return RemoteHist(
            client=self,
            hist_id=hist_id,
            token=token,
            template=ChunkedHist.from_metadata_json(response.hist_json),
        )

    def stats(self, *, token: str | None = None, timeout: int = 10) -> StatsDict:
        """Fetch server statistics.

        Args:
            token: Optional token to request token-scoped statistics.
            timeout: RPC timeout in seconds.

        Returns:
            A dictionary of global server statistics and, when available,
            token-scoped counters.

        Example:
            >>> stats = client.stats()
            >>> stats["histogram_count"]
        """
        response = self.stub.Stats(
            hist_pb2.StatsRequest(),
            timeout=timeout,
            metadata=self._metadata(token),
        )
        stats: StatsDict = {
            "histogram_count": response.histogram_count,
            "histogram_bytes": response.histogram_bytes,
            "active_rpcs": response.active_rpcs,
            "version": response.version,
            "uptime_seconds": response.uptime_seconds,
            "user_cpu_seconds": response.user_cpu_seconds,
            "system_cpu_seconds": response.system_cpu_seconds,
            "rpc_calls_total": dict(response.rpc_calls_total),
            "observed_at": response.observed_at.ToDatetime(),
        }
        if token is not None and response.HasField("token_scoped"):
            stats["token_scoped"] = {
                "histogram_count": response.token_scoped.histogram_count,
                "histogram_bytes": response.token_scoped.histogram_bytes,
                "rpc_calls_total": dict(response.token_scoped.rpc_calls_total),
            }
        return stats


class RemoteHist:
    __slots__ = ("_client", "_hist_id", "_token", "_template")

    def __init__(
        self,
        client: Client,
        hist_id: str,
        template: ChunkedHist,
        token: str | None = None,
    ) -> None:
        self._client = client
        self._hist_id = hist_id
        self._token = token
        self._template = template

    @property
    def client(self) -> Client:
        return self._client

    @property
    def hist_id(self) -> str:
        return self._hist_id

    @property
    def token(self) -> str | None:
        return self._token

    def _metadata(self) -> tuple[tuple[str, str], ...] | None:
        return self.client._metadata(self.token)

    def __repr__(self) -> str:
        return (
            "RemoteHist("
            f"hist_id={self.hist_id!r}, "
            f"address={self.client.address!r}, "
            f"token={self.token!r}"
            ")"
        )

    @classmethod
    def from_connection_info(
        cls,
        connection_info: ConnectionInfoDict,
        *,
        timeout: int = 10,
    ) -> RemoteHist:
        """Reconnect a remote histogram from serialized connection info.

        Args:
            connection_info: Output from `get_connection_info()`.
            timeout: RPC timeout in seconds for the reconnect `Describe` call.

        Returns:
            A fully connected `RemoteHist`.

        Example:
            >>> info = remote_hist.get_connection_info()
            >>> reconnect = RemoteHist.from_connection_info(info)
        """
        client = Client(connection_info["address"])
        return client.connect(
            connection_info["hist_id"],
            token=connection_info["token"],
            timeout=timeout,
        )

    def __getitem__(
        self,
        selection: Mapping[str, ChunkScalar | tp.Iterable[ChunkScalar]],
    ) -> RemoteHistSlice:
        """Create a remote slice over chunk axes.

        Args:
            selection: Mapping of chunk-axis name to one or more allowed values.

        Returns:
            A `RemoteHistSlice` that can be snapshotted from the server.

        Example:
            >>> sliced = remote_hist[{"cat": "a"}]
            >>> snapshot = sliced.snapshot()
        """
        return RemoteHistSlice(
            remote=self,
            selection=normalize_chunk_selection(
                selection,
                axis_names=(axis.name for axis in self._template.axes),
                chunk_axis_names=self._template.chunk_axis_names,
            ),
        )

    def get_connection_info(self) -> ConnectionInfoDict:
        """Return reconnect information for this remote histogram.

        Returns:
            A dictionary containing the server address, histogram id, and token.

        Example:
            >>> info = remote_hist.get_connection_info()
            >>> reconnect = RemoteHist.from_connection_info(info)
        """
        return {
            "address": self.client.address,
            "hist_id": self.hist_id,
            "token": self.token,
        }

    def _make_dense_hist(self) -> Hist:
        return Hist(
            *self._template.dense_axes,
            storage=self._template.storage_type(),
            name=self._template.name,
            label=self._template.label,
        )

    def fill(
        self,
        *,
        timeout: int = 10,
        unique_id: tp.Any | None = None,
        compression: FillCompression | None = None,
        **kwargs: tp.Any,
    ) -> hist_pb2.FillResponse:
        """Fill one remote chunk.

        Args:
            timeout: RPC timeout in seconds.
            unique_id: Optional idempotency key for the fill request.
            compression: Optional dense payload compression for this RPC.
                Use `None` (default) for no compression. Supported codecs are
                `"zstd"` and `"lz4"`.
            **kwargs: Named axis values and optional storage arguments such as
                `weight` or `sample`.

        Returns:
            The gRPC fill response.

        Example:
            >>> remote_hist.fill(x=[0.2, 0.4], cat="a")
        """
        compression = _validate_fill_compression(compression)
        chunk_key, dense_kwargs = self._template.split_fill_kwargs(kwargs)
        dense_hist = self._make_dense_hist()
        dense_hist.fill(**dense_kwargs)
        chunk = serialize_chunk_payload(
            chunk_key,
            dense_hist.view(flow=True),
            shape=self._template.dense_view_shape,
            dtype=self._template.dense_view_dtype,
            codec=compression,
        )
        request = hist_pb2.FillRequest(
            hist_id=self.hist_id,
            chunk_key=chunk.chunk_key,
            dense_view=chunk.dense_view,
        )
        _set_dense_view_codec(request, compression)
        if unique_id is not None:
            request.unique_id = serialize_unique_id(unique_id)
        return self.client.stub.Fill(
            request,
            timeout=timeout,
            metadata=self._metadata(),
        )

    def fill_many(
        self,
        fills: tp.Iterable[Mapping[str, tp.Any]],
        *,
        timeout: int = 10,
        unique_id: tp.Any | None = None,
        compression: FillCompression | None = None,
    ) -> hist_pb2.FillResponse:
        """Fill multiple remote chunks in a single request.

        This is useful for reducing gRPC request overhead by bundling several
        fills into one RPC.

        Args:
            fills: Iterable of fill keyword-argument mappings.
            timeout: RPC timeout in seconds.
            unique_id: Optional idempotency key for the fill-many request.
            compression: Optional dense payload compression for this RPC.
                Use `None` (default) for no compression. Supported codecs are
                `"zstd"` and `"lz4"`.

        Returns:
            The gRPC fill response.

        Example:
            >>> remote_hist.fill_many(
            ...     [
            ...         {"x": [0.2], "cat": "a"},
            ...         {"x": [0.4], "cat": "b"},
            ...     ]
            ... )
        """
        compression = _validate_fill_compression(compression)
        fills_list = list(fills)
        split_fills = [
            self._template.split_fill_kwargs(fill_kwargs) for fill_kwargs in fills_list
        ]
        chunks: list[hist_pb2.ChunkPayload] = []
        if fills_list:
            first_fill_axes = set(fills_list[0])
            if not all(
                first_fill_axes == set(fill_kwargs) for fill_kwargs in fills_list[1:]
            ):
                raise ValueError("all fills in fill_many must use compatible axes")

            # Accumulate fills per distinct chunk key before serializing, so
            # N fills of the same chunk cost one dense payload instead of N
            # (the server merges them anyway).
            grouped: dict[ChunkKey, list[dict[str, tp.Any]]] = {}
            for chunk_key, dense_kwargs in split_fills:
                grouped.setdefault(chunk_key, []).append(dense_kwargs)

            dense_hist = self._make_dense_hist()
            dense_view = dense_hist.view(flow=True)
            for chunk_key, dense_kwargs_list in grouped.items():
                try:
                    for dense_kwargs in dense_kwargs_list:
                        dense_hist.fill(**dense_kwargs)
                    chunks.append(
                        serialize_chunk_payload(
                            chunk_key,
                            dense_view,
                            shape=self._template.dense_view_shape,
                            dtype=self._template.dense_view_dtype,
                            codec=compression,
                        )
                    )
                finally:
                    _zero_dense_view(dense_view)
        request = hist_pb2.FillManyRequest(
            hist_id=self.hist_id,
            chunks=chunks,
        )
        _set_dense_view_codec(request, compression)
        if unique_id is not None:
            request.unique_id = serialize_unique_id(unique_id)
        return self.client.stub.FillMany(
            request,
            timeout=timeout,
            metadata=self._metadata(),
        )

    def exists(self, *, timeout: int = 10) -> bool:
        """Check whether this histogram still exists on the server.

        Args:
            timeout: RPC timeout in seconds.

        Returns:
            `True` if the histogram exists and is accessible, else `False`.

        Example:
            >>> remote_hist.exists()
            True
        """
        response = self.client.stub.Exists(
            hist_pb2.ExistsRequest(hist_id=self.hist_id),
            timeout=timeout,
            metadata=self._metadata(),
        )
        return response.exists

    def was_filled_with_unique_id(
        self, unique_id: tp.Any, *, timeout: int = 10
    ) -> bool:
        """Check whether this histogram has already been filled with `unique_id`.

        Args:
            unique_id: Idempotency key to check.
            timeout: RPC timeout in seconds.

        Returns:
            `True` if a prior `fill()` or `fill_many()` used this `unique_id`,
            else `False`.

        Example:
            >>> remote_hist.was_filled_with_unique_id(["foo", "bar"])
            False
        """
        response = self.client.stub.WasFilledWithUniqueId(
            hist_pb2.WasFilledWithUniqueIdRequest(
                hist_id=self.hist_id,
                unique_id=serialize_unique_id(unique_id),
            ),
            timeout=timeout,
            metadata=self._metadata(),
        )
        return response.was_filled

    def snapshot(
        self,
        delete_from_server: bool = False,
        *,
        timeout: int = 10,
        compression: FillCompression | None = None,
    ) -> ChunkedHist:
        """Fetch the current remote histogram contents.

        Args:
            delete_from_server: Whether to remove the histogram after snapshotting.
            timeout: RPC timeout in seconds.
            compression: Optional dense payload compression for this RPC.
                Use `None` (default) for no compression. Supported codecs are
                `"zstd"` and `"lz4"`.

        Returns:
            A local `ChunkedHist` snapshot of the server-side histogram.

        Example:
            >>> snapshot = remote_hist.snapshot()
            >>> dense = snapshot.to_hist()
        """
        compression = _validate_fill_compression(compression)
        request = hist_pb2.SnapshotRequest(
            hist_id=self.hist_id,
            delete_from_server=delete_from_server,
        )
        _set_dense_view_codec(request, compression)
        response = self.client.stub.Snapshot(
            request,
            timeout=timeout,
            metadata=self._metadata(),
        )
        return deserialize_chunked_hist_payload(response.payload)

    def flush(
        self,
        destination: str = "hist.h5",
        *,
        timeout: int = 10,
    ) -> hist_pb2.FlushResponse:
        """Write the remote histogram to disk on the server.

        Args:
            destination: Output path on the server filesystem.
            timeout: RPC timeout in seconds.

        Returns:
            The gRPC flush response.

        Example:
            >>> remote_hist.flush("snapshot.h5")
        """
        return self.client.stub.Flush(
            hist_pb2.FlushRequest(hist_id=self.hist_id, destination=destination),
            timeout=timeout,
            metadata=self._metadata(),
        )

    def delete(self, *, timeout: int = 10) -> hist_pb2.DeleteResponse:
        """Delete the remote histogram from the server.

        Args:
            timeout: RPC timeout in seconds.

        Returns:
            The gRPC delete response.

        Example:
            >>> remote_hist.delete()
        """
        return self.client.stub.Delete(
            hist_pb2.DeleteRequest(hist_id=self.hist_id),
            timeout=timeout,
            metadata=self._metadata(),
        )

    def reset(self, *, timeout: int = 10) -> hist_pb2.ResetResponse:
        """Reset the remote histogram contents in place.

        Args:
            timeout: RPC timeout in seconds.

        Returns:
            The gRPC reset response.

        Example:
            >>> remote_hist.reset()
        """
        return self.client.stub.Reset(
            hist_pb2.ResetRequest(hist_id=self.hist_id),
            timeout=timeout,
            metadata=self._metadata(),
        )


@dataclass(frozen=True, slots=True)
class RemoteHistSlice:
    remote: RemoteHist
    selection: Mapping[str, tuple[ChunkScalar, ...]]

    def __repr__(self) -> str:
        return (
            "RemoteHistSlice("
            f"hist_id={self.remote.hist_id!r}, "
            f"address={self.remote.client.address!r}, "
            f"token={self.remote.token!r}, "
            f"selection={dict(self.selection)!r}"
            ")"
        )

    def snapshot(
        self,
        *,
        timeout: int = 10,
        compression: FillCompression | None = None,
    ) -> ChunkedHist:
        """Fetch a snapshot of the selected remote chunks.

        Args:
            timeout: RPC timeout in seconds.
            compression: Optional dense payload compression for this RPC.
                Use `None` (default) for no compression. Supported codecs are
                `"zstd"` and `"lz4"`.

        Returns:
            A local `ChunkedHist` containing only the selected chunks.

        Example:
            >>> sliced = remote_hist[{"cat": "a"}]
            >>> snapshot = sliced.snapshot()
        """
        compression = _validate_fill_compression(compression)
        request = hist_pb2.SnapshotRequest(
            hist_id=self.remote.hist_id,
            delete_from_server=False,
            chunk_selectors=[
                hist_pb2.ChunkSelector(
                    axis=axis_name,
                    values=[serialize_chunk_scalar(value) for value in values],
                )
                for axis_name, values in self.selection.items()
            ],
        )
        _set_dense_view_codec(request, compression)
        response = self.remote.client.stub.Snapshot(
            request,
            timeout=timeout,
            metadata=self.remote._metadata(),
        )
        return deserialize_chunked_hist_payload(response.payload)
