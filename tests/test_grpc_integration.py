from __future__ import annotations

import grpc
import hist
import numpy as np
import pytest
from hist import Hist

from histserv.chunked_hist import ChunkedHist
from histserv.client import Client, RemoteHist
from histserv.protos import hist_pb2
from tests.histogram_fixtures import (
    CategoricalHistCase,
    categorical_hist_cases,
    regular_hist,
)

INTEGRATION_CATEGORICAL_CASES = categorical_hist_cases()
CATEGORICAL_AXIS_NAME_CASES: tuple[tuple[str, ...], ...] = (
    ("cat",),
    ("cat", "var"),
    ("cat", "var", "region"),
)
COMPRESSION_CASES = [None, "zstd", "lz4"]


def _weighted_categorical_hist() -> Hist:
    return categorical_hist_cases()[0].make_hist()


def _weighted_str_categorical_hist(axis_names: tuple[str, ...]) -> Hist:
    return Hist(
        hist.axis.Regular(128, -5, 5, name="x"),
        *[
            hist.axis.StrCategory([], growth=True, name=axis_name)
            for axis_name in axis_names
        ],
        storage=hist.storage.Weight(),
    )


def _randomized_categorical_fill_calls(
    *,
    axis_names: tuple[str, ...],
    rng: np.random.Generator,
    n_calls: int,
    samples_per_call: int,
) -> list[dict[str, object]]:
    fill_calls: list[dict[str, object]] = []
    for i in range(n_calls):
        fill_kwargs: dict[str, object] = {
            "x": np.clip(
                rng.normal(loc=0.2 * (i + 1), scale=1.1, size=samples_per_call),
                -5.0,
                5.0,
            ).astype(np.float32),
            "weight": rng.uniform(0.25, 2.0, size=samples_per_call).astype(np.float32),
        }
        for axis_index, axis_name in enumerate(axis_names):
            fill_kwargs[axis_name] = (
                f"{axis_name}_{(i + axis_index) % (axis_index + 3)}"
            )
        fill_calls.append(fill_kwargs)
    return fill_calls


def _assert_hist_views_match(actual: np.ndarray, expected: np.ndarray) -> None:
    if actual.dtype.fields is None:
        np.testing.assert_equal(actual, expected)
        return
    assert actual.dtype == expected.dtype
    for field_name in actual.dtype.fields:
        np.testing.assert_allclose(actual[field_name], expected[field_name])


def test_init_accepts_explicit_hist_id(client: Client) -> None:
    remote_hist = client.init(
        regular_hist(),
        hist_id="my-histogram",
        token="alice",
    )

    assert remote_hist.hist_id == "my-histogram"
    assert remote_hist.exists() is True


def test_remote_fill_matches_local_hist_for_regular_axes(client: Client) -> None:
    local_hist = regular_hist()
    remote_hist = client.init(regular_hist(), token="alice")

    fill_calls = [
        {"x": np.array([0.25, 1.5, 3.75], dtype=np.float32)},
        {"x": np.array([0.5, 1.75], dtype=np.float32)},
    ]

    for fill_kwargs in fill_calls:
        local_hist.fill(**fill_kwargs)
        response = remote_hist.fill(**fill_kwargs)
        assert isinstance(response, hist_pb2.FillResponse)

    remote_snapshot = remote_hist.snapshot()
    np.testing.assert_equal(
        remote_snapshot.to_hist().view(flow=True), local_hist.view(flow=True)
    )


def test_remote_fill_many_matches_local_hist_for_regular_axes(client: Client) -> None:
    local_hist = regular_hist()
    remote_hist = client.init(regular_hist(), token="alice")

    fill_calls = [
        {"x": np.array([0.25, 1.5, 3.75], dtype=np.float32)},
        {"x": np.array([0.5, 1.75], dtype=np.float32)},
    ]
    for fill_kwargs in fill_calls:
        local_hist.fill(**fill_kwargs)

    response = remote_hist.fill_many(fill_calls)
    assert isinstance(response, hist_pb2.FillResponse)

    remote_snapshot = remote_hist.snapshot()
    np.testing.assert_equal(
        remote_snapshot.to_hist().view(flow=True), local_hist.view(flow=True)
    )


@pytest.mark.parametrize("compression", COMPRESSION_CASES)
def test_remote_init_supports_per_rpc_compression(
    client: Client,
    compression: str | None,
) -> None:
    local_hist = regular_hist()
    local_hist.fill(x=np.array([0.25, 1.5, 3.75], dtype=np.float32))
    local_hist.fill(x=np.array([0.5, 1.75], dtype=np.float32))

    remote_hist = client.init(local_hist, token="alice", compression=compression)
    remote_snapshot = remote_hist.snapshot()
    np.testing.assert_equal(
        remote_snapshot.to_hist().view(flow=True), local_hist.view(flow=True)
    )


@pytest.mark.parametrize("compression", COMPRESSION_CASES)
def test_remote_fill_supports_per_rpc_compression(
    client: Client,
    compression: str | None,
) -> None:
    local_hist = regular_hist()
    remote_hist = client.init(regular_hist(), token="alice")

    fill_kwargs = {"x": np.array([0.25, 1.5, 3.75], dtype=np.float32)}
    local_hist.fill(**fill_kwargs)
    response = remote_hist.fill(**fill_kwargs, compression=compression)
    assert isinstance(response, hist_pb2.FillResponse)

    remote_snapshot = remote_hist.snapshot()
    np.testing.assert_equal(
        remote_snapshot.to_hist().view(flow=True), local_hist.view(flow=True)
    )


@pytest.mark.parametrize("compression", COMPRESSION_CASES)
def test_remote_fill_many_supports_per_rpc_compression(
    client: Client,
    compression: str | None,
) -> None:
    local_hist = regular_hist()
    remote_hist = client.init(regular_hist(), token="alice")

    fill_calls = [
        {"x": np.array([0.25, 1.5, 3.75], dtype=np.float32)},
        {"x": np.array([0.5, 1.75], dtype=np.float32)},
    ]
    for fill_kwargs in fill_calls:
        local_hist.fill(**fill_kwargs)

    response = remote_hist.fill_many(fill_calls, compression=compression)
    assert isinstance(response, hist_pb2.FillResponse)

    remote_snapshot = remote_hist.snapshot()
    np.testing.assert_equal(
        remote_snapshot.to_hist().view(flow=True), local_hist.view(flow=True)
    )


@pytest.mark.parametrize("compression", COMPRESSION_CASES)
def test_remote_snapshot_supports_per_rpc_compression(
    client: Client,
    compression: str | None,
) -> None:
    local_hist = regular_hist()
    local_hist.fill(x=np.array([0.25, 1.5, 3.75], dtype=np.float32))
    local_hist.fill(x=np.array([0.5, 1.75], dtype=np.float32))

    remote_hist = client.init(local_hist, token="alice")
    remote_snapshot = remote_hist.snapshot(compression=compression)
    np.testing.assert_equal(
        remote_snapshot.to_hist().view(flow=True), local_hist.view(flow=True)
    )


@pytest.mark.parametrize(
    "axis_names",
    CATEGORICAL_AXIS_NAME_CASES,
    ids=lambda names: f"{len(names)}d_categorical",
)
@pytest.mark.parametrize("compression", COMPRESSION_CASES)
def test_remote_fill_many_matches_local_hist_for_randomized_categorical_payloads(
    client: Client,
    axis_names: tuple[str, ...],
    compression: str | None,
) -> None:
    rng = np.random.default_rng(12345)
    local_hist = _weighted_str_categorical_hist(axis_names)
    remote_hist = client.init(_weighted_str_categorical_hist(axis_names), token="alice")

    fill_calls = _randomized_categorical_fill_calls(
        axis_names=axis_names,
        rng=rng,
        n_calls=8,
        samples_per_call=96,
    )
    for fill_kwargs in fill_calls:
        local_hist.fill(**fill_kwargs)

    response = remote_hist.fill_many(fill_calls, compression=compression)
    assert isinstance(response, hist_pb2.FillResponse)

    remote_snapshot = remote_hist.snapshot().to_hist()
    for axis_name in axis_names:
        axis_index = remote_snapshot.axes.name.index(axis_name)
        expected_index = local_hist.axes.name.index(axis_name)
        assert list(remote_snapshot.axes[axis_index]) == list(
            local_hist.axes[expected_index]
        )
    _assert_hist_views_match(
        remote_snapshot.view(flow=True),
        local_hist.view(flow=True),
    )


@pytest.mark.parametrize(
    "axis_names",
    CATEGORICAL_AXIS_NAME_CASES,
    ids=lambda names: f"{len(names)}d_categorical",
)
@pytest.mark.parametrize("compression", COMPRESSION_CASES)
def test_snapshot_matches_local_hist_after_mixed_fill_and_fill_many_for_categorical_axes(
    client: Client,
    axis_names: tuple[str, ...],
    compression: str | None,
) -> None:
    rng = np.random.default_rng(54321)
    local_hist = _weighted_str_categorical_hist(axis_names)
    remote_hist = client.init(_weighted_str_categorical_hist(axis_names), token="alice")

    first_fill: dict[str, object] = {
        "x": np.array([0.25, 1.5, 3.75], dtype=np.float32),
        "weight": np.array([1.0, 0.5, 2.0], dtype=np.float32),
    }
    for axis_index, axis_name in enumerate(axis_names):
        first_fill[axis_name] = f"lead_{axis_name}_{axis_index}"
    local_hist.fill(**first_fill)
    response = remote_hist.fill(**first_fill, compression=compression)
    assert isinstance(response, hist_pb2.FillResponse)

    fill_many_calls = _randomized_categorical_fill_calls(
        axis_names=axis_names,
        rng=rng,
        n_calls=5,
        samples_per_call=64,
    )
    for fill_kwargs in fill_many_calls:
        local_hist.fill(**fill_kwargs)
    response = remote_hist.fill_many(fill_many_calls, compression=compression)
    assert isinstance(response, hist_pb2.FillResponse)

    tail_fill: dict[str, object] = {
        "x": np.array([-4.5, -0.25, 2.0], dtype=np.float32),
        "weight": np.array([0.75, 1.25, 1.5], dtype=np.float32),
    }
    for axis_index, axis_name in enumerate(axis_names):
        tail_fill[axis_name] = f"tail_{axis_name}_{axis_index % 2}"
    local_hist.fill(**tail_fill)
    response = remote_hist.fill(**tail_fill, compression=compression)
    assert isinstance(response, hist_pb2.FillResponse)

    remote_snapshot = remote_hist.snapshot().to_hist()
    for axis_name in axis_names:
        axis_index = remote_snapshot.axes.name.index(axis_name)
        expected_index = local_hist.axes.name.index(axis_name)
        assert list(remote_snapshot.axes[axis_index]) == list(
            local_hist.axes[expected_index]
        )
    _assert_hist_views_match(
        remote_snapshot.view(flow=True),
        local_hist.view(flow=True),
    )


def test_remote_fill_rejects_unknown_per_rpc_compression(client: Client) -> None:
    remote_hist = client.init(regular_hist(), token="alice")

    with pytest.raises(ValueError, match="unsupported fill compression"):
        remote_hist.fill(
            x=np.array([0.25, 1.5, 3.75], dtype=np.float32),
            compression="bad",
        )


def test_remote_fill_many_rejects_unknown_per_rpc_compression(client: Client) -> None:
    remote_hist = client.init(regular_hist(), token="alice")

    with pytest.raises(ValueError, match="unsupported fill compression"):
        remote_hist.fill_many(
            [{"x": np.array([0.25, 1.5, 3.75], dtype=np.float32)}],
            compression="bad",
        )


def test_init_preserves_existing_bin_yields(client: Client) -> None:
    local = regular_hist()
    local.fill(x=np.array([0.25, 1.5, 3.75], dtype=np.float32))
    remote_hist = client.init(local, token="alice")
    np.testing.assert_equal(
        remote_hist.snapshot().to_hist().view(flow=True), local.view(flow=True)
    )


def test_client_connect_reconnects_existing_hist(client: Client) -> None:
    remote_hist = client.init(regular_hist(), token="alice")
    remote_hist.fill(x=np.array([0.25, 1.5, 3.75], dtype=np.float32))

    reconnect = client.connect(remote_hist.hist_id, token=remote_hist.token)

    assert reconnect.client.address == client.address
    assert reconnect.hist_id == remote_hist.hist_id
    assert reconnect.token == remote_hist.token
    np.testing.assert_equal(
        reconnect.snapshot().to_hist().view(flow=True),
        remote_hist.snapshot().to_hist().view(flow=True),
    )


def test_remote_hist_can_reconnect_from_connection_info(client: Client) -> None:
    remote_hist = client.init(regular_hist(), token="alice")
    remote_hist.fill(x=np.array([0.25, 1.5, 3.75], dtype=np.float32))

    connection_info = remote_hist.get_connection_info()
    reconnect = RemoteHist.from_connection_info(connection_info)

    assert reconnect.client.address == client.address
    assert reconnect.hist_id == remote_hist.hist_id
    assert reconnect.token == remote_hist.token
    np.testing.assert_equal(
        reconnect.snapshot().to_hist().view(flow=True),
        remote_hist.snapshot().to_hist().view(flow=True),
    )


def test_remote_fill_reject_with_duped_unique_id(client: Client) -> None:
    remote_hist = client.init(regular_hist(), token="alice")
    unique_id = ["foo", "bar"]

    assert remote_hist.was_filled_with_unique_id(unique_id) is False
    remote_hist.fill(
        x=np.array([0.25, 1.5, 3.75], dtype=np.float32), unique_id=unique_id
    )
    assert remote_hist.was_filled_with_unique_id(unique_id) is True

    with pytest.raises(grpc.RpcError, match="StatusCode.ALREADY_EXISTS"):
        remote_hist.fill(
            x=np.array([0.25, 1.5, 3.75], dtype=np.float32), unique_id=["foo", "bar"]
        )


@pytest.mark.parametrize(
    "case",
    INTEGRATION_CATEGORICAL_CASES,
    ids=lambda case: case.name,
)
def test_remote_fill_matches_local_hist_for_weighted_categorical_axes(
    client: Client,
    case: CategoricalHistCase,
) -> None:
    local_hist = case.make_hist()
    remote_hist = client.init(case.make_hist(), token="alice")

    for fill_kwargs in case.fill_calls:
        local_hist.fill(**fill_kwargs)
        response = remote_hist.fill(**fill_kwargs)
        assert isinstance(response, hist_pb2.FillResponse)

    remote_snapshot = remote_hist.snapshot().to_hist()
    for axis_name, expected in zip(
        case.axis_names, case.expected_axis_values, strict=True
    ):
        axis_index = remote_snapshot.axes.name.index(axis_name)
        assert list(remote_snapshot.axes[axis_index]) == expected
    np.testing.assert_equal(remote_snapshot.view(flow=True), local_hist.view(flow=True))


def test_snapshot_delete_from_server_removes_hist(client: Client) -> None:
    local_hist = regular_hist()
    remote_hist = client.init(regular_hist(), token="alice")

    fill_kwargs = {"x": np.array([0.5, 1.5], dtype=np.float32)}
    local_hist.fill(**fill_kwargs)
    remote_hist.fill(**fill_kwargs)

    dropped_snapshot = remote_hist.snapshot(delete_from_server=True)
    np.testing.assert_equal(
        dropped_snapshot.to_hist().view(flow=True), local_hist.view(flow=True)
    )

    with pytest.raises(grpc.RpcError, match="StatusCode.NOT_FOUND"):
        remote_hist.snapshot()


def test_missing_hist_id_raises_not_found(client: Client) -> None:
    remote_hist = client.init(regular_hist(), token="alice")
    remote_hist.snapshot(delete_from_server=True)
    with pytest.raises(grpc.RpcError, match="StatusCode.NOT_FOUND"):
        remote_hist.fill(x=np.array([0.5], dtype=np.float32))


def test_exists_reflects_presence_and_token_visibility(client: Client) -> None:
    remote_hist = client.init(regular_hist(), token="alice")
    with pytest.raises(grpc.RpcError, match="StatusCode.NOT_FOUND"):
        client.connect(remote_hist.hist_id, token="bob")

    assert remote_hist.exists()

    remote_hist.delete()

    assert not remote_hist.exists()


def test_delete_removes_histogram(client: Client) -> None:
    remote_hist = client.init(regular_hist(), token="alice")
    response = remote_hist.delete()

    assert isinstance(response, hist_pb2.DeleteResponse)
    assert not remote_hist.exists()

    with pytest.raises(grpc.RpcError, match="StatusCode.NOT_FOUND"):
        remote_hist.snapshot()


@pytest.mark.parametrize(
    "case",
    INTEGRATION_CATEGORICAL_CASES,
    ids=lambda case: case.name,
)
def test_reset_clears_contents_and_collapses_growable_categories(
    client: Client,
    case: CategoricalHistCase,
) -> None:
    remote_hist = client.init(case.make_hist(), token="alice")

    first_fill = dict(case.fill_calls[0])
    second_fill = dict(case.fill_calls[1])
    remote_hist.fill(**first_fill, unique_id="fill-before-reset")
    remote_hist.fill(**second_fill)

    response = remote_hist.reset()
    assert isinstance(response, hist_pb2.ResetResponse)

    reset_snapshot = remote_hist.snapshot().to_hist()
    expected_reset = case.make_hist()
    for axis_name in case.axis_names:
        axis_index = reset_snapshot.axes.name.index(axis_name)
        assert list(reset_snapshot.axes[axis_index]) == []
    np.testing.assert_equal(
        reset_snapshot.view(flow=True), expected_reset.view(flow=True)
    )


def test_remote_fill_rejects_array_values_for_int_categorical_axes(
    client: Client,
) -> None:
    remote_hist = client.init(_weighted_categorical_hist(), token="alice")
    invalid_fill = {
        "x": np.array([0.5, 1.5], dtype=np.float64),
        "cat": np.array([1, 1], dtype=np.int64),
        "weight": np.array([1.0, 2.0], dtype=np.float64),
    }

    with pytest.raises(
        ValueError,
        match=r"categorical chunk axis 'cat' only accepts scalar int/str values",
    ):
        remote_hist.fill(**invalid_fill)


def test_get_connection_info_returns_reconnectable_payload(client: Client) -> None:
    remote_hist = client.init(regular_hist(), token="alice")

    assert remote_hist.get_connection_info() == {
        "address": client.address,
        "hist_id": remote_hist.hist_id,
        "token": "alice",
    }


def test_connect_round_trip_via_connection_info(client: Client) -> None:
    remote_hist = client.init(regular_hist(), token="alice")
    connection_info = remote_hist.get_connection_info()
    reconnected = RemoteHist.from_connection_info(connection_info)

    remote_hist.fill(x=np.array([0.25, 1.5], dtype=np.float32))
    reconnected.fill(x=np.array([3.25], dtype=np.float32))

    expected = regular_hist()
    expected.fill(x=np.array([0.25, 1.5], dtype=np.float32))
    expected.fill(x=np.array([3.25], dtype=np.float32))

    snapshot = reconnected.snapshot()
    np.testing.assert_equal(
        snapshot.to_hist().view(flow=True), expected.view(flow=True)
    )


def test_different_token_cannot_access_histogram(client: Client) -> None:
    remote_hist = client.init(regular_hist(), token="alice")
    with pytest.raises(grpc.RpcError, match="StatusCode.NOT_FOUND"):
        client.connect(remote_hist.hist_id, token="bob")


def test_remote_slice_snapshot_returns_only_selected_chunks(client: Client) -> None:
    remote_hist = client.init(
        Hist.new.Reg(4, 0, 4, name="x")
        .IntCat([], growth=True, name="cat")
        .StrCat([], growth=True, name="var")
        .Weight()
    )
    remote_hist.fill(
        x=np.array([0.5], dtype=np.float32),
        cat=1,
        var="a",
        weight=np.array([1.0], dtype=np.float32),
    )
    remote_hist.fill(
        x=np.array([1.5], dtype=np.float32),
        cat=1,
        var="b",
        weight=np.array([1.0], dtype=np.float32),
    )
    remote_hist.fill(
        x=np.array([2.5], dtype=np.float32),
        cat=2,
        var="a",
        weight=np.array([1.0], dtype=np.float32),
    )

    sliced = remote_hist[{"cat": 1}].snapshot().to_hist()

    assert list(sliced.axes["cat"]) == [1]
    np.testing.assert_equal(sliced.sum(flow=True).value, 2.0)


def test_flush_writes_hdf5_file_and_removes_hist(
    client: Client, tmp_path
) -> None:
    h5py = pytest.importorskip("h5py")
    remote_hist = client.init(regular_hist(), token="alice")
    remote_hist.fill(x=np.array([0.5, 1.5], dtype=np.float32))

    destination = tmp_path / "flushed.h5"
    remote_hist.flush(str(destination))

    assert not remote_hist.exists()
    with h5py.File(destination) as h5_file:
        assert remote_hist.hist_id in h5_file


def test_flush_without_h5py_reports_missing_extra(
    client: Client, monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    remote_hist = client.init(regular_hist(), token="alice")

    # Simulate a server installed without the optional hdf5 extra.
    import sys

    monkeypatch.setitem(sys.modules, "h5py", None)
    with pytest.raises(
        grpc.RpcError, match=r"FAILED_PRECONDITION.*histserv\[hdf5\]"
    ):
        remote_hist.flush(str(tmp_path / "flushed.h5"))
    monkeypatch.undo()

    # The histogram is untouched by the failed flush.
    assert remote_hist.exists()


def test_token_rpc_counters_dropped_with_last_histogram(
    client: Client, grpc_server
) -> None:
    first = client.init(regular_hist(), token="alice")
    second = client.init(regular_hist(), token="alice")
    first.fill(x=np.array([0.5], dtype=np.float32))

    def alice_counters() -> list[tuple[str, str]]:
        histogrammer = grpc_server._server.histogrammer
        return [key for key in histogrammer._rpc_calls_by_token if key[0] == "alice"]

    assert alice_counters()

    # Counters survive while the token still owns a histogram ...
    first.delete()
    assert alice_counters()

    # ... and are dropped with its last histogram.
    second.delete()
    assert not alice_counters()


def test_fill_many_sends_one_chunk_per_distinct_key(
    client: Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    def make_hist() -> Hist:
        return Hist(
            hist.axis.Regular(8, 0, 1, name="x"),
            hist.axis.StrCategory([], growth=True, name="cat"),
        )

    local_hist = make_hist()
    remote_hist = client.init(make_hist(), token="alice")

    stub = remote_hist.client.stub
    captured: list[hist_pb2.FillManyRequest] = []
    original_fill_many = stub.FillMany

    def spy(request: hist_pb2.FillManyRequest, **kwargs: object):
        captured.append(request)
        return original_fill_many(request, **kwargs)

    monkeypatch.setattr(stub, "FillMany", spy)

    fill_calls = [
        {"x": [0.1], "cat": "a"},
        {"x": [0.2], "cat": "b"},
        {"x": [0.3], "cat": "a"},
        {"x": [0.4], "cat": "a"},
    ]
    for fill_kwargs in fill_calls:
        local_hist.fill(**fill_kwargs)
    remote_hist.fill_many(fill_calls)

    # Four fills over two distinct chunk keys -> two dense payloads.
    assert len(captured) == 1
    assert len(captured[0].chunks) == 2

    remote_snapshot = remote_hist.snapshot()
    np.testing.assert_equal(
        remote_snapshot.to_hist().view(flow=True), local_hist.view(flow=True)
    )
