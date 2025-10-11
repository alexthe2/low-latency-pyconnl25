from array import array
import asyncio
from collections import deque
from dataclasses import dataclass, field
import gc
import logging
import os
import socket
import struct
import threading
import time
from typing import Deque, Literal

import msgspec
import numpy as np
import uvloop

from pycon.models import Message

now_monotonic = time.monotonic

logger: logging.Logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

DEFAULT_SHARDS = max(2, min(8, os.cpu_count() or 4))
DEFAULT_WORKERS = max(2, os.cpu_count() or 4)
DEFAULT_QUEUE_MAX = 200_000
SOCKET_RCVBUF_BYTES = 8 * 1024 * 1024

ACTIVE_WINDOW_SEC = 30.0
LOG_STATS_INTERVAL = 10.0


RING_CAP = 65536  # power of two; big enough to avoid wrap in one interval
RING_MASK = RING_CAP - 1

# ---------- Grid config (London bbox) ----------
GRID_ROWS, GRID_COLS = 20, 20
LAT_MIN, LAT_MAX = 51.30, 51.50
LON_MIN, LON_MAX = -0.20, 0.00


def _coord_to_cell(lat: float, lon: float) -> tuple[int, int] | None:
    if not (LAT_MIN <= lat <= LAT_MAX and LON_MIN <= lon <= LON_MAX):
        return None
    lat_f = (lat - LAT_MIN) / (LAT_MAX - LAT_MIN) if LAT_MAX > LAT_MIN else 0.0
    lon_f = (lon - LON_MIN) / (LON_MAX - LON_MIN) if LON_MAX > LON_MIN else 0.0
    r = min(max(int(lat_f * GRID_ROWS), 0), GRID_ROWS - 1)
    c = min(max(int(lon_f * GRID_COLS), 0), GRID_COLS - 1)
    return (r, c)


def configure_gc(disable: bool = True) -> None:
    try:
        gc.freeze()
    except Exception:
        pass
    if disable:
        gc.disable()


def _safe_deque_list(dq: Deque) -> list:
    # Convert to list; retry once if another thread mutates during iteration.
    for _ in range(2):
        try:
            return list(dq)
        except RuntimeError:
            time.sleep(0)  # yield

    # Last resort: copy after yielding
    return list(dq)


def _drain_proc_ring(stats: "ShardStats", which: str) -> list[float]:
    """Drain all new durations for the chosen consumer ('stats' or 'agg')."""
    out: list[float] = []
    head = stats.proc_head
    tail_attr = "proc_tail_stats" if which == "stats" else "proc_tail_agg"
    tail = getattr(stats, tail_attr)
    if head - tail > RING_CAP:  # overrun: drop oldest
        tail = head - RING_CAP
    while tail != head:
        idx = tail & RING_MASK
        dt_s = stats.proc_ring[idx]
        if dt_s > 0.0:
            out.append(dt_s)

        tail += 1
    setattr(stats, tail_attr, tail)
    return out


def msgs_delta_for(stats: "ShardStats", which: str) -> int:
    if which == "stats":
        prev = stats.last_total_stats
        cur = stats.total_msgs
        stats.last_total_stats = cur
    else:
        prev = stats.last_total_agg
        cur = stats.total_msgs
        stats.last_total_agg = cur
    d = cur - prev
    return d if d > 0 else 0


def _percentiles_ms_from_seconds(
    durations_s: list[float], ps: list[float]
) -> dict[float, float]:
    if not durations_s:
        return {p: 0.0 for p in ps}

    arr = np.fromiter(durations_s, dtype=np.float64) * 1000.0
    vals = np.percentile(arr, ps, method="linear")
    return dict(zip(ps, vals.tolist()))


_csv_fh = None


def _open_csv():
    global _csv_fh
    if _csv_fh is None:
        new = not os.path.exists("report.csv")
        _csv_fh = open("report.csv", "a", buffering=1)  # line buffered
        if new:
            _csv_fh.write(
                "timestamp,ok_msgs,lost_msgs,ooo_msgs,queuefull_msgs,attempts,"
                "loss_percent,throughput,avg_proc_time_ms,"
                "p50_ms,p90_ms,p99_ms,p99_9_ms,active_peers,"
                "bufcall_p50,bufcall_p90,bufcall_p99\n"
            )
    return _csv_fh


@dataclass(slots=True)
class ShardStats:
    # Peers + queuefull
    peers_last_seen: dict[tuple[str, int], float] = field(default_factory=dict)
    dropped_queue_full: int = 0
    recent_queuefull: Deque[float] = field(default_factory=deque)

    # Totals since start
    start_time: float = field(default_factory=now_monotonic)
    total_msgs: int = 0
    lost_msgs: int = 0
    dropped_out_of_order: int = 0
    unknown_loc_readings: int = 0
    proc_time_total: float = 0.0  # seconds

    # Per-sensor sequencing
    per_sensor_last_seq: dict[str, int] = field(default_factory=dict)

    # Rolling window (last ACTIVE_WINDOW_SEC)
    recent_lost: Deque[float] = field(default_factory=deque)  # each lost counted once
    recent_ooo: Deque[float] = field(default_factory=deque)  # ooo drops

    # durations for processed messages (seconds)
    proc_ring: array = field(default_factory=lambda: array("d", [0.0]) * RING_CAP)
    proc_head: int = 0

    proc_tail_stats: int = 0  # consumed by stats_task
    proc_tail_agg: int = 0  # consumed by aggregate_loop
    msg_tail_stats: int = 0
    msg_tail_agg: int = 0

    last_total_stats: int = 0
    last_total_agg: int = 0

    count_per_buffer_call: Deque[int] = field(
        default_factory=lambda: deque(maxlen=10000)
    )

    def prune_recent(self, window: float = ACTIVE_WINDOW_SEC) -> None:
        now = now_monotonic()
        cutoff = now - window
        # loss subtypes
        while self.recent_lost and self.recent_lost[0] < cutoff:
            self.recent_lost.popleft()
        while self.recent_ooo and self.recent_ooo[0] < cutoff:
            self.recent_ooo.popleft()
        while self.recent_queuefull and self.recent_queuefull[0] < cutoff:
            self.recent_queuefull.popleft()

    def active_connections_count(self, now: float | None = None) -> int:
        if now is None:
            now = now_monotonic()
        cutoff = now - ACTIVE_WINDOW_SEC
        stale = [addr for addr, ts in self.peers_last_seen.items() if ts < cutoff]
        for addr in stale:
            self.peers_last_seen.pop(addr, None)
        return len(self.peers_last_seen)

    def report_total(self, shard_id: int, qsize: int) -> None:
        now = now_monotonic()
        elapsed = now - self.start_time
        throughput = self.total_msgs / elapsed if elapsed > 0 else 0.0
        avg_proc_ms = self.proc_time_total / max(self.total_msgs, 1) * 1000.0
        tot_ok = self.total_msgs
        tot_lost = self.lost_msgs
        tot_ooo = self.dropped_out_of_order
        tot_qfull = self.dropped_queue_full
        attempts = tot_ok + tot_lost + tot_ooo + tot_qfull
        loss_pct = (
            (100.0 * (tot_lost + tot_ooo + tot_qfull) / attempts) if attempts else 0.0
        )
        logger.info(
            f"[S{shard_id} STATS total] elapsed={elapsed:.1f}s, "
            f"ok={tot_ok}, lost={tot_lost}, ooo={tot_ooo}, queuefull={tot_qfull}, "
            f"attempts={attempts}, loss%={loss_pct:.2f}, throughput={throughput:.2f} msg/s, "
            f"avg_proc_time={avg_proc_ms:.3f} ms/msg, queue_size={qsize}, "
            f"unknown_loc_readings={self.unknown_loc_readings}"
        )


@dataclass(slots=True)
class ShardState:
    shard_id: int
    # Assigned inside shard thread
    stop_event: asyncio.Event | None = None
    raw_queue: asyncio.Queue[tuple[bytes, tuple[str, int]]] | None = None

    # Live stats
    stats: ShardStats = field(default_factory=ShardStats)

    # Grid state (per shard)
    cell_latest: list[list[dict[str, float]]] = field(
        default_factory=lambda: [
            [{} for _ in range(GRID_COLS)] for _ in range(GRID_ROWS)
        ]
    )
    sensor_loc: dict[str, tuple[float, float]] = field(default_factory=dict)
    sensor_cell: dict[str, tuple[int, int] | None] = field(default_factory=dict)

    cell_sum: list[list[float]] = field(
        default_factory=lambda: [[0.0] * GRID_COLS for _ in range(GRID_ROWS)]
    )
    cell_cnt: list[list[int]] = field(
        default_factory=lambda: [[0] * GRID_COLS for _ in range(GRID_ROWS)]
    )

    def _move_sensor_cell(self, sensor_id: str, new_rc: tuple[int, int] | None) -> None:
        old_rc = self.sensor_cell.get(sensor_id)
        if old_rc is not None and old_rc != new_rc:
            r, c = old_rc
            self.cell_latest[r][c].pop(sensor_id, None)
        self.sensor_cell[sensor_id] = new_rc

    def _update_cell_with_reading(self, sensor_id: str, temp_c: float) -> None:
        rc = self.sensor_cell.get(sensor_id)
        if rc is None:
            return
        r, c = rc
        prev = self.cell_latest[r][c].get(sensor_id)
        if prev is None:
            self.cell_cnt[r][c] += 1
            self.cell_sum[r][c] += temp_c
        else:
            self.cell_sum[r][c] += temp_c - prev
        self.cell_latest[r][c][sensor_id] = float(temp_c)


def _apply_sequence_bookkeeping(state: ShardState, seq_src, seq_nr) -> bool:
    """
    Returns True if the message should be processed, False if dropped
    because it's out-of-order (seq_nr < last).
    Updates lost_msgs for gaps and per_sensor_last_seq on accept.
    """
    now = now_monotonic()
    last = state.stats.per_sensor_last_seq.get(seq_src)
    if last is not None:
        if seq_nr < last:
            state.stats.dropped_out_of_order += 1
            state.stats.recent_ooo.append(now)
            return False  # drop older packet
        if seq_nr > last + 1:
            missed = seq_nr - (last + 1)
            state.stats.lost_msgs += missed
            state.stats.recent_lost.extend([now] * missed)
    state.stats.per_sensor_last_seq[seq_src] = seq_nr
    return True


LEN = struct.Struct(format="!I")  # 4-byte big-endian length prefix


class TCPBufferedProtocol(asyncio.BufferedProtocol):
    __slots__: tuple[
        Literal["state"],
        Literal["transport"],
        Literal["_buf"],
        Literal["_mv"],
        Literal["_w"],
        Literal["_r"],
        Literal["_expected"],
    ] = ("state", "transport", "_buf", "_mv", "_w", "_r", "_expected")

    def __init__(self, state: ShardState):
        self.state = state
        self.transport: asyncio.BaseTransport | None = None
        # One reusable receive buffer per connection.
        # Size to your largest expected frame; grow-once if needed.
        self._buf = bytearray(1 << 20)  # 1 MiB to start; adjust as needed
        self._mv = memoryview(self._buf)
        self._w = 0  # write cursor
        self._r = 0  # read cursor (start of next frame)
        self._expected = -1  # -1 means "need length"

    # ---- lifecycle ----
    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport
        sock = transport.get_extra_info("socket")
        if sock is not None:
            try:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except Exception:
                pass

    def get_buffer(self, sizehint: int) -> memoryview:
        # Ensure capacity; grow geometrically (rare) to keep amortized O(1).
        need = self._w + max(sizehint, 4096)
        if need > len(self._buf):
            newcap = max(need, len(self._buf) * 2)
            nb = bytearray(newcap)
            nb[: self._w - self._r] = self._buf[self._r : self._w]
            self._buf = nb
            self._mv = memoryview(self._buf)
            self._w -= self._r
            self._r = 0
        return self._mv[self._w :]

    def buffer_updated(self, nbytes: int) -> None:
        if nbytes <= 0:
            return
        self._w += nbytes
        self._process_frames()

    def connection_lost(self, exc: Exception | None) -> None:
        # no ref cycles; GC can stay off
        self.transport = None

    # ---- parsing ----
    def _process_frames(self) -> None:
        st_stats = self.state.stats
        mv = self._mv
        r = self._r
        w = self._w
        local_proc_vals = []  # local list of dt_s for this callback
        local_total_msgs = 0

        # --- fast path, no logging or attribute traffic ---
        while True:
            if self._expected < 0:
                if w - r < LEN.size:
                    break
                (length,) = LEN.unpack_from(mv, r)
                r += LEN.size
                self._expected = length
                if not (0 <= length <= 8 * 1024 * 1024):
                    if self.transport:
                        self.transport.close()
                    return

            if w - r < self._expected:
                break

            t0 = time.perf_counter_ns()
            ok = True
            try:
                msg = _loads_from_mem(mv, r, self._expected)
            except Exception as e:
                ok = False
                if logger.isEnabledFor(logging.WARNING):
                    logger.warning(f"[S{self.state.shard_id}] TCP decode error: {e!r}")

            if ok:
                seq_src = msg.sequence_src
                seq_nr = msg.sequence_nr
                if (seq_src is None) or (
                    isinstance(seq_nr, int)
                    and _apply_sequence_bookkeeping(self.state, seq_src, seq_nr)
                ):
                    self._apply_message(self.state, msg)
                    local_total_msgs += 1

            dt_s = (time.perf_counter_ns() - t0) * 1e-9
            local_proc_vals.append(dt_s)
            r += self._expected
            self._expected = -1

        # ---- commit once ----
        st_stats.total_msgs += local_total_msgs
        st_stats.count_per_buffer_call.append(local_total_msgs)
        st_stats.proc_time_total += sum(local_proc_vals)
        # push durations to ring using local vars to minimize attribute hits
        proc_head = st_stats.proc_head
        ring = st_stats.proc_ring
        mask = RING_MASK
        for dt in local_proc_vals:
            ring[proc_head & mask] = dt
            proc_head += 1
        st_stats.proc_head = proc_head

        self._r, self._w = r, w

    def _apply_message(self, st: ShardState, msg: Message) -> None:
        st_stats = st.stats
        kind = msg.kind
        payload = msg.payload
        if kind == "registration":
            # payload = cast(RegistrationMessage, payload)
            sid = payload.sensor_id
            loc = payload.location
            if sid and isinstance(loc, (list, tuple)) and len(loc) == 2:
                try:
                    lat = float(loc[0])
                    lon = float(loc[1])
                    st.sensor_loc[sid] = (lat, lon)
                    st._move_sensor_cell(sid, _coord_to_cell(lat, lon))
                except (TypeError, ValueError):
                    pass
        elif kind == "reading":
            # payload = cast(SensorReading, payload)
            reading = payload.temperature_reading
            if reading is None:
                return
            sid = reading.sensor_id
            tc = reading.temperature_celsius
            if sid is not None and isinstance(tc, (int, float)):
                if sid in st.sensor_loc:
                    st._update_cell_with_reading(sid, float(tc))
                else:
                    st_stats.unknown_loc_readings += 1


async def stats_task(state: ShardState):
    """
    Periodically read ring buffers (no hot-path wall clocks),
    stamp a single time.time() per tick, and compute interval + ~30s-window stats.
    """
    st = state.stats
    window_durations: Deque[tuple[float, float]] = deque()  # (stamp, dt_s)
    window_msgs: Deque[tuple[float, int]] = deque()

    LOG_INT = LOG_STATS_INTERVAL

    while True:
        await asyncio.sleep(LOG_INT)

        # --- Drain rings since last read (lock-free SPSC style) ---
        # read durations
        proc = _drain_proc_ring(st, "stats")
        msg_count = msgs_delta_for(st, "stats")

        now = now_monotonic()
        cutoff = now - ACTIVE_WINDOW_SEC

        # Tag the whole batch with one wall-clock stamp (keeps tails clean)
        if msg_count:
            window_msgs.append((now, msg_count))

        for dt in proc:
            window_durations.append((now, dt))

        # prune 30s window
        while window_msgs and window_msgs[0][0] < cutoff:
            window_msgs.popleft()

        while window_durations and window_durations[0][0] < cutoff:
            window_durations.popleft()

        # compute interval stats
        interval_msgs = msg_count
        interval_avg_ms = (sum(proc) / len(proc) * 1000.0) if proc else 0.0
        pct_i = _percentiles_ms_from_seconds(proc, [50.0, 90.0, 99.0, 99.9])
        ip50 = pct_i[50.0]
        ip90 = pct_i[90.0]
        ip99 = pct_i[99.0]
        ip999 = pct_i[99.9]

        # compute ~30s window stats
        win_durs = [d for _, d in window_durations]
        win_avg_ms = (sum(win_durs) / len(win_durs) * 1000.0) if win_durs else 0.0
        pct_w = _percentiles_ms_from_seconds(win_durs, [50.0, 90.0, 99.0, 99.9])
        wp50 = pct_w[50.0]
        wp90 = pct_w[90.0]
        wp99 = pct_w[99.0]
        wp999 = pct_w[99.9]

        # loss/ooo/queuefull still use wall-time from their (rare) events
        # prune their windows here too
        st.prune_recent(ACTIVE_WINDOW_SEC)
        ok = sum(cnt for _, cnt in window_msgs)
        lost = len(st.recent_lost)
        ooo = len(st.recent_ooo)
        qfull = len(st.recent_queuefull)
        attempts = ok + lost + ooo + qfull
        loss_pct = (100.0 * (lost + ooo + qfull) / attempts) if attempts else 0.0

        # Log
        logger.info(
            f"[S{state.shard_id} INTERVAL {LOG_INT:.0f}s] "
            f"msgs={interval_msgs}, avg_proc={interval_avg_ms:.3f} ms, "
            f"p50={ip50:.3f} ms, p90={ip90:.3f} ms, p99={ip99:.3f} ms, p99.9={ip999:.3f} ms"
        )
        logger.info(
            f"[S{state.shard_id} WINDOW {int(ACTIVE_WINDOW_SEC)}s] "
            f"ok={ok}, lost={lost}, ooo={ooo}, queuefull={qfull}, attempts={attempts}, loss%={loss_pct:.2f}, "
            f"throughput={ok / ACTIVE_WINDOW_SEC:.2f} msg/s, "
            f"avg_proc_time={win_avg_ms:.3f} ms, "
            f"p50={wp50:.3f} ms, p90={wp90:.3f} ms, p99={wp99:.3f} ms, p99.9={wp999:.3f} ms, "
            f"active_peers~={st.active_connections_count(now)}, queuefull_recent={qfull}"
        )


def aggregate_loop(states: list[ShardState], interval: float = 5.0):
    """
    Periodically snapshot all shards and print combined stats and a combined grid (every ~30s).
    Uses snapshot copies to avoid 'mutated during iteration' errors.
    """
    last_grid = 0.0
    while True:
        try:
            time.sleep(interval)

            tot_ok = tot_lost = tot_ooo = tot_qfull = tot_unknown = 0
            tot_proc = 0.0
            tot_recent_ok = tot_recent_lost = tot_recent_ooo = tot_recent_qfull = 0
            tot_recent_proc_sum = 0.0
            tot_recent_proc_cnt = 0
            active_peers = 0
            queues = 0
            all_durations_s: list[float] = []

            now = now_monotonic()
            starts: list[float] = []

            for st in states:
                # Snapshot rolling deques FIRST (avoid iter races)
                rec_lost = _safe_deque_list(st.stats.recent_lost)
                rec_ooo = _safe_deque_list(st.stats.recent_ooo)
                rec_qfull = _safe_deque_list(st.stats.recent_queuefull)

                # Totals
                tot_ok += st.stats.total_msgs
                tot_lost += st.stats.lost_msgs
                tot_ooo += st.stats.dropped_out_of_order
                tot_qfull += st.stats.dropped_queue_full
                tot_unknown += st.stats.unknown_loc_readings
                tot_proc += st.stats.proc_time_total
                starts.append(st.stats.start_time)

                msg_count = msgs_delta_for(st.stats, "agg")
                durations = _drain_proc_ring(st.stats, "agg")

                # Recent window
                tot_recent_ok += msg_count
                tot_recent_lost += len(rec_lost)
                tot_recent_ooo += len(rec_ooo)
                tot_recent_qfull += len(rec_qfull)

                if durations:
                    tot_recent_proc_sum += sum(durations)
                    tot_recent_proc_cnt += len(durations)
                    all_durations_s.extend(durations)

                # Active peers & queue sizes (best-effort)
                active_peers += st.stats.active_connections_count(now)
                queues += st.raw_queue.qsize() if st.raw_queue else 0

            attempts = tot_ok + tot_lost + tot_ooo + tot_qfull
            loss_pct = (
                (100.0 * (tot_lost + tot_ooo + tot_qfull) / attempts)
                if attempts
                else 0.0
            )

            recent_attempts = (
                tot_recent_ok + tot_recent_lost + tot_recent_ooo + tot_recent_qfull
            )
            recent_loss_pct = (
                (
                    100.0
                    * (tot_recent_lost + tot_recent_ooo + tot_recent_qfull)
                    / recent_attempts
                )
                if recent_attempts
                else 0.0
            )
            recent_avg_proc_ms = (
                (tot_recent_proc_sum / tot_recent_proc_cnt * 1000.0)
                if tot_recent_proc_cnt
                else 0.0
            )
            throughput = tot_recent_ok / interval  # msgs/sec over this tick

            pct = _percentiles_ms_from_seconds(
                all_durations_s, [50.0, 90.0, 99.0, 99.9]
            )
            p50, p90, p99, p999 = pct[50.0], pct[90.0], pct[99.0], pct[99.9]

            all_counts = []
            for st in states:
                all_counts.extend(_safe_deque_list(st.stats.count_per_buffer_call))

            count_pcts = [50.0, 90.0, 99.0]
            if all_counts:
                count_arr = np.array(all_counts, dtype=np.float64)
                cp50, cp90, cp99 = np.percentile(count_arr, count_pcts, method="linear")
            else:
                cp50 = cp90 = cp99 = 0.0

            logger.info(
                f"[AGG last {int(ACTIVE_WINDOW_SEC)}s] "
                f"ok={tot_recent_ok}, lost={tot_recent_lost}, ooo={tot_recent_ooo}, queuefull={tot_recent_qfull}, "
                f"attempts={recent_attempts}, loss%={recent_loss_pct:.2f}, "
                f"throughput={tot_recent_ok / ACTIVE_WINDOW_SEC:.2f} msg/s, "
                f"avg_proc_time={recent_avg_proc_ms:.3f} ms/msg, "
                f"p50={p50:.3f} ms, p90={p90:.3f} ms, p99={p99:.3f} ms, p99.9={p999:.3f} ms, "
                f"active_peers~={active_peers}, total_queue_size~{queues}"
                f"buffer_call_count_p50={cp50:.1f}, p90={cp90:.1f}, p99={cp99:.1f}"
            )

            # write to report.csv file
            csv_exists = os.path.exists("report.csv")
            fh = _open_csv()

            fh.write(
                f"{time.strftime('%Y-%m-%d %H:%M:%S')},"
                f"{tot_recent_ok},{tot_recent_lost},{tot_recent_ooo},{tot_recent_qfull},"
                f"{recent_attempts},{recent_loss_pct:.2f},{tot_recent_ok / ACTIVE_WINDOW_SEC:.2f},"
                f"{recent_avg_proc_ms:.3f},"
                f"{p50:.3f},{p90:.3f},{p99:.3f},{p999:.3f},"
                f"{active_peers},"
                f"{cp50:.1f},{cp90:.1f},{cp99:.1f}\n"
            )

            start0 = min(starts) if starts else now
            elapsed_min = max((now - start0) / 60.0, 0.0001)
            avg_proc_ms_total = tot_proc / max(tot_ok, 1) * 1000.0
            logger.info(
                f"[AGG total ~{elapsed_min:.1f}m] ok={tot_ok}, lost={tot_lost}, ooo={tot_ooo}, queuefull={tot_qfull}, "
                f"loss%={loss_pct:.2f}, avg_proc_time={avg_proc_ms_total:.3f} ms/msg, unknown_loc_readings={tot_unknown}"
            )

            # Combined grid every ~30s — snapshot cell dict values before summing
            if now - last_grid >= 30.0:
                logger.info(
                    "[AGG GRID avg °C] 20x20 lat[%.2f..%.2f] lon[%.2f..%.2f]",
                    LAT_MIN,
                    LAT_MAX,
                    LON_MIN,
                    LON_MAX,
                )
                for r in range(GRID_ROWS):
                    row_vals = []
                    for c in range(GRID_COLS):
                        sum_temp = 0.0
                        cnt = 0
                        cell_sum = 0.0
                        cell_cnt = 0
                        for st in states:
                            cell_sum += st.cell_sum[r][c]
                            cell_cnt += st.cell_cnt[r][c]
                        row_vals.append(
                            f"{(cell_sum / cell_cnt):.1f}" if cell_cnt else "----"
                        )

                    logger.info("AG row%02d: %s", r, " ".join(row_vals))
                last_grid = now

        except Exception as e:
            # If something still slips through, don’t kill the loop; just log and continue.
            logger.error(f"Aggregator loop error: {e}")


_JSON_DECODER = msgspec.json.Decoder(type=Message)


def _loads(data: bytes) -> Message:
    return _JSON_DECODER.decode(data)


def _loads_from_mem(mv: memoryview, start: int, n: int) -> Message:
    # msgspec accepts buffer-compatible objects; mv[start:start+n] is zero-copy view
    return _JSON_DECODER.decode(mv[start : start + n])


def _pin_to_this_thread_cpu(cpu: int) -> None:
    try:
        os.sched_setaffinity(0, {cpu})  # current thread
    except AttributeError:
        pass  # non-Linux


def run_tcp_shard(
    state: ShardState,
    host: str,
    port: int,
    queue_max: int,
    workers: int,
    backlog: int = 65535,
):
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)

    stop_event = asyncio.Event()
    state.stop_event = stop_event

    async def run():
        server = await loop.create_server(
            lambda: TCPBufferedProtocol(state),
            host=host,
            port=port,
            reuse_port=True,
            backlog=backlog,
        )
        addrs = ", ".join(str(s.getsockname()) for s in server.sockets or [])
        logger.info(f"[S{state.shard_id}] TCP listening on {addrs} (BufferedProtocol)")

        stats_handle = asyncio.create_task(stats_task(state))
        try:
            await stop_event.wait()
        finally:
            stats_handle.cancel()
            server.close()
            await server.wait_closed()

    _pin_to_this_thread_cpu(state.shard_id % (os.cpu_count() or 1))

    try:
        loop.run_until_complete(run())
    finally:
        loop.stop()
        loop.close()


def run_server(
    host: str = "0.0.0.0",
    port: int = 9000,
    shards: int = 0,
    workers: int = 0,
    queue_max: int = DEFAULT_QUEUE_MAX,
    **kwargs,
) -> None:
    configure_gc()

    if shards <= 0:
        shards = DEFAULT_SHARDS
    if workers <= 0:
        workers = DEFAULT_WORKERS
    assert 1 <= shards <= 256 and 1 <= workers <= 256
    states: list[ShardState] = [ShardState(shard_id=i) for i in range(shards)]

    threads: list[threading.Thread] = []
    for st in states:
        t = threading.Thread(
            target=run_tcp_shard,
            args=(st, host, port, queue_max, workers),
            daemon=True,
            name=f"tcp-shard-{st.shard_id}",
        )
        threads.append(t)
        t.start()

    # reuse your existing aggregator
    t_agg = threading.Thread(
        target=aggregate_loop, args=(states, 5.0), daemon=True, name="agg-thread"
    )
    t_agg.start()

    logger.info(
        f"TCP server running with {shards} shards and {workers} workers per shard"
    )
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        logger.info("Server shutting down...")
