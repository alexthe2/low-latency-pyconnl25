from collections.abc import Callable
from dataclasses import asdict, is_dataclass
import json
import logging
import random
import socket
import string
import struct
import time
import uuid
import zlib

import msgspec

from pycon.models import Message, RegistrationMessage, SensorReading, TemperatureReading

logger: logging.Logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


def random_sensor_id(length: int = 8) -> str:
    """Generate a random sensor ID like sns-abc12345."""
    alphabet = string.ascii_lowercase + string.digits
    return "sns-" + "".join(random.choice(alphabet) for _ in range(length))


def random_location() -> tuple[float, float]:
    """Generate a random (latitude, longitude) location within London area."""
    lat = 51.30 + random.uniform(0, 0.2)  # ~51.30 to ~51.50
    lon = -0.20 + random.uniform(0, 0.2)  # ~-0.20 to ~0.00
    return (round(lat, 6), round(lon, 6))


class Sensor:
    def __init__(self, sensor_id: str, location: tuple[float, float]):
        self.sensor_id = sensor_id
        self.location = location
        self.sequence_src = uuid.uuid4().hex  # stable per sensor
        self.sequence_nr = 0

    def _next_seq(self) -> int:
        self.sequence_nr += 1
        return self.sequence_nr

    def build_registration(self) -> Message:
        reg = RegistrationMessage(
            sensor_id=self.sensor_id,
            sensor_type=["temperature"],
            location=self.location,
        )
        return Message(
            kind="registration",
            payload=reg,
            sequence_src=self.sequence_src,
            sequence_nr=self._next_seq(),
        )

    def build_temperature(self, temp_c: float) -> Message:
        reading = TemperatureReading(
            sensor_id=self.sensor_id,
            timestamp=time.time(),
            temperature_celsius=temp_c,
        )
        return Message(
            kind="reading",
            payload=SensorReading(temperature_reading=reading),
            sequence_src=self.sequence_src,
            sequence_nr=self._next_seq(),
        )


def _as_plain(obj):
    """Convert dataclasses (and nested dataclasses) to plain Python containers."""
    if is_dataclass(obj):
        return asdict(obj)

    if hasattr(obj, "__dict__"):
        return {k: _as_plain(v) for k, v in obj.__dict__.items()}

    return obj


def _to_tcp_payload(msg: Message, *, json_impl: str, framing: str) -> bytes:
    """Encode message as bytes and frame for TCP."""
    # Encode without newline first
    if json_impl == "msgpack":
        body = msgspec.msgpack.encode(msg)
    elif json_impl == "msgspec":
        body = msgspec.json.encode(msg)
    else:  # "json"
        body = json.dumps(_as_plain(msg), separators=(",", ":")).encode("utf-8")

    if framing == "len":
        # 4-byte big-endian length prefix
        return struct.pack("!I", len(body)) + body
    elif framing == "nl":
        return body + b"\n"
    else:
        raise ValueError("framing must be 'len' or 'nl'")


class _TCPSenderPool:
    """
    Maintain N persistent TCP connections and round-robin sends.
    Reconnects with exponential backoff if a socket dies.
    """

    def __init__(
        self,
        host: str,
        port: int,
        *,
        connections: int = 2,
        json_impl: str = "json",
        framing: str = "len",
        connect_timeout: float = 10.0,
    ):
        assert connections >= 1
        self.host = host
        self.port = port
        self.conns: list[socket.socket | None] = [None] * connections
        self.next_idx = 0
        self.connect_timeout = connect_timeout
        self.framing = framing  # "len" (length-prefixed) or "nl" (\n-delimited)
        self.json_impl = json_impl
        self._ensure_all_connected()

    def _connect_one(self, idx: int) -> None:
        backoff = 0.25
        while True:
            try:
                s = socket.create_connection(
                    (self.host, self.port), timeout=self.connect_timeout
                )
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                try:
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                except Exception:
                    pass
                self.conns[idx] = s
                return
            except Exception as e:
                # Jittered exponential backoff, capped
                sleep_s = min(5.0, backoff) * (0.5 + random.random())
                logger.warning(
                    f"[TCP-{idx}] connect failed: {e!r}; retrying in {sleep_s:.2f}s"
                )
                time.sleep(sleep_s)
                backoff = min(backoff * 2.0, 5.0)

    def _ensure_all_connected(self) -> None:
        for i in range(len(self.conns)):
            if self.conns[i] is None:
                self._connect_one(i)

    def _pick(self) -> int:
        i = self.next_idx
        self.next_idx = (self.next_idx + 1) % len(self.conns)
        return i

    def send(self, msg: Message) -> None:
        payload = _to_tcp_payload(msg, json_impl=self.json_impl, framing=self.framing)

        # --- choose a sticky connection based on the sensor's source id ---
        key = (msg.sequence_src or "default").encode("utf-8")
        idx = zlib.crc32(key) % len(self.conns)

        # ensure socket exists
        if self.conns[idx] is None:
            self._connect_one(idx)

        s = self.conns[idx]

        try:
            s.sendall(payload)
            return
        except Exception as e:
            logger.warning(f"[TCP-{idx}] send failed ({e!r}); reconnecting socket")
            try:
                if s:
                    s.close()
            except Exception:
                pass
            self.conns[idx] = None
            self._connect_one(idx)
            # one retry on the same idx for simplicity
            self.conns[idx].sendall(payload)

    def close(self) -> None:
        for i, s in enumerate(self.conns):
            if s:
                try:
                    s.shutdown(socket.SHUT_RDWR)
                except Exception:
                    pass
                try:
                    s.close()
                except Exception:
                    pass
                self.conns[i] = None


def make_tcp_sender(
    host: str = "server",
    port: int = 9000,
    *,
    connections: int = 2,
    json_impl: str = "json",
    framing: str = "len",
) -> Callable[[Message], None]:
    pool = _TCPSenderPool(
        host, port, connections=connections, json_impl=json_impl, framing=framing
    )

    def send(msg: Message) -> None:
        pool.send(msg)

    return send


def run_sensor(
    host: str = "server",
    port: int = 9000,
    messages_per_minute: int = 60,
    *,
    tcp_connections: int = 2,  # >1 helps throughput/latency under loss
    registration_every_s: float = 10.0,
    json_impl: str = "msgspec",
    **kwargs,
) -> None:
    sensor = Sensor(sensor_id=random_sensor_id(), location=random_location())

    sender = make_tcp_sender(
        host, port, connections=tcp_connections, json_impl=json_impl, framing="len"
    )

    # Average interval in seconds
    base_interval = 60.0 / max(messages_per_minute, 1)

    # Desync startup
    time.sleep(random.uniform(0, min(1.0, base_interval)))

    # Always register once at startup
    sender(sensor.build_registration())
    logger.info(f"Registered sensor {sensor.sensor_id}")

    sent_count = 0
    last_report = time.time()
    last_registration = time.time()

    while True:
        try:
            # Random temperature around 20°C
            temp = round(20.0 + random.uniform(-5.0, 5.0), 2)
            msg = sensor.build_temperature(temp)
            sender(msg)
            sent_count += 1

            # Jittered interval
            interval = random.uniform(0.5 * base_interval, 1.5 * base_interval)
            time.sleep(interval)

            now = time.time()
            # periodic registration
            if now - last_registration >= registration_every_s:
                sender(sensor.build_registration())
                last_registration = now

            # Report throughput every 10s
            if now - last_report >= 10.0:
                rate = sent_count / (now - last_report)
                logger.info(
                    "[%s] sending rate: %.2f msg/s (over last %.1fs)",
                    sensor.sensor_id,
                    rate,
                    now - last_report,
                )
                sent_count = 0
                last_report = now

        except Exception as e:
            logger.warning("Send error for %s: %r", sensor.sensor_id, e)
            # Gentle backoff; TCP path will also reconnect inside the pool
            time.sleep(0.5 + random.random())
