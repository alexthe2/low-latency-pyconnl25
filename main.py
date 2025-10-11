from __future__ import annotations

import argparse
import logging
import signal
import sys
from types import FrameType
from typing import NoReturn


from pycon.sensor import run_sensor
from pycon.server import run_server


def register_shutdown_handler() -> None:
    """Exit cleanly on SIGTERM."""

    def _sigterm(_signum: int, _frame: FrameType | None) -> NoReturn:
        sys.exit(0)

    signal.signal(signal.SIGTERM, _sigterm)


def configure_logging(log_file: str | None, log_level: str) -> None:
    fmt = "%(asctime)s %(levelname)s %(filename)s:%(lineno)s %(name)s %(message)s"
    level = getattr(logging, log_level.upper(), logging.INFO)

    if log_file:
        logging.basicConfig(level=level, format=fmt, filename=log_file, force=True)
    else:
        logging.basicConfig(level=level, format=fmt, stream=sys.stdout, force=True)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--log-file", "-l")
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )
    p.add_argument("--mode", choices=["server", "sensor"], default="server")

    p.add_argument("--host", default="127.0.0.1", help="Server host/IP")
    p.add_argument("--port", type=int, default=9000, help="Server TCP port")

    rate = p.add_mutually_exclusive_group()
    rate.add_argument(
        "--messages-per-minute",
        type=int,
        help="Average messages per minute (sensor mode)",
    )
    rate.add_argument(
        "--messages-per-second",
        type=float,
        help="Average messages per second (sensor mode)",
    )

    args = p.parse_args()

    # Normalize to messages_per_minute for sensor mode
    if args.messages_per_minute is None and args.messages_per_second is None:
        args.messages_per_minute = 60  # default = 1 msg/sec
    elif args.messages_per_second is not None:
        args.messages_per_minute = int(args.messages_per_second * 60)

    return args


def main() -> None:
    args = parse_args()
    configure_logging(args.log_file, args.log_level)
    register_shutdown_handler()

    logging.getLogger(__name__).info("Starting in %s mode on %s:%d", args.mode, args.host, args.port)

    if args.mode == "server":
        run_server(host=args.host, port=args.port)
    else:
        run_sensor(
            host=args.host,
            port=args.port,
            messages_per_minute=args.messages_per_minute,
        )


if __name__ == "__main__":
    main()
