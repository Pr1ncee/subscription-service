"""Enumerations used by the consumers process."""

from enum import StrEnum


class Outcome(StrEnum):
    """How a consumed message is settled with RabbitMQ (see `acking.settle`)."""

    ACK = "ack"
    RETRY = "retry"
    DEAD_LETTER = "dead_letter"
