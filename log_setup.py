#!/usr/bin/env python3
"""
Centralized logging configuration for announce_scanner.

Stack: standard logging + ANSI ColorFormatter.
Colors are enabled for TTY (Windows console + Linux/VPS terminal).
Default level is DEBUG, override via env LOG_LEVEL.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timezone
from logging.handlers import TimedRotatingFileHandler

LOG_NAME = "announce_scanner"

# ANSI Colors
_ANSI = {
    "RESET":   "\033[0m",
    "GREY":    "\033[90m",
    "BLUE":    "\033[34m",
    "CYAN":    "\033[36m",
    "GREEN":   "\033[32m",
    "YELLOW":  "\033[33m",
    "RED":     "\033[31m",
    "BOLD":    "\033[1m",
}

# Level labels -> Color
_LEVEL_COLOR = {
    logging.DEBUG:    "\033[90m",   # dim grey
    logging.INFO:     "\033[36m",   # cyan
    logging.WARNING:  "\033[33m",   # yellow
    logging.ERROR:    "\033[31m",   # red
    logging.CRITICAL: "\033[31m\033[1m",  # bold red
}

# Soft colors for message text (256-color ANSI)
_MSG_COLOR = {
    logging.DEBUG:    "\033[38;5;245m",   # soft grey
    logging.INFO:     "\033[38;5;114m",   # soft green
    logging.WARNING:  "\033[38;5;179m",   # soft amber
    logging.ERROR:    "\033[38;5;174m",   # soft red
    logging.CRITICAL: "\033[38;5;174m",   # soft red
}

_LEVEL_LABEL = {
    logging.DEBUG:    "DEBUG",
    logging.INFO:     "INFO",
    logging.WARNING:  "WARN",
    logging.ERROR:    "ERROR",
    logging.CRITICAL: "CRIT",
}


def _enable_windows_vt() -> None:
    """Enable VT processing in Windows console."""
    if sys.platform != "win32":
        return
    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32
        # ENABLE_VIRTUAL_TERMINAL_PROCESSING = 0x0004
        kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
    except Exception:  # noqa: BLE001
        pass


def _ensure_utf8_stdout() -> None:
    """Ensure stdout/stderr support UTF-8."""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")
        except Exception:  # noqa: BLE001
            pass


class ColorFormatter(logging.Formatter):
    """
    Format: timestamp  LEVEL  message
    """
    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.now(timezone.utc).astimezone().strftime("%H:%M:%S")
        level_color = _LEVEL_COLOR.get(record.levelno, "")
        msg_color = _MSG_COLOR.get(record.levelno, "")
        label = _LEVEL_LABEL.get(record.levelno, record.levelname)
        reset = _ANSI["RESET"]
        msg = record.getMessage()
        return (
            f"{_ANSI['GREY']}{ts}{reset} "
            f"{level_color}{label:<5}{reset} "
            f"{msg_color}{msg}{reset}"
        )


def setup_logging(level: int | None = None) -> logging.Logger:
    """Initializes the centralized logger."""
    _enable_windows_vt()
    _ensure_utf8_stdout()

    logger = logging.getLogger(LOG_NAME)
    if level is None:
        env_level = os.environ.get("LOG_LEVEL", "").upper()
        level = getattr(logging, env_level, logging.DEBUG) if env_level else logging.DEBUG

    logger.setLevel(level)
    logger.propagate = False

    for h in list(logger.handlers):
        logger.removeHandler(h)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(ColorFormatter())
    logger.addHandler(handler)

    # File handler with daily rotation
    os.makedirs("logs", exist_ok=True)
    file_handler = TimedRotatingFileHandler(
        "logs/announce_scanner.log",
        when="midnight",
        interval=1,
        backupCount=30,
        encoding="utf-8",
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s  %(levelname)-5s %(message)s", datefmt="%H:%M:%S"),
    )
    logger.addHandler(file_handler)

    return logger


def get_logger() -> logging.Logger:
    """Returns the centralized logger instance."""
    return logging.getLogger(LOG_NAME)
