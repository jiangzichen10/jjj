"""Single-instance lock based on exclusive file creation."""

from __future__ import annotations

import json
import os
import socket
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


class RunnerAlreadyActive(RuntimeError):
    pass


class SingleRunnerLock:
    def __init__(self, path: Path):
        self.path = Path(path).resolve()
        self._fd: Optional[int] = None

    def acquire(self) -> "SingleRunnerLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "pid": os.getpid(),
            "host": socket.gethostname(),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            self._fd = os.open(
                str(self.path), os.O_CREAT | os.O_EXCL | os.O_WRONLY
            )
        except FileExistsError as exc:
            detail = self.path.read_text(encoding="utf-8", errors="replace")
            raise RunnerAlreadyActive(
                f"Runner lock already exists: {self.path}; owner={detail.strip()}"
            ) from exc
        os.write(self._fd, json.dumps(payload, ensure_ascii=False).encode("utf-8"))
        os.fsync(self._fd)
        return self

    def release(self) -> None:
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None
            try:
                self.path.unlink()
            except FileNotFoundError:
                pass

    def __enter__(self) -> "SingleRunnerLock":
        return self.acquire()

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()
