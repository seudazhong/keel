"""Shared project/coding storage root resolution (WS-R).

Server and worker MUST resolve the **same** filesystem path for project/coding storage so that
an artifact written by the worker (e.g. a review report) is readable by the server's report
APIs. In cloud this is a shared/RWX volume named by ``KEEL_PROJECT_STORAGE_ROOT``; a
container-local ``./.keel/projects`` default is only ever valid for single-host local
development. :func:`resolve_project_storage_root` centralizes that decision and
:func:`verify_shared_storage` fails closed if the root is not a usable, writable directory.
"""

from __future__ import annotations

import os
from pathlib import Path

# App environments treated as single-host local development, where a container-local default
# storage root is acceptable. Everything else is "cloud" and REQUIRES an explicit shared root.
_LOCAL_ENVS = frozenset({"dev", "local", "test"})

_LOCAL_DEFAULT = Path.cwd() / ".keel" / "projects"


class SharedStorageUnavailable(RuntimeError):
    """The shared project/coding storage root is not configured or not usable."""


def resolve_project_storage_root(configured_root: str, *, app_env: str) -> Path:
    """Return the project/coding storage root shared by server and worker.

    * A non-empty ``configured_root`` (``KEEL_PROJECT_STORAGE_ROOT``) is always honoured.
    * Otherwise, in a local/dev/test environment, fall back to ``./.keel/projects``.
    * Otherwise (cloud) raise :class:`SharedStorageUnavailable`: a container-local default would
      silently split server and worker storage, so review artifacts written by the worker would
      be invisible to the server. Fail closed instead.
    """
    root = configured_root.strip()
    if root:
        return Path(root).expanduser()
    if app_env.strip().lower() in _LOCAL_ENVS:
        return _LOCAL_DEFAULT
    raise SharedStorageUnavailable(
        "KEEL_PROJECT_STORAGE_ROOT must be set to a shared volume path in cloud environments; "
        "a container-local .keel/projects default is not allowed."
    )


def verify_shared_storage(root: Path) -> None:
    """Confirm the storage root exists and is writable (probe write/read/delete).

    Fails closed with :class:`SharedStorageUnavailable` so startup/readiness can surface a
    mis-mounted or read-only shared volume rather than silently degrading.
    """
    try:
        root.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise SharedStorageUnavailable(f"storage root {root} is not creatable: {exc}") from exc
    probe = root / ".keel-storage-probe"
    try:
        probe.write_bytes(b"ok")
        if probe.read_bytes() != b"ok":  # pragma: no cover - defensive
            raise SharedStorageUnavailable(f"storage root {root} failed a read-back probe")
    except OSError as exc:
        raise SharedStorageUnavailable(f"storage root {root} is not writable: {exc}") from exc
    finally:
        try:
            os.unlink(probe)
        except OSError:
            pass


__all__ = [
    "SharedStorageUnavailable",
    "resolve_project_storage_root",
    "verify_shared_storage",
]
