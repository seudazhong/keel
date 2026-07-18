"""Generate/reuse the runtime DB login password + a libpq pgpass file (M3A, WS-DB).

Compose/K8s bootstrap helper (mirrors the sandbox ``keel-secret-init``, but for the non-owner
runtime DB login). It writes two files into a mounted secret volume:

* the raw password (consumed once by :mod:`keel_core.provision_runtime_cli` via
  ``--password-file`` to mint/repair the login), and
* a libpq ``pgpass`` file (``host:port:db:user:password``) that keel-server/keel-worker use for
  PASSWORD-LESS connects (``PGPASSFILE``), so the runtime secret never appears in a URL, in argv,
  in the process environment, or in logs.

Both files are written ``0600`` via a sibling tempfile + :func:`os.replace` (atomic — a reader
never sees a partial secret). Idempotent: an existing non-empty password is reused so re-running
never desyncs the pgpass from the already-provisioned login; only the pgpass is refreshed.
Otherwise a fresh password is generated with :func:`secrets.token_urlsafe` and never printed.
"""

from __future__ import annotations

import os
import secrets
import tempfile
from pathlib import Path

# token_urlsafe(48) -> 64 url-safe chars ([A-Za-z0-9_-]); it contains no ':' or '\\', so it is
# safe UNESCAPED in a libpq pgpass field (whose field separator is ':' and escape char is '\\').
_PASSWORD_ENTROPY_BYTES = 48

_DEFAULT_PASSWORD_PATH = "/keel-secrets-db/runtime_db_password"
_DEFAULT_PGPASS_PATH = "/keel-secrets-db/runtime_pgpass"


def _write_secret_file(path: Path, content: str) -> None:
    """Atomically write ``content`` to ``path`` with 0600 perms (owner read/write only)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix="." + path.name + ".")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
        os.chmod(tmp, 0o600)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def ensure_runtime_secret(
    *,
    password_path: Path,
    pgpass_path: Path,
    host: str,
    port: str,
    dbname: str,
    user: str,
) -> str:
    """Ensure the password + pgpass files exist (0600); return a status word (never the secret).

    Reuses an existing non-empty password (so the pgpass stays in sync with the already-provisioned
    login) and refreshes only the pgpass; otherwise generates a new password. The pgpass line is
    ``host:port:dbname:user:password``. The generated password contains no ``:``/``\\`` so it needs
    no escaping; a defensive check rejects anything that would corrupt the pgpass line.
    """
    try:
        password = password_path.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeError):
        password = ""
    if password:
        os.chmod(password_path, 0o600)
        status = "reused"
    else:
        password = secrets.token_urlsafe(_PASSWORD_ENTROPY_BYTES)
        _write_secret_file(password_path, password + "\n")
        status = "generated"
    if any(ch in password for ch in (":", "\\", "\n", "\r")):
        # Defensive: token_urlsafe never produces these, but never emit a pgpass line a stray
        # value would split or truncate (which could silently change the matched credential).
        raise ValueError("runtime password contains a character unsafe for a libpq pgpass field")
    _write_secret_file(pgpass_path, f"{host}:{port}:{dbname}:{user}:{password}\n")
    return status


def main() -> int:
    password_path = Path(os.environ.get("KEEL_RUNTIME_DB_PASSWORD_FILE", _DEFAULT_PASSWORD_PATH))
    pgpass_path = Path(os.environ.get("KEEL_RUNTIME_PGPASS_FILE", _DEFAULT_PGPASS_PATH))
    status = ensure_runtime_secret(
        password_path=password_path,
        pgpass_path=pgpass_path,
        host=os.environ.get("KEEL_RUNTIME_DB_HOST", "postgres"),
        port=os.environ.get("KEEL_RUNTIME_DB_PORT", "5432"),
        dbname=os.environ.get("KEEL_RUNTIME_DB_NAME", "keel"),
        user=os.environ.get("KEEL_RUNTIME_DB_USER", "keel_runtime_login"),
    )
    # Never print the secret: only a status word + the non-secret pgpass path.
    print(
        f"keel runtime DB password {status} (0600); "
        f"pgpass written to {pgpass_path}; value not logged"
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - module entry
    raise SystemExit(main())
