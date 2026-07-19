"""Unit coverage for the deterministic patch snapshot transfer archive (WS-PP, M4 P3a).

No Postgres/Redis/network. Proves the archive is byte-deterministic, that every bound is
fail-closed, that untrusted paths and special tar entries are rejected, and that applying a
sandbox export onto a disposable worktree honours the binary/mode/delete policy.
"""

from __future__ import annotations

import gzip
import io
import os
import stat
import sys
import tarfile
from pathlib import Path

import pytest

from keel_core.patch.errors import (
    PatchBoundsExceeded,
    PatchPolicyViolation,
    PatchValidationError,
)
from keel_core.patch.transfer import (
    DEFAULT_SNAPSHOT_BOUNDS,
    SnapshotBounds,
    SnapshotFile,
    apply_export_to_worktree,
    build_snapshot_archive,
    build_snapshot_from_directory,
    parse_snapshot_archive,
    scan_directory_for_snapshot,
    validate_snapshot_path,
)

_POSIX_ONLY = pytest.mark.skipif(sys.platform == "win32", reason="POSIX file modes")


def _craft_archive(members: list[tuple[tarfile.TarInfo, bytes | None]]) -> bytes:
    """Serialize arbitrary tar members into a gzip archive (for hostile-input tests)."""

    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w", format=tarfile.GNU_FORMAT) as tar:
        for info, data in members:
            tar.addfile(info, io.BytesIO(data) if data is not None else None)
    gz = io.BytesIO()
    with gzip.GzipFile(fileobj=gz, mode="wb", mtime=0) as handle:
        handle.write(raw.getvalue())
    return gz.getvalue()


def _reg(name: str, data: bytes) -> tuple[tarfile.TarInfo, bytes]:
    info = tarfile.TarInfo(name)
    info.type = tarfile.REGTYPE
    info.size = len(data)
    return info, data


def _special(name: str, typ: bytes, *, link: str = "") -> tuple[tarfile.TarInfo, None]:
    info = tarfile.TarInfo(name)
    info.type = typ
    info.linkname = link
    return info, None


# --- Determinism + roundtrip ---------------------------------------------------------


def test_build_is_byte_deterministic(tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_bytes(b"one\n")
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "b.py").write_bytes(b"print(1)\n")
    first, manifest_first = build_snapshot_from_directory(tmp_path)
    second, manifest_second = build_snapshot_from_directory(tmp_path)
    assert first == second
    assert manifest_first == manifest_second


def test_roundtrip_preserves_exact_bytes_and_flags(tmp_path: Path) -> None:
    (tmp_path / "crlf.txt").write_bytes(b"line1\r\nline2\r\n")
    (tmp_path / "img.bin").write_bytes(b"\x00\x01\x02\x00rest")
    archive, manifest = build_snapshot_from_directory(tmp_path)
    parsed = parse_snapshot_archive(archive)
    assert parsed.files["crlf.txt"].data == b"line1\r\nline2\r\n"
    assert parsed.files["crlf.txt"].binary is False
    assert parsed.files["img.bin"].binary is True
    by_path = {entry.path: entry for entry in manifest.entries}
    assert by_path["img.bin"].binary is True
    assert by_path["crlf.txt"].sha256 == parsed.files["crlf.txt"].sha256
    assert manifest.total_bytes == sum(entry.size for entry in manifest.entries)


def test_manifest_is_sorted_by_path(tmp_path: Path) -> None:
    for name in ("zeta.txt", "alpha.txt", "mid.txt"):
        (tmp_path / name).write_bytes(b"x")
    _, manifest = build_snapshot_from_directory(tmp_path)
    assert list(manifest.paths) == ["alpha.txt", "mid.txt", "zeta.txt"]


# --- Forbidden / .git handling -------------------------------------------------------


def test_build_skips_git_and_secret_paths(tmp_path: Path) -> None:
    (tmp_path / "keep.txt").write_bytes(b"keep")
    (tmp_path / ".git").mkdir()
    (tmp_path / ".git" / "config").write_bytes(b"[core]")
    (tmp_path / "id_rsa").write_bytes(b"PRIVATE")
    (tmp_path / ".env").write_bytes(b"SECRET=1")
    _, manifest = build_snapshot_from_directory(tmp_path)
    assert list(manifest.paths) == ["keep.txt"]


def test_parse_rejects_forbidden_path_in_archive() -> None:
    archive = _craft_archive([_reg(".git/config", b"[core]")])
    with pytest.raises(PatchPolicyViolation):
        parse_snapshot_archive(archive)


# --- Path validation -----------------------------------------------------------------


@pytest.mark.parametrize(
    "bad",
    ["../escape", "a\\b", "/abs", "~user", "C:/win", "a/./b", "a/../b", "", "a//b"],
)
def test_validate_snapshot_path_rejects_hostile(bad: str) -> None:
    with pytest.raises((PatchValidationError, PatchBoundsExceeded)):
        validate_snapshot_path(bad)


def test_validate_snapshot_path_rejects_nul() -> None:
    with pytest.raises(PatchValidationError):
        validate_snapshot_path("a\x00b")


def test_validate_snapshot_path_rejects_overlong() -> None:
    bounds = SnapshotBounds(max_path_chars=8)
    with pytest.raises(PatchBoundsExceeded):
        validate_snapshot_path("way-too-long/path.txt", bounds=bounds)


@pytest.mark.parametrize("bad", ["../evil", "a\\b", "/abs", "sub/../../x"])
def test_parse_rejects_traversal_variants(bad: str) -> None:
    archive = _craft_archive([_reg(bad, b"data")])
    with pytest.raises(PatchValidationError):
        parse_snapshot_archive(archive)


def test_parse_rejects_duplicate_and_casefold_collision() -> None:
    dup = _craft_archive([_reg("a.txt", b"x"), _reg("a.txt", b"y")])
    with pytest.raises(PatchValidationError):
        parse_snapshot_archive(dup)
    collide = _craft_archive([_reg("a.txt", b"x"), _reg("A.txt", b"y")])
    with pytest.raises(PatchValidationError):
        parse_snapshot_archive(collide)


# --- Special tar entries fail closed -------------------------------------------------


@pytest.mark.parametrize(
    "member",
    [
        _special("link", tarfile.SYMTYPE, link="target"),
        _special("hard", tarfile.LNKTYPE, link="a.txt"),
        _special("chardev", tarfile.CHRTYPE),
        _special("blockdev", tarfile.BLKTYPE),
        _special("fifo", tarfile.FIFOTYPE),
        _special("adir", tarfile.DIRTYPE),
    ],
)
def test_parse_rejects_special_entries(member: tuple[tarfile.TarInfo, None]) -> None:
    archive = _craft_archive([member])
    with pytest.raises(PatchPolicyViolation):
        parse_snapshot_archive(archive)


def test_parse_rejects_sparse_and_contiguous() -> None:
    for typ in (tarfile.GNUTYPE_SPARSE, tarfile.CONTTYPE):
        info = tarfile.TarInfo("f")
        info.type = typ
        info.size = 0
        with pytest.raises(PatchPolicyViolation):
            parse_snapshot_archive(_craft_archive([(info, b"")]))


# --- Bounds --------------------------------------------------------------------------


def test_parse_rejects_gzip_bomb_total_bound() -> None:
    payload = io.BytesIO()
    with gzip.GzipFile(fileobj=payload, mode="wb") as handle:
        handle.write(b"\x00" * (32 * 1024 * 1024))
    bounds = SnapshotBounds(max_total_bytes=1_000_000, max_archive_bytes=10_000_000)
    with pytest.raises(PatchBoundsExceeded):
        parse_snapshot_archive(payload.getvalue(), bounds=bounds)


def test_parse_rejects_oversized_compressed_body() -> None:
    archive = _craft_archive([_reg("a.txt", b"hello")])
    bounds = SnapshotBounds(max_archive_bytes=4)
    with pytest.raises(PatchBoundsExceeded):
        parse_snapshot_archive(archive, bounds=bounds)


def test_parse_rejects_per_file_and_count_bounds() -> None:
    too_big = _craft_archive([_reg("a.txt", b"x" * 100)])
    with pytest.raises(PatchBoundsExceeded):
        parse_snapshot_archive(too_big, bounds=SnapshotBounds(max_file_bytes=10))
    too_many = _craft_archive([_reg(f"f{i}.txt", b"x") for i in range(5)])
    with pytest.raises(PatchBoundsExceeded):
        parse_snapshot_archive(too_many, bounds=SnapshotBounds(max_files=2))


def test_build_enforces_bounds(tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_bytes(b"x" * 50)
    with pytest.raises(PatchBoundsExceeded):
        build_snapshot_from_directory(tmp_path, bounds=SnapshotBounds(max_file_bytes=10))


def test_snapshot_bounds_reject_non_positive() -> None:
    with pytest.raises(PatchValidationError):
        SnapshotBounds(max_files=0)


def test_parse_rejects_empty_and_non_bytes() -> None:
    with pytest.raises(PatchValidationError):
        parse_snapshot_archive(b"")
    with pytest.raises(PatchValidationError):
        parse_snapshot_archive("not-bytes")  # type: ignore[arg-type]
    with pytest.raises(PatchValidationError):
        parse_snapshot_archive(b"not a gzip stream at all")


# --- Filesystem special entries fail closed ------------------------------------------


def _can_symlink(tmp_path: Path) -> bool:
    probe = tmp_path / "_symlink_probe"
    probe.mkdir()
    target = probe / "target"
    target.write_bytes(b"x")
    link = probe / "link"
    ok = False
    try:
        os.symlink(target, link)
        ok = True
    except (OSError, NotImplementedError):
        ok = False
    finally:
        import shutil

        shutil.rmtree(probe, ignore_errors=True)
    return ok


def test_scan_fails_closed_on_symlink(tmp_path: Path) -> None:
    if not _can_symlink(tmp_path):
        pytest.skip("symlinks not permitted on this platform")
    (tmp_path / "real.txt").write_bytes(b"ok")
    os.symlink(tmp_path / "real.txt", tmp_path / "link.txt")
    with pytest.raises(PatchPolicyViolation):
        scan_directory_for_snapshot(tmp_path)


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="FIFO unsupported")
def test_scan_fails_closed_on_fifo(tmp_path: Path) -> None:
    (tmp_path / "real.txt").write_bytes(b"ok")
    os.mkfifo(tmp_path / "pipe")  # type: ignore[attr-defined]
    with pytest.raises(PatchPolicyViolation):
        scan_directory_for_snapshot(tmp_path)


def test_symlinked_git_is_skipped_not_failed(tmp_path: Path) -> None:
    if not _can_symlink(tmp_path):
        pytest.skip("symlinks not permitted on this platform")
    (tmp_path / "real.txt").write_bytes(b"ok")
    os.symlink(tmp_path, tmp_path / ".git")
    _, manifest = build_snapshot_from_directory(tmp_path)
    assert list(manifest.paths) == ["real.txt"]


# --- Apply: binary / mode / delete policy --------------------------------------------


def test_apply_writes_deletes_and_preserves(tmp_path: Path) -> None:
    (tmp_path / "keep.txt").write_bytes(b"orig\n")
    (tmp_path / "gone.txt").write_bytes(b"delete me\n")
    (tmp_path / "pic.bin").write_bytes(b"\x00PIC\x00")
    export, _ = build_snapshot_archive(
        [
            SnapshotFile("keep.txt", b"changed\n", False, False),
            SnapshotFile("pic.bin", b"\x00PIC\x00", False, True),
            SnapshotFile("new/created.txt", b"fresh\n", False, False),
        ]
    )
    result = apply_export_to_worktree(tmp_path, export)
    assert result.written == ("keep.txt", "new/created.txt")
    assert result.deleted == ("gone.txt",)
    assert result.unchanged_binaries == ("pic.bin",)
    assert (tmp_path / "keep.txt").read_bytes() == b"changed\n"
    assert not (tmp_path / "gone.txt").exists()
    assert (tmp_path / "new" / "created.txt").read_bytes() == b"fresh\n"
    assert (tmp_path / "pic.bin").read_bytes() == b"\x00PIC\x00"


def test_apply_new_file_is_not_executable(tmp_path: Path) -> None:
    export, _ = build_snapshot_archive([SnapshotFile("new.sh", b"#!/bin/sh\n", False, False)])
    apply_export_to_worktree(tmp_path, export)
    mode = os.stat(tmp_path / "new.sh").st_mode
    assert not mode & 0o111


@_POSIX_ONLY
def test_apply_new_file_forced_0644(tmp_path: Path) -> None:
    export, _ = build_snapshot_archive([SnapshotFile("new.txt", b"x", True, False)])
    apply_export_to_worktree(tmp_path, export)
    assert stat.S_IMODE(os.stat(tmp_path / "new.txt").st_mode) == 0o644


@_POSIX_ONLY
def test_apply_preserves_existing_mode(tmp_path: Path) -> None:
    script = tmp_path / "run.sh"
    script.write_bytes(b"old\n")
    os.chmod(script, 0o755)
    export, _ = build_snapshot_archive([SnapshotFile("run.sh", b"new\n", False, False)])
    apply_export_to_worktree(tmp_path, export)
    assert stat.S_IMODE(os.stat(script).st_mode) == 0o755
    assert script.read_bytes() == b"new\n"


def test_apply_blocks_binary_modification(tmp_path: Path) -> None:
    (tmp_path / "pic.bin").write_bytes(b"\x00OLD\x00")
    export, _ = build_snapshot_archive([SnapshotFile("pic.bin", b"\x00NEW\x00", False, True)])
    with pytest.raises(PatchPolicyViolation):
        apply_export_to_worktree(tmp_path, export)
    assert (tmp_path / "pic.bin").read_bytes() == b"\x00OLD\x00"


def test_apply_blocks_new_binary(tmp_path: Path) -> None:
    export, _ = build_snapshot_archive([SnapshotFile("added.bin", b"\x00NEW\x00", False, True)])
    with pytest.raises(PatchPolicyViolation):
        apply_export_to_worktree(tmp_path, export)


def test_apply_blocks_text_to_binary(tmp_path: Path) -> None:
    (tmp_path / "note.txt").write_bytes(b"text\n")
    export, _ = build_snapshot_archive([SnapshotFile("note.txt", b"\x00BIN\x00", False, True)])
    with pytest.raises(PatchPolicyViolation):
        apply_export_to_worktree(tmp_path, export)
    assert (tmp_path / "note.txt").read_bytes() == b"text\n"


def test_apply_leaves_omitted_binary_untouched(tmp_path: Path) -> None:
    (tmp_path / "pic.bin").write_bytes(b"\x00KEEP\x00")
    (tmp_path / "note.txt").write_bytes(b"old\n")
    export, _ = build_snapshot_archive([SnapshotFile("note.txt", b"new\n", False, False)])
    result = apply_export_to_worktree(tmp_path, export)
    assert (tmp_path / "pic.bin").read_bytes() == b"\x00KEEP\x00"
    assert result.deleted == ()


def test_apply_validates_before_mutation(tmp_path: Path) -> None:
    (tmp_path / "text.txt").write_bytes(b"safe\n")
    (tmp_path / "pic.bin").write_bytes(b"\x00OLD\x00")
    export, _ = build_snapshot_archive(
        [
            SnapshotFile("text.txt", b"would change\n", False, False),
            SnapshotFile("pic.bin", b"\x00HOSTILE\x00", False, True),
        ]
    )
    with pytest.raises(PatchPolicyViolation):
        apply_export_to_worktree(tmp_path, export)
    assert (tmp_path / "text.txt").read_bytes() == b"safe\n"


def test_apply_requires_existing_directory(tmp_path: Path) -> None:
    export, _ = build_snapshot_archive([SnapshotFile("a.txt", b"x", False, False)])
    with pytest.raises(PatchValidationError):
        apply_export_to_worktree(tmp_path / "missing", export)


def test_default_bounds_handle_large_tree() -> None:
    assert DEFAULT_SNAPSHOT_BOUNDS.max_files >= 10_000
