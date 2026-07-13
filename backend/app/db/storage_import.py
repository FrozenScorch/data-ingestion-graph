"""Atomically import legacy bind-mounted data into writable appliance volumes."""

from __future__ import annotations

import os
import shutil
from pathlib import Path

_MARKER_NAME = ".legacy-import-complete"
_STAGING_NAME = ".legacy-import-staging"


def import_legacy_tree(source: Path, destination: Path) -> bool:
    """Import once without overwriting volume data; return True when work was done."""
    destination.mkdir(parents=True, exist_ok=True)
    marker = destination / _MARKER_NAME
    if marker.exists():
        return False

    staging = destination / _STAGING_NAME
    shutil.rmtree(staging, ignore_errors=True)
    staging.mkdir()
    try:
        for child in source.iterdir():
            staged = staging / child.name
            if child.is_symlink():
                raise RuntimeError(f"legacy storage contains unsupported symlink: {child}")
            if child.is_dir():
                shutil.copytree(child, staged, symlinks=True)
            else:
                shutil.copy2(child, staged)

        for staged in sorted(staging.rglob("*"), key=lambda path: len(path.parts)):
            relative = staged.relative_to(staging)
            target = destination / relative
            if staged.is_symlink():
                raise RuntimeError(f"legacy storage contains unsupported symlink: {relative}")
            if staged.is_dir():
                if target.exists() and not target.is_dir():
                    raise RuntimeError(f"legacy directory conflicts with volume file: {relative}")
                target.mkdir(parents=True, exist_ok=True)
            elif target.is_dir():
                raise RuntimeError(f"legacy file conflicts with volume directory: {relative}")
            elif not target.exists():
                target.parent.mkdir(parents=True, exist_ok=True)
                os.replace(staged, target)
        shutil.rmtree(staging)
        marker_temp = destination / f"{_MARKER_NAME}.tmp"
        marker_temp.write_text("legacy bind import completed\n", encoding="utf-8")
        os.replace(marker_temp, marker)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return True


def chown_tree(path: Path, user: str = "appuser") -> None:
    import pwd

    account = pwd.getpwnam(user)
    for root, directories, files in os.walk(path):
        os.chown(root, account.pw_uid, account.pw_gid)
        for name in (*directories, *files):
            os.chown(Path(root) / name, account.pw_uid, account.pw_gid)


def main() -> None:
    pairs = (
        (Path("/legacy/uploads"), Path("/app/data/uploads")),
        (Path("/legacy/temp"), Path("/app/data/temp")),
    )
    for source, destination in pairs:
        import_legacy_tree(source, destination)
        chown_tree(destination)


if __name__ == "__main__":
    main()
