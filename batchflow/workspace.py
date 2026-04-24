"""
workspace.py — WorkspaceManager.

Replaces the ad-hoc ``_copy_bps_folder`` logic from eoPipelines.py with
a proper workspace that is:

- Versioned: records the source hash so a stale local copy is detected.
- Explicit: raises rather than silently proceeding with a stale tree.
- Instrument-aware: applies the same per-instrument overrides as before.

A workspace is a working directory containing:

    ./bps/              local copy of BPS YAML files
    ./logs/             per-node submission logs
    ./batchflow.db      SQLite state store (default name)
    ./workspace.json    metadata: source path, hash, instrument, created_at

Usage
-----
    ws = WorkspaceManager(
        source_bps_dir=Path(os.environ["EO_PIPE_DIR"]) / "bps",
        instrument="LSSTCam",
        work_dir=Path("."),
    )
    ws.prepare()           # copies bps/ if needed, raises if stale
    bps_dir = ws.bps_dir   # Path to local ./bps/
"""
from __future__ import annotations

import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path


class WorkspaceError(RuntimeError):
    pass


class WorkspaceManager:
    """
    Manages the local working directory for a workflow run.

    Parameters
    ----------
    source_bps_dir : Path
        The package-level bps/ directory (``$EO_PIPE_DIR/bps`` or similar).
    instrument : str
        Instrument name, e.g. ``"LSSTCam"``, ``"LSST-TS8"``, ``"LATISS"``.
        Controls which per-instrument overrides are applied.
    work_dir : Path
        Working directory for this run.  Defaults to the current directory.
    force_refresh : bool
        If True, always re-copy bps/ even if the local copy is current.
    """

    _METADATA_FILE = "workspace.json"
    _INSTRUMENTS_WITH_SUBDIR = {"LATISS"}
    _INSTRUMENTS_WITH_BPS_OVERRIDE = {"LSST-TS8"}

    def __init__(
        self,
        source_bps_dir: Path,
        instrument: str,
        work_dir: Path = Path("."),
        force_refresh: bool = False,
    ) -> None:
        self.source_bps_dir = Path(source_bps_dir)
        self.instrument     = instrument
        self.work_dir       = Path(work_dir)
        self.force_refresh  = force_refresh

    @property
    def bps_dir(self) -> Path:
        return self.work_dir / "bps"

    @property
    def log_dir(self) -> Path:
        return self.work_dir / "logs"

    @property
    def db_path(self) -> Path:
        return self.work_dir / "batchflow.db"

    @property
    def _metadata_path(self) -> Path:
        return self.work_dir / self._METADATA_FILE

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def prepare(self) -> None:
        """
        Ensure the workspace is ready for a workflow run.

        - Creates work_dir and log_dir if they don't exist.
        - Copies the bps/ tree if absent or stale (source hash changed).
        - Applies instrument-specific overrides.
        - Writes workspace.json for auditability.

        Raises
        ------
        WorkspaceError
            If the source bps/ directory does not exist.
        WorkspaceError
            If the local bps/ is stale AND force_refresh is False,
            prompting the caller to pass force_refresh=True explicitly.
        """
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(exist_ok=True)

        if not self.source_bps_dir.exists():
            raise WorkspaceError(
                f"Source BPS directory not found: {self.source_bps_dir}"
            )

        source_hash = self._hash_dir(self.source_bps_dir)

        if self.bps_dir.exists():
            meta = self._load_metadata()
            if meta and meta.get("source_hash") == source_hash and not self.force_refresh:
                # Local copy is current — nothing to do.
                return
            if not self.force_refresh:
                raise WorkspaceError(
                    f"Local bps/ at {self.bps_dir} is stale "
                    f"(source has changed at {self.source_bps_dir}). "
                    "Re-run with force_refresh=True to update it."
                )
            shutil.rmtree(self.bps_dir)

        self._copy_bps(source_hash)
        self._apply_instrument_overrides()
        self._write_metadata(source_hash)

    def summary(self) -> dict:
        """Return a dict describing the current workspace state."""
        meta = self._load_metadata() or {}
        return {
            "work_dir":   str(self.work_dir),
            "bps_dir":    str(self.bps_dir),
            "instrument": self.instrument,
            "bps_ready":  self.bps_dir.exists(),
            **meta,
        }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _copy_bps(self, source_hash: str) -> None:
        src = self.source_bps_dir
        # Instrument-specific subdirectory (e.g. auxtel/ for LATISS).
        if self.instrument in self._INSTRUMENTS_WITH_SUBDIR:
            subdir = self.instrument.lower().replace("latiss", "auxtel")
            src = src / subdir
            if not src.exists():
                raise WorkspaceError(
                    f"Instrument subdir not found: {src}"
                )
        shutil.copytree(src, self.bps_dir)

    def _apply_instrument_overrides(self) -> None:
        """
        Apply per-instrument file overrides after the initial copy.
        Mirrors the TS8 bps_cpPtc.yaml override from the original code.
        """
        if self.instrument in self._INSTRUMENTS_WITH_BPS_OVERRIDE:
            override_src = (
                self.source_bps_dir / "cp_pipe" / self.instrument / "bps_cpPtc.yaml"
            )
            override_dst = self.bps_dir / "cp_pipe" / "bps_cpPtc.yaml"
            if override_src.exists():
                override_dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy(override_src, override_dst)

    def _write_metadata(self, source_hash: str) -> None:
        meta = {
            "source_bps_dir": str(self.source_bps_dir),
            "source_hash":    source_hash,
            "instrument":     self.instrument,
            "created_at":     datetime.now(timezone.utc).isoformat(),
        }
        self._metadata_path.write_text(json.dumps(meta, indent=2))

    def _load_metadata(self) -> dict | None:
        if not self._metadata_path.exists():
            return None
        try:
            return json.loads(self._metadata_path.read_text())
        except (json.JSONDecodeError, OSError):
            return None

    @staticmethod
    def _hash_dir(path: Path) -> str:
        """
        Compute a stable SHA-256 over the names and contents of all files
        in *path* (sorted for determinism).
        """
        h = hashlib.sha256()
        for fpath in sorted(path.rglob("*")):
            if fpath.is_file():
                h.update(str(fpath.relative_to(path)).encode())
                h.update(fpath.read_bytes())
        return h.hexdigest()
