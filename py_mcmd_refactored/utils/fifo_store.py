from __future__ import annotations

import logging
import os
import stat
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Mapping, Optional


DualWritePathFactory = Callable[[str, str, str], Optional[Path]]


@dataclass(frozen=True)
class FifoEndpoint:
    engine: str
    step_id: str
    basename: str
    fifo_path: Path
    dual_write_path: Optional[Path] = None


@dataclass
class FifoStepResources:
    engine: str
    step_id: str
    step_dir: Path
    endpoints: dict[str, FifoEndpoint] = field(default_factory=dict)
    status: str = "prepared"  # prepared | success | failed


class FifoStore:
    """Owns FIFO resources for engine step outputs.

    Notes
    -----
    - This class does not know anything about orchestrator sequencing.
    - Callers decide which prior step to read from.
    - `finalize_step_success()` marks a step successful but intentionally does
      not delete it, because a following step may still need to consume it.
    - `finalize_step_failure()` cleans up the failed step immediately.
    - `cleanup_step()` and `cleanup_all()` provide deterministic teardown.
    """

    def __init__(
        self,
        *,
        root_dir: str | Path,
        output_basenames_by_engine: Mapping[str, list[str] | tuple[str, ...]],
        developer_mode: bool = False,
        dual_write_path_factory: Optional[DualWritePathFactory] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.root_dir = Path(root_dir)
        self.root_dir.mkdir(parents=True, exist_ok=True)

        self.developer_mode = bool(developer_mode)
        self.dual_write_path_factory = dual_write_path_factory
        self.logger = logger or logging.getLogger(__name__)

        self._output_basenames_by_engine = self._normalize_output_map(
            output_basenames_by_engine
        )
        self._steps: dict[tuple[str, str], FifoStepResources] = {}

    @staticmethod
    def _normalize_engine(engine: str) -> str:
        normalized = str(engine).strip().upper()
        if not normalized:
            raise ValueError("engine must be a non-empty string")
        return normalized

    @staticmethod
    def _normalize_step_id(step_id: str | int) -> str:
        value = str(step_id).strip()
        if not value:
            raise ValueError("step_id must be a non-empty string/int")
        return value

    @classmethod
    def _normalize_output_map(
        cls,
        output_basenames_by_engine: Mapping[str, list[str] | tuple[str, ...]],
    ) -> dict[str, tuple[str, ...]]:
        normalized: dict[str, tuple[str, ...]] = {}
        for engine, basenames in output_basenames_by_engine.items():
            eng = cls._normalize_engine(engine)
            cleaned: list[str] = []
            for raw_name in basenames:
                name = Path(raw_name).name
                if name != raw_name:
                    raise ValueError(
                        f"FIFO output names must be basenames only; got '{raw_name}'"
                    )
                if not name:
                    raise ValueError("FIFO output basename cannot be empty")
                cleaned.append(name)
            normalized[eng] = tuple(cleaned)
        return normalized

    def _key(self, engine: str, step_id: str | int) -> tuple[str, str]:
        eng = self._normalize_engine(engine)
        sid = self._normalize_step_id(step_id)
        if eng not in self._output_basenames_by_engine:
            raise ValueError(
                f"Unsupported engine '{engine}'. Expected one of: "
                f"{sorted(self._output_basenames_by_engine)}"
            )
        return eng, sid

    @staticmethod
    def _safe_unlink(path: Path) -> None:
        try:
            if path.exists() or path.is_symlink():
                path.unlink()
        except FileNotFoundError:
            pass

    @staticmethod
    def _is_fifo(path: Path) -> bool:
        try:
            return stat.S_ISFIFO(path.stat().st_mode)
        except FileNotFoundError:
            return False

    def prepare_step(self, engine: str, step_id: str | int) -> FifoStepResources:
        eng, sid = self._key(engine, step_id)
        key = (eng, sid)
        if key in self._steps:
            raise ValueError(f"FIFO resources already prepared for {eng} step {sid}")

        step_dir = self.root_dir / eng / sid
        step_dir.mkdir(parents=True, exist_ok=True)

        endpoints: dict[str, FifoEndpoint] = {}
        for basename in self._output_basenames_by_engine[eng]:
            fifo_path = step_dir / basename
            self._safe_unlink(fifo_path)
            os.mkfifo(fifo_path)

            dual_write_path: Optional[Path] = None
            if self.developer_mode and self.dual_write_path_factory is not None:
                dual_write_path = self.dual_write_path_factory(eng, sid, basename)
                if dual_write_path is not None:
                    dual_write_path.parent.mkdir(parents=True, exist_ok=True)

            endpoint = FifoEndpoint(
                engine=eng,
                step_id=sid,
                basename=basename,
                fifo_path=fifo_path,
                dual_write_path=dual_write_path,
            )
            endpoints[basename] = endpoint

            self.logger.info(
                "[FIFO] created engine=%s step=%s basename=%s fifo=%s%s",
                eng,
                sid,
                basename,
                fifo_path,
                f" dual_write={dual_write_path}" if dual_write_path else "",
            )

        resources = FifoStepResources(
            engine=eng,
            step_id=sid,
            step_dir=step_dir,
            endpoints=endpoints,
            status="prepared",
        )
        self._steps[key] = resources
        return resources

    def get_step(self, engine: str, step_id: str | int) -> FifoStepResources:
        eng, sid = self._key(engine, step_id)
        key = (eng, sid)
        if key not in self._steps:
            raise KeyError(f"No FIFO resources registered for {eng} step {sid}")
        return self._steps[key]

    def get_fifo_path(self, engine: str, step_id: str | int, basename: str) -> Path:
        resources = self.get_step(engine, step_id)
        normalized = Path(basename).name
        if normalized not in resources.endpoints:
            raise KeyError(
                f"FIFO basename '{basename}' is not registered for "
                f"{resources.engine} step {resources.step_id}"
            )
        return resources.endpoints[normalized].fifo_path

    def finalize_step_success(self, engine: str, step_id: str | int) -> None:
        resources = self.get_step(engine, step_id)
        resources.status = "success"
        self.logger.info(
            "[FIFO] finalized success engine=%s step=%s endpoints=%s",
            resources.engine,
            resources.step_id,
            sorted(resources.endpoints),
        )

    def finalize_step_failure(self, engine: str, step_id: str | int) -> None:
        resources = self.get_step(engine, step_id)
        resources.status = "failed"
        self.logger.info(
            "[FIFO] finalized failure engine=%s step=%s; cleaning up",
            resources.engine,
            resources.step_id,
        )
        self.cleanup_step(engine, step_id)

    def cleanup_step(self, engine: str, step_id: str | int) -> None:
        eng, sid = self._key(engine, step_id)
        key = (eng, sid)
        resources = self._steps.pop(key, None)
        if resources is None:
            return

        for endpoint in resources.endpoints.values():
            self._safe_unlink(endpoint.fifo_path)
            self.logger.info(
                "[FIFO] removed engine=%s step=%s basename=%s fifo=%s",
                resources.engine,
                resources.step_id,
                endpoint.basename,
                endpoint.fifo_path,
            )

        try:
            resources.step_dir.rmdir()
        except OSError:
            # Non-empty parent cleanup is intentionally conservative.
            pass

    def cleanup_all(self) -> None:
        for engine, step_id in list(self._steps.keys()):
            self.cleanup_step(engine, step_id)
        self.logger.info("[FIFO] cleanup_all completed")