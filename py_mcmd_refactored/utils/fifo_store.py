# from __future__ import annotations

# import logging
# import os
# import stat
# from dataclasses import dataclass, field
# from pathlib import Path
# from typing import Callable, Mapping, Optional


# DualWritePathFactory = Callable[[str, str, str], Optional[Path]]


# @dataclass(frozen=True)
# class FifoEndpoint:
#     engine: str
#     step_id: str
#     basename: str
#     fifo_path: Path
#     dual_write_path: Optional[Path] = None


# @dataclass
# class FifoStepResources:
#     engine: str
#     step_id: str
#     step_dir: Path
#     endpoints: dict[str, FifoEndpoint] = field(default_factory=dict)
#     status: str = "prepared"  # prepared | success | failed


# class FifoStore:
#     """Owns FIFO resources for engine step outputs.

#     Notes
#     -----
#     - This class does not know anything about orchestrator sequencing.
#     - Callers decide which prior step to read from.
#     - `finalize_step_success()` marks a step successful but intentionally does
#       not delete it, because a following step may still need to consume it.
#     - `finalize_step_failure()` cleans up the failed step immediately.
#     - `cleanup_step()` and `cleanup_all()` provide deterministic teardown.
#     """

#     def __init__(
#         self,
#         *,
#         root_dir: str | Path,
#         output_basenames_by_engine: Mapping[str, list[str] | tuple[str, ...]],
#         developer_mode: bool = False,
#         dual_write_path_factory: Optional[DualWritePathFactory] = None,
#         logger: Optional[logging.Logger] = None,
#     ) -> None:
#         self.root_dir = Path(root_dir)
#         self.root_dir.mkdir(parents=True, exist_ok=True)

#         self.developer_mode = bool(developer_mode)
#         self.dual_write_path_factory = dual_write_path_factory
#         self.logger = logger or logging.getLogger(__name__)

#         self._output_basenames_by_engine = self._normalize_output_map(
#             output_basenames_by_engine
#         )
#         self._steps: dict[tuple[str, str], FifoStepResources] = {}

#     @staticmethod
#     def _normalize_engine(engine: str) -> str:
#         normalized = str(engine).strip().upper()
#         if not normalized:
#             raise ValueError("engine must be a non-empty string")
#         return normalized

#     @staticmethod
#     def _normalize_step_id(step_id: str | int) -> str:
#         value = str(step_id).strip()
#         if not value:
#             raise ValueError("step_id must be a non-empty string/int")
#         return value

#     @classmethod
#     def _normalize_output_map(
#         cls,
#         output_basenames_by_engine: Mapping[str, list[str] | tuple[str, ...]],
#     ) -> dict[str, tuple[str, ...]]:
#         normalized: dict[str, tuple[str, ...]] = {}
#         for engine, basenames in output_basenames_by_engine.items():
#             eng = cls._normalize_engine(engine)
#             cleaned: list[str] = []
#             for raw_name in basenames:
#                 name = Path(raw_name).name
#                 if name != raw_name:
#                     raise ValueError(
#                         f"FIFO output names must be basenames only; got '{raw_name}'"
#                     )
#                 if not name:
#                     raise ValueError("FIFO output basename cannot be empty")
#                 cleaned.append(name)
#             normalized[eng] = tuple(cleaned)
#         return normalized

#     def _key(self, engine: str, step_id: str | int) -> tuple[str, str]:
#         eng = self._normalize_engine(engine)
#         sid = self._normalize_step_id(step_id)
#         if eng not in self._output_basenames_by_engine:
#             raise ValueError(
#                 f"Unsupported engine '{engine}'. Expected one of: "
#                 f"{sorted(self._output_basenames_by_engine)}"
#             )
#         return eng, sid

#     @staticmethod
#     def _safe_unlink(path: Path) -> None:
#         try:
#             if path.exists() or path.is_symlink():
#                 path.unlink()
#         except FileNotFoundError:
#             pass

#     @staticmethod
#     def _is_fifo(path: Path) -> bool:
#         try:
#             return stat.S_ISFIFO(path.stat().st_mode)
#         except FileNotFoundError:
#             return False

#     def prepare_step(self, engine: str, step_id: str | int) -> FifoStepResources:
#         eng, sid = self._key(engine, step_id)
#         key = (eng, sid)
#         if key in self._steps:
#             raise ValueError(f"FIFO resources already prepared for {eng} step {sid}")

#         step_dir = self.root_dir / eng / sid
#         step_dir.mkdir(parents=True, exist_ok=True)

#         endpoints: dict[str, FifoEndpoint] = {}
#         for basename in self._output_basenames_by_engine[eng]:
#             fifo_path = step_dir / basename
#             self._safe_unlink(fifo_path)
#             os.mkfifo(fifo_path)

#             dual_write_path: Optional[Path] = None
#             if self.developer_mode and self.dual_write_path_factory is not None:
#                 dual_write_path = self.dual_write_path_factory(eng, sid, basename)
#                 if dual_write_path is not None:
#                     dual_write_path.parent.mkdir(parents=True, exist_ok=True)

#             endpoint = FifoEndpoint(
#                 engine=eng,
#                 step_id=sid,
#                 basename=basename,
#                 fifo_path=fifo_path,
#                 dual_write_path=dual_write_path,
#             )
#             endpoints[basename] = endpoint

#             self.logger.info(
#                 "[FIFO] created engine=%s step=%s basename=%s fifo=%s%s",
#                 eng,
#                 sid,
#                 basename,
#                 fifo_path,
#                 f" dual_write={dual_write_path}" if dual_write_path else "",
#             )

#         resources = FifoStepResources(
#             engine=eng,
#             step_id=sid,
#             step_dir=step_dir,
#             endpoints=endpoints,
#             status="prepared",
#         )
#         self._steps[key] = resources
#         return resources

#     def get_step(self, engine: str, step_id: str | int) -> FifoStepResources:
#         eng, sid = self._key(engine, step_id)
#         key = (eng, sid)
#         if key not in self._steps:
#             raise KeyError(f"No FIFO resources registered for {eng} step {sid}")
#         return self._steps[key]

#     def get_fifo_path(self, engine: str, step_id: str | int, basename: str) -> Path:
#         resources = self.get_step(engine, step_id)
#         normalized = Path(basename).name
#         if normalized not in resources.endpoints:
#             raise KeyError(
#                 f"FIFO basename '{basename}' is not registered for "
#                 f"{resources.engine} step {resources.step_id}"
#             )
#         return resources.endpoints[normalized].fifo_path

#     def finalize_step_success(self, engine: str, step_id: str | int) -> None:
#         resources = self.get_step(engine, step_id)
#         resources.status = "success"
#         self.logger.info(
#             "[FIFO] finalized success engine=%s step=%s endpoints=%s",
#             resources.engine,
#             resources.step_id,
#             sorted(resources.endpoints),
#         )

#     def finalize_step_failure(self, engine: str, step_id: str | int) -> None:
#         resources = self.get_step(engine, step_id)
#         resources.status = "failed"
#         self.logger.info(
#             "[FIFO] finalized failure engine=%s step=%s; cleaning up",
#             resources.engine,
#             resources.step_id,
#         )
#         self.cleanup_step(engine, step_id)

#     def cleanup_step(self, engine: str, step_id: str | int) -> None:
#         eng, sid = self._key(engine, step_id)
#         key = (eng, sid)
#         resources = self._steps.pop(key, None)
#         if resources is None:
#             return

#         for endpoint in resources.endpoints.values():
#             self._safe_unlink(endpoint.fifo_path)
#             self.logger.info(
#                 "[FIFO] removed engine=%s step=%s basename=%s fifo=%s",
#                 resources.engine,
#                 resources.step_id,
#                 endpoint.basename,
#                 endpoint.fifo_path,
#             )

#         try:
#             resources.step_dir.rmdir()
#         except OSError:
#             # Non-empty parent cleanup is intentionally conservative.
#             pass

#     def cleanup_all(self) -> None:
#         for engine, step_id in list(self._steps.keys()):
#             self.cleanup_step(engine, step_id)
#         self.logger.info("[FIFO] cleanup_all completed")

from __future__ import annotations

import logging
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


def _discover_managed_root(explicit_root: Optional[str | Path] = None) -> Path:
    if explicit_root is not None:
        return Path(explicit_root)

    env_root = os.getenv("PY_MCMD_MANAGED_OUTPUT_ROOT")
    if env_root:
        return Path(env_root)

    shm_root = Path("/dev/shm")
    if shm_root.exists() and os.access(shm_root, os.W_OK):
        return shm_root / "py_mcmd_refactored"

    return Path.cwd() / ".managed_outputs"


@dataclass
class StepResources:
    engine: str
    step_id: str
    managed_root: Path
    disk_root: Path
    developer_mode: bool = False
    status: str = "prepared"

    def runtime_dir(self, box_number: Optional[int] = None) -> Path:
        if self.engine == "NAMD":
            if box_number not in (0, 1):
                raise ValueError("NAMD runtime_dir requires box_number 0 or 1")
            suffix = "a" if int(box_number) == 0 else "b"
            return self.managed_root / self.engine / f"{self.step_id}_{suffix}"
        elif self.engine == "GOMC":
            return self.managed_root / self.engine / self.step_id
        raise ValueError(f"Unsupported engine: {self.engine}")

    def disk_dir(self, box_number: Optional[int] = None) -> Path:
        if self.engine == "NAMD":
            if box_number not in (0, 1):
                raise ValueError("NAMD disk_dir requires box_number 0 or 1")
            suffix = "a" if int(box_number) == 0 else "b"
            return self.disk_root / f"{self.step_id}_{suffix}"
        elif self.engine == "GOMC":
            return self.disk_root / self.step_id
        raise ValueError(f"Unsupported engine: {self.engine}")

    def create_runtime_dirs(self) -> None:
        if self.engine == "NAMD":
            self.runtime_dir(0).mkdir(parents=True, exist_ok=True)
            self.runtime_dir(1).mkdir(parents=True, exist_ok=True)
        else:
            self.runtime_dir().mkdir(parents=True, exist_ok=True)

    def mirror_to_disk(self) -> None:
        if self.engine == "NAMD":
            self._mirror_tree(self.runtime_dir(0), self.disk_dir(0))
            self._mirror_tree(self.runtime_dir(1), self.disk_dir(1))
        else:
            self._mirror_tree(self.runtime_dir(), self.disk_dir())

    @staticmethod
    def _mirror_tree(src: Path, dst: Path) -> None:
        if not src.exists():
            return
        dst.mkdir(parents=True, exist_ok=True)
        for item in src.rglob("*"):
            rel = item.relative_to(src)
            target = dst / rel
            if item.is_dir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(item, target)


class ManagedArtifactStore:
    """
    Tmpfs-backed managed output store.

    Public API intentionally stays close to the previous FIFO abstraction:
      - prepare_step(engine, step_id)
      - finalize_step_success(engine, step_id)
      - finalize_step_failure(engine, step_id)
      - cleanup_step(engine, step_id)
      - cleanup_all()
    """

    def __init__(
        self,
        *,
        disk_roots: dict[str, str | Path],
        developer_mode: bool = False,
        managed_root: Optional[str | Path] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.logger = logger or logging.getLogger(__name__)
        self.managed_root = _discover_managed_root(managed_root)
        self.managed_root.mkdir(parents=True, exist_ok=True)

        self.disk_roots = {
            str(engine).upper(): Path(path)
            for engine, path in disk_roots.items()
        }
        self.developer_mode = bool(developer_mode)
        self._steps: dict[tuple[str, str], StepResources] = {}

    def _key(self, engine: str, step_id: str | int) -> tuple[str, str]:
        eng = str(engine).strip().upper()
        sid = str(step_id).strip()
        if eng not in self.disk_roots:
            raise ValueError(f"Unsupported engine '{engine}'")
        if not sid:
            raise ValueError("step_id must be non-empty")
        return eng, sid

    def prepare_step(self, engine: str, step_id: str | int) -> StepResources:
        eng, sid = self._key(engine, step_id)
        key = (eng, sid)
        if key in self._steps:
            raise ValueError(f"Managed step already prepared for {eng} step {sid}")

        resources = StepResources(
            engine=eng,
            step_id=sid,
            managed_root=self.managed_root,
            disk_root=self.disk_roots[eng],
            developer_mode=self.developer_mode,
            status="prepared",
        )
        resources.create_runtime_dirs()
        self._steps[key] = resources

        self.logger.info(
            "[ARTIFACT_STORE] prepared engine=%s step=%s managed_root=%s",
            eng,
            sid,
            self.managed_root,
        )
        return resources

    def get_step(self, engine: str, step_id: str | int) -> StepResources:
        key = self._key(engine, step_id)
        if key not in self._steps:
            raise KeyError(f"No managed resources registered for {key[0]} step {key[1]}")
        return self._steps[key]

    def finalize_step_success(self, engine: str, step_id: str | int) -> None:
        resources = self.get_step(engine, step_id)
        resources.status = "success"

        if self.developer_mode:
            resources.mirror_to_disk()
            self.logger.info(
                "[ARTIFACT_STORE] mirrored step to disk engine=%s step=%s",
                resources.engine,
                resources.step_id,
            )

        self.logger.info(
            "[ARTIFACT_STORE] finalized success engine=%s step=%s",
            resources.engine,
            resources.step_id,
        )

    def finalize_step_failure(self, engine: str, step_id: str | int) -> None:
        resources = self.get_step(engine, step_id)
        resources.status = "failed"
        self.logger.info(
            "[ARTIFACT_STORE] finalized failure engine=%s step=%s; cleaning runtime dirs",
            resources.engine,
            resources.step_id,
        )
        self.cleanup_step(engine, step_id)

    def cleanup_step(self, engine: str, step_id: str | int) -> None:
        key = self._key(engine, step_id)
        resources = self._steps.pop(key, None)
        if resources is None:
            return

        if resources.engine == "NAMD":
            shutil.rmtree(resources.runtime_dir(0), ignore_errors=True)
            shutil.rmtree(resources.runtime_dir(1), ignore_errors=True)
        else:
            shutil.rmtree(resources.runtime_dir(), ignore_errors=True)

        self.logger.info(
            "[ARTIFACT_STORE] cleaned runtime dirs engine=%s step=%s",
            resources.engine,
            resources.step_id,
        )

    # def cleanup_all(self) -> None:
    #     for engine, step_id in list(self._steps.keys()):
    #         self.cleanup_step(engine, step_id)

    #     for engine in self.disk_roots:
    #         try:
    #             (self.managed_root / engine).rmdir()
    #         except OSError:
    #             pass

    #     self.logger.info("[ARTIFACT_STORE] cleanup_all completed")
    def cache_dir(self, engine: str) -> Path:
        eng, _ = self._key(engine, "cache")
        path = self.managed_root / "_engine_cache" / eng
        path.mkdir(parents=True, exist_ok=True)
        return path

    def cleanup_cache_dir(self, engine: str) -> None:
        shutil.rmtree(
            self.managed_root / "_engine_cache" / str(engine).strip().upper(),
            ignore_errors=True,
        )

    def cleanup_all(self) -> None:
        for engine, step_id in list(self._steps.keys()):
            self.cleanup_step(engine, step_id)

        shutil.rmtree(self.managed_root / "_engine_cache", ignore_errors=True)

        for engine in self.disk_roots:
            try:
                (self.managed_root / engine).rmdir()
            except OSError:
                pass

        self.logger.info("[ARTIFACT_STORE] cleanup_all completed")


# backward-compatible alias
# FifoStore = ManagedArtifactStore
class FifoStore:
    def __init__(self, **kwargs):
        if "disk_roots" in kwargs or "managed_root" in kwargs:
            self._impl = ManagedArtifactStore(**kwargs)
        else:
            self._impl = LegacyFifoStore(**kwargs)

    def __getattr__(self, name):
        return getattr(self._impl, name)
# FifoStepResources = StepResources
# keep the legacy FIFO test symbol available
# the orchestrator only uses it as an import/type alias


from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Mapping, Optional
import logging
import os
import shutil
import stat

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
    status: str = "prepared"


class LegacyFifoStore:
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
        self._output_basenames_by_engine = {
            str(engine).strip().upper(): tuple(Path(x).name for x in basenames)
            for engine, basenames in output_basenames_by_engine.items()
        }
        self._steps: dict[tuple[str, str], FifoStepResources] = {}

    def _key(self, engine: str, step_id: str | int) -> tuple[str, str]:
        eng = str(engine).strip().upper()
        sid = str(step_id).strip()
        if eng not in self._output_basenames_by_engine:
            raise ValueError(f"Unsupported engine '{engine}'")
        return eng, sid

    @staticmethod
    def _safe_unlink(path: Path) -> None:
        try:
            if path.exists() or path.is_symlink():
                path.unlink()
        except FileNotFoundError:
            pass

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

            dual_write_path = None
            if self.developer_mode and self.dual_write_path_factory is not None:
                dual_write_path = self.dual_write_path_factory(eng, sid, basename)
                if dual_write_path is not None:
                    dual_write_path.parent.mkdir(parents=True, exist_ok=True)

            endpoints[basename] = FifoEndpoint(
                engine=eng,
                step_id=sid,
                basename=basename,
                fifo_path=fifo_path,
                dual_write_path=dual_write_path,
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
        key = self._key(engine, step_id)
        if key not in self._steps:
            raise KeyError(f"No FIFO resources registered for {key[0]} step {key[1]}")
        return self._steps[key]

    def get_fifo_path(self, engine: str, step_id: str | int, basename: str) -> Path:
        resources = self.get_step(engine, step_id)
        return resources.endpoints[Path(basename).name].fifo_path

    def finalize_step_success(self, engine: str, step_id: str | int) -> None:
        self.get_step(engine, step_id).status = "success"

    def finalize_step_failure(self, engine: str, step_id: str | int) -> None:
        self.get_step(engine, step_id).status = "failed"
        self.cleanup_step(engine, step_id)

    def cleanup_step(self, engine: str, step_id: str | int) -> None:
        key = self._key(engine, step_id)
        resources = self._steps.pop(key, None)
        if resources is None:
            return
        for endpoint in resources.endpoints.values():
            self._safe_unlink(endpoint.fifo_path)
        try:
            resources.step_dir.rmdir()
        except OSError:
            pass

    def cleanup_all(self) -> None:
        for engine, step_id in list(self._steps.keys()):
            self.cleanup_step(engine, step_id)

