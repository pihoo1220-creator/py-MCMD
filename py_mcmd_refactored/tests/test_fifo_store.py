import os
import stat
import sys
from pathlib import Path

import pytest

sys.path.insert(0, "/home/arsalan/wsu-gomc/py-MCMD-hm/py_mcmd_refactored")

from utils.fifo_store import FifoStore


pytestmark = pytest.mark.skipif(
    not hasattr(os, "mkfifo"),
    reason="FIFO tests require os.mkfifo support",
)


def make_store(tmp_path: Path, **kwargs) -> FifoStore:
    base = dict(
        root_dir=tmp_path / "fifo_store",
        output_basenames_by_engine={
            "NAMD": ["namdOut.restart.coor", "out.dat"],
            "GOMC": ["Output_data_BOX_0_restart.coor", "out.dat"],
        },
    )
    base.update(kwargs)
    return FifoStore(**base)


def test_prepare_step_creates_fifo_endpoints(tmp_path: Path):
    store = make_store(tmp_path)

    resources = store.prepare_step("NAMD", 0)

    assert resources.engine == "NAMD"
    assert resources.step_id == "0"
    assert resources.status == "prepared"
    assert sorted(resources.endpoints) == ["namdOut.restart.coor", "out.dat"]

    coor_path = resources.endpoints["namdOut.restart.coor"].fifo_path
    out_path = resources.endpoints["out.dat"].fifo_path

    assert coor_path.exists()
    assert out_path.exists()
    assert stat.S_ISFIFO(coor_path.stat().st_mode)
    assert stat.S_ISFIFO(out_path.stat().st_mode)


def test_get_fifo_path_exposes_prior_step_resource_for_consumer(tmp_path: Path):
    store = make_store(tmp_path)
    store.prepare_step("GOMC", 1)

    fifo_path = store.get_fifo_path("GOMC", 1, "Output_data_BOX_0_restart.coor")

    assert fifo_path.name == "Output_data_BOX_0_restart.coor"
    assert fifo_path.exists()
    assert stat.S_ISFIFO(fifo_path.stat().st_mode)


def test_finalize_step_success_marks_step_without_cleaning_it(tmp_path: Path):
    store = make_store(tmp_path)
    resources = store.prepare_step("NAMD", 2)

    fifo_path = resources.endpoints["out.dat"].fifo_path
    assert fifo_path.exists()

    store.finalize_step_success("NAMD", 2)

    saved = store.get_step("NAMD", 2)
    assert saved.status == "success"
    assert fifo_path.exists()
    assert stat.S_ISFIFO(fifo_path.stat().st_mode)


def test_finalize_step_failure_cleans_up_step_resources(tmp_path: Path):
    store = make_store(tmp_path)
    resources = store.prepare_step("GOMC", 3)

    fifo_path = resources.endpoints["out.dat"].fifo_path
    assert fifo_path.exists()

    store.finalize_step_failure("GOMC", 3)

    assert not fifo_path.exists()
    with pytest.raises(KeyError):
        store.get_step("GOMC", 3)


def test_cleanup_step_removes_fifo_resources_after_success(tmp_path: Path):
    store = make_store(tmp_path)
    resources = store.prepare_step("NAMD", 4)
    store.finalize_step_success("NAMD", 4)

    fifo_path = resources.endpoints["namdOut.restart.coor"].fifo_path
    assert fifo_path.exists()

    store.cleanup_step("NAMD", 4)

    assert not fifo_path.exists()
    with pytest.raises(KeyError):
        store.get_step("NAMD", 4)


def test_cleanup_all_removes_every_registered_step(tmp_path: Path):
    store = make_store(tmp_path)
    s0 = store.prepare_step("NAMD", 0)
    s1 = store.prepare_step("GOMC", 1)

    assert s0.endpoints["out.dat"].fifo_path.exists()
    assert s1.endpoints["out.dat"].fifo_path.exists()

    store.cleanup_all()

    assert not s0.endpoints["out.dat"].fifo_path.exists()
    assert not s1.endpoints["out.dat"].fifo_path.exists()

    with pytest.raises(KeyError):
        store.get_step("NAMD", 0)
    with pytest.raises(KeyError):
        store.get_step("GOMC", 1)


def test_prepare_step_rejects_duplicate_engine_step_registration(tmp_path: Path):
    store = make_store(tmp_path)
    store.prepare_step("NAMD", 5)

    with pytest.raises(ValueError, match="already prepared"):
        store.prepare_step("NAMD", 5)


def test_unknown_engine_is_rejected(tmp_path: Path):
    store = make_store(tmp_path)

    with pytest.raises(ValueError, match="Unsupported engine"):
        store.prepare_step("LAMMPS", 0)


def test_developer_mode_records_dual_write_paths_via_hook(tmp_path: Path):
    def dual_write_factory(engine: str, step_id: str, basename: str) -> Path:
        return tmp_path / "mirror" / engine / step_id / basename

    store = make_store(
        tmp_path,
        developer_mode=True,
        dual_write_path_factory=dual_write_factory,
    )

    resources = store.prepare_step("GOMC", 7)
    endpoint = resources.endpoints["out.dat"]

    assert endpoint.dual_write_path == tmp_path / "mirror" / "GOMC" / "7" / "out.dat"
    assert endpoint.dual_write_path.parent.exists()