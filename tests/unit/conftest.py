import importlib
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

COMMON = {
    "dates": "sql.dates",
    "hashing": "model.hashing",
    "manifest": "model.manifest",
}
ROOTS = {
    "pair_recon_left": "pair_recon_left.common",
    "pair_recon_right": "pair_recon_right.engine.common",
}
REPO = Path(__file__).resolve().parents[2]


@pytest.fixture(autouse=True)
def isolated_environment(monkeypatch):
    saved = dict(os.environ)
    for module_name in ("pair_recon.app.cli", "pair_recon_left.cli", "pair_recon_left.cli.config"):
        module = importlib.import_module(module_name)
        if not hasattr(module, "find_dotenv"):
            continue
        real = module.find_dotenv

        def guarded(*args, _real=real, **kwargs):
            found = _real(*args, **kwargs)
            return "" if found and Path(found).resolve().is_relative_to(REPO.parent) else found

        monkeypatch.setattr(module, "find_dotenv", guarded)
    yield
    os.environ.clear()
    os.environ.update(saved)


@pytest.fixture(params=["pair_recon_left", "pair_recon_right"])
def common(request):
    root = ROOTS[request.param]
    modules = {name: importlib.import_module(f"{root}.{path}") for name, path in COMMON.items()}
    return SimpleNamespace(side=request.param, **modules)
