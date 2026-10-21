import importlib.util
from pathlib import Path
from types import SimpleNamespace as NS

import pytest

ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location("pair_recon_setup_install", ROOT / "setup/install.py")
assert SPEC and SPEC.loader
install = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(install)


def item(path, kind):
    return NS(path=path, object_type=NS(value=kind))


class WorkspaceFiles:
    def __init__(self, objects):
        self.objects = {value.path: value for value in objects}
        self.deleted = []

    def mkdirs(self, _path):
        return None

    def upload(self, path, _body, **options):
        kind = "NOTEBOOK" if options["format"].value == "SOURCE" else "FILE"
        self.objects[path] = item(path, kind)

    def list(self, path):
        prefix = path.rstrip("/") + "/"
        return iter(
            value
            for name, value in sorted(self.objects.items())
            if name.startswith(prefix) and "/" not in name[len(prefix) :]
        )

    def delete(self, path, *, recursive=None):
        self.deleted.append((path, recursive))
        prefix = path.rstrip("/") + "/"
        for name in list(self.objects):
            if name == path or (recursive and name.startswith(prefix)):
                del self.objects[name]


def test_upload_removes_remote_files_missing_locally(tmp_path):
    source = tmp_path / "pair_recon_right"
    source.mkdir()
    (source / "notebook.py").write_text("print('notebook')\n", encoding="utf-8")
    (source / "keep.py").write_text("keep = True\n", encoding="utf-8")
    (source / "new.py").write_text("new = True\n", encoding="utf-8")
    root = "/Users/me@example.com/app/pair_recon_right"
    files = WorkspaceFiles(
        [
            item(f"{root}/notebook", "NOTEBOOK"),
            item(f"{root}/keep.py", "FILE"),
            item(f"{root}/old.py", "FILE"),
            item(f"{root}/old", "DIRECTORY"),
            item(f"{root}/old/child.py", "FILE"),
        ]
    )

    uploaded, deleted = install.upload(NS(workspace=files), source, root)

    assert uploaded == [f"{root}/keep.py", f"{root}/new.py", f"{root}/notebook"]
    assert set(deleted) == {f"{root}/old.py", f"{root}/old", f"{root}/old/child.py"}
    assert set(files.objects) == {
        f"{root}/keep.py",
        f"{root}/new.py",
        f"{root}/notebook",
    }


def test_upload_failure_does_not_delete_remote_files(tmp_path):
    source = tmp_path / "pair_recon_right"
    source.mkdir()
    (source / "new.py").write_text("new = True\n", encoding="utf-8")
    root = "/Users/me@example.com/app/pair_recon_right"
    files = WorkspaceFiles([item(f"{root}/old.py", "FILE")])

    def fail_upload(*_args, **_kwargs):
        raise OSError("upload failed")

    files.upload = fail_upload
    with pytest.raises(OSError, match="upload failed"):
        install.upload(NS(workspace=files), source, root)

    assert files.deleted == []
    assert f"{root}/old.py" in files.objects
