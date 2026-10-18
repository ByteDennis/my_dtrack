import io

from pair_recon.core.backends.store import ApiVolumeStore


class Files:
    def __init__(self):
        self.values = {}

    def create_directory(self, path):
        return path

    def upload(self, path, contents, *, overwrite=None):
        self.values[path] = contents.read()

    def download(self, path):
        return type("Download", (), {"contents": io.BytesIO(self.values[path])})()

    def get_metadata(self, path):
        if path not in self.values:
            raise FileNotFoundError(path)
        return object()

    def list_directory_contents(self, path):
        prefix = path.rstrip("/") + "/"
        return [
            type("Entry", (), {"path": key, "is_directory": False})()
            for key in sorted(self.values)
            if key.startswith(prefix)
        ]

    def delete(self, path):
        self.values.pop(path, None)


def test_store():
    files = Files()
    store = ApiVolumeStore("/Volumes/main/recon/artifacts/runs", files=files).child("run-1")
    store.put_json("pairs/订单/value.json", {"value": "✓"})
    assert store.exists("pairs/订单/value.json")
    assert store.get_json("pairs/订单/value.json") == {"value": "✓"}
    assert store.list_keys("pairs") == ["pairs/订单/value.json"]
    assert store.delete_keys(["pairs/订单/value.json"]) == 1
    assert not store.exists("pairs/订单/value.json")
