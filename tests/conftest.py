from __future__ import annotations

import json


class MemoryStore:
    root_uri = "s3://test/run"

    def __init__(self):
        self.objects = {}
        self.events = []

    def put_json(self, key, value):
        self.events.append(("put", key))
        self.objects[key] = json.loads(json.dumps(value))
        return key

    def put_bytes(self, key, body, *, content_type):
        self.events.append(("put", key))
        self.objects[key] = bytes(body)
        return key

    def get_json(self, key):
        return json.loads(json.dumps(self.objects[key]))

    def get_bytes(self, key):
        value = self.objects[key]
        return value if isinstance(value, bytes) else json.dumps(value).encode()

    def exists(self, key):
        return key in self.objects

    def list_keys(self, prefix, *, start_after=None):
        rows = sorted(key for key in self.objects if key.startswith(prefix))
        return [key for key in rows if start_after is None or key > start_after]

    def list_json(self, prefix, *, start_after=None):
        return [
            (key, self.get_json(key))
            for key in self.list_keys(prefix, start_after=start_after)
            if isinstance(self.objects[key], dict)
        ]

    def heartbeat(self, now=None):
        return self.put_json("_HEARTBEAT", {"epoch_s": 0 if now is None else now})

    def heartbeat_age(self, now=None):
        return 0

    def commit_unit(self, sequence, unit_id, payload, pointer):
        payload_key = f"units/{unit_id}.json"
        self.put_json(payload_key, payload)
        row = {**pointer, "unit_id": unit_id, "payload_key": payload_key, "sequence": sequence}
        self.put_json(f"_INDEX/{sequence:08d}-{unit_id}.json", row)
        return row

    def ready_unclaimed(self):
        return max(0, len(self.list_keys("_INDEX/")) - len(self.list_keys("results/")))

    def close_stage1(self, pairs):
        return self.put_json("stage1/_CLOSED", {"pairs": sorted(pairs)})

    def close_manifests(self, pairs, expected_units):
        return self.put_json(
            "manifest/_CLOSED", {"pairs": sorted(pairs), "expected_units": expected_units}
        )

    def put_result(self, unit_id, result):
        return self.put_json(f"results/{unit_id}.json", result)

    def put_dead(self, unit_id, result):
        return self.put_json(f"dead/{unit_id}.json", result)
