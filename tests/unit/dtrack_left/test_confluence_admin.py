import json
from types import SimpleNamespace

import pytest

from dtrack_left import confluence


# >>> in-memory Confluence DC: renumbering (7.x) or gapped (9.x) versions, routes missing under rest/api <<< #
class FakeConfluence:
    def __init__(self, *, renumber=False, experimental_only=("version",), cloud=False):
        self.renumber, self.experimental_only, self.cloud = renumber, experimental_only, cloud
        self.versions = {"1": [1, 2, 3, 4, 5, 6]}
        self.restrictions = {}
        self.attachments = {}
        self.properties = {}
        self.calls = []

    def _reply(self, status, body=None):
        content = json.dumps(body).encode() if body is not None else b""
        return SimpleNamespace(
            status_code=status,
            content=content,
            text=content.decode(),
            headers={"Content-Type": "application/json"},
            json=lambda: body,
        )

    def request(self, method, path, advanced_mode=True, **kwargs):
        self.calls.append((method, path, kwargs.get("params"), kwargs.get("json")))
        root, _, rest = path.partition("/content")
        parts = rest.strip("/").split("/")
        if root == "rest/api" and method == "GET" and parts[-1:] == list(self.experimental_only):
            return self._reply(404)
        if method == "GET" and parts[1:] == ["version"]:
            numbers = sorted(self.versions[parts[0]], reverse=True)
            start = kwargs["params"]["start"]
            return self._reply(
                200, {"results": [{"number": n} for n in numbers[start : start + 200]]}
            )
        if method == "GET" and len(parts) == 1 and parts[0] in self.versions:
            return self._reply(200, {"version": {"number": max(self.versions[parts[0]])}})
        if method == "PUT" and len(parts) == 1 and parts[0] in self.versions:
            self.versions[parts[0]].append(kwargs["json"]["version"]["number"])
            return self._reply(200, {"id": parts[0]})
        if method == "DELETE" and parts[1:2] == ["version"]:
            page, number = parts[0], int(parts[2])
            if number not in self.versions[page] or number == max(self.versions[page]):
                return self._reply(400, {"message": "bad version"})
            kept = [n for n in self.versions[page] if n != number]
            self.versions[page] = list(range(1, len(kept) + 1)) if self.renumber else kept
            return self._reply(204)
        if parts[1:2] == ["restriction"]:
            if method == "PUT":
                for op in kwargs["json"]:
                    self.restrictions[op["operation"]] = op["restrictions"]
                return self._reply(200, [])
            out = {
                op: {
                    "restrictions": {
                        "user": {"results": self.restrictions.get(op, {}).get("user", [])},
                        "group": {"results": self.restrictions.get(op, {}).get("group", [])},
                    }
                }
                for op in ("read", "update")
            }
            return self._reply(200, out)
        if parts[1:2] == ["property"]:
            page = parts[0]
            if method == "GET":
                return self._reply(200, {"results": list(self.properties.get(page, {}).values())})
            body = kwargs["json"]
            if method == "POST":
                row = {**body, "version": {"number": 1}}
                self.properties.setdefault(page, {})[body["key"]] = row
                return self._reply(200, row)
            if method == "PUT":
                self.properties.setdefault(page, {})[body["key"]] = body
                return self._reply(200, body)
        if parts[1:3] == ["child", "attachment"]:
            name = (kwargs.get("params") or {}).get("filename")
            if method == "GET":
                found = self.attachments.get(name)
                return self._reply(200, {"results": [found] if found else []})
            upload = kwargs["files"]["file"][0]
            assert kwargs["headers"]["X-Atlassian-Token"] == "no-check"
            existing = self.attachments.get(upload)
            number = existing["version"]["number"] + 1 if existing else 1
            row = {"id": f"att-{upload}", "title": upload, "version": {"number": number}}
            self.attachments[upload] = row
            return self._reply(200, row if existing else {"results": [row]})
        if method == "DELETE" and parts[0].startswith("att-"):
            self.attachments.pop(parts[0].removeprefix("att-"))
            return self._reply(204)
        if method == "DELETE":
            return self._reply(204)
        raise AssertionError(f"unexpected {method} {path}")

    def get_current_user(self):
        return {"accountId": "acc-me"} if self.cloud else {"username": "me"}

    def update_or_create(self, *args, **kwargs):
        self.versions["1"].append(max(self.versions["1"]) + 1)
        return {"id": "1"}


@pytest.mark.parametrize("renumber", [False, True], ids=["9.x-gaps", "7.x-renumber"])
def test_keep_versions_relists_after_each_delete_on_both_numbering_schemes(renumber):
    client = FakeConfluence(renumber=renumber)
    outcome = confluence.keep_versions(client, "1", 2)
    assert outcome["deleted"] == 4 and len(outcome["remaining"]) == 2
    assert client.versions["1"] == ([1, 2] if renumber else [5, 6])
    assert any(path.startswith("rest/experimental/") for _, path, *_ in client.calls)


@pytest.mark.parametrize("renumber", [False, True], ids=["9.x-gaps", "7.x-renumber"])
def test_delete_versions_goes_highest_first_so_renumbering_cannot_shift_targets(renumber):
    client = FakeConfluence(renumber=renumber)
    assert confluence.delete_versions(client, "1", [2, 4]) == [2, 4]
    deletes = [path for method, path, *_ in client.calls if method == "DELETE"]
    assert deletes == ["rest/api/content/1/version/4", "rest/api/content/1/version/2"]
    assert len(client.versions["1"]) == 4


def test_delete_versions_refuses_the_current_and_unknown_versions():
    client = FakeConfluence()
    with pytest.raises(ValueError, match="current version"):
        confluence.delete_versions(client, "1", [6])
    with pytest.raises(ValueError, match=r"no version\(s\) \[9\]"):
        confluence.delete_versions(client, "1", [9])


def test_keep_versions_stops_when_a_delete_has_no_effect():
    client = FakeConfluence()
    client.request = lambda method, path, **kw: (
        FakeConfluence._reply(client, 204)
        if method == "DELETE"
        else FakeConfluence.request(client, method, path, **kw)
    )
    with pytest.raises(confluence.ConfluenceError, match="had no effect"):
        confluence.keep_versions(client, "1", 2)


@pytest.mark.parametrize("cloud", [False, True])
def test_restrictions_always_keep_the_caller_and_accept_groups(cloud):
    client = FakeConfluence(cloud=cloud)
    me = "acc-me" if cloud else "me"
    key = "accountId" if cloud else "username"
    confluence.set_restrictions(client, "1", read=["group:readers", "alice"], edit=["bob"], me=me)
    put = next(body for method, _, _, body in client.calls if method == "PUT")
    read = next(op["restrictions"] for op in put if op["operation"] == "read")
    assert read["group"] == [{"type": "group", "name": "readers"}]
    assert read["user"] == [{"type": "known", key: "alice"}, {"type": "known", key: me}]
    confluence.set_restrictions(client, "1", read=[], edit=[], me=me)
    assert client.restrictions == {
        "read": {"user": [], "group": []},
        "update": {"user": [], "group": []},
    }
    with pytest.raises(ValueError, match="group:<name>"):
        confluence.set_restrictions(client, "1", read=["team:x"], me=me)


def test_upload_attachment_adds_a_new_version_for_an_existing_name(tmp_path):
    client = FakeConfluence()
    path = tmp_path / "report.xlsx"
    path.write_bytes(b"one")
    assert confluence.upload_attachment(client, "1", path)["version"] == 1
    path.write_bytes(b"two")
    assert confluence.upload_attachment(client, "1", path)["version"] == 2
    posts = [path for method, path, *_ in client.calls if method == "POST"]
    assert posts == [
        "rest/api/content/1/child/attachment",
        "rest/api/content/1/child/attachment/att-report.xlsx/data",
    ]
    confluence.delete_attachment(client, "1", "report.xlsx")
    with pytest.raises(ValueError, match="no attachment"):
        confluence.delete_attachment(client, "1", "report.xlsx")


def test_errors_carry_the_route_and_status():
    client = FakeConfluence()
    with pytest.raises(confluence.ConfluenceError, match="DELETE content/1/version/9: HTTP 400"):
        confluence.confluence_call(client, "DELETE", "content/1/version/9")


def test_publish_applies_attach_detach_versions_then_restrictions(tmp_path):
    client = FakeConfluence()
    attachment = tmp_path / "note.txt"
    attachment.write_text("x")
    page = {"title": "t", "body": "<p/>"}
    outcome = confluence.publish_tracking_page(
        client,
        page,
        space="DT",
        attachments=[attachment],
        keep=3,
        restrict_read=["group:readers"],
    )
    assert outcome["attachments"][0]["name"] == "note.txt" and outcome["detached"] == []
    assert outcome["versions"]["remaining"] == [5, 6, 7]
    assert outcome["restrictions"]["read"] == {"users": ["me"], "groups": ["readers"]}
    assert outcome["restrictions"]["update"] == {"users": ["me"], "groups": []}
    methods = [method for method, *_ in client.calls]
    assert methods.index("POST") < methods.index("DELETE") < methods.index("PUT")


def test_template_state_is_versioned_and_exact_page_update_preserves_identity():
    client = FakeConfluence()
    state = {
        "version": 1,
        "template_name": "page.xhtml",
        "template_xhtml": "<h1>{{title}}</h1>",
        "data": {"title": "Validation", "tables": []},
    }
    page = {"title": "Validation", "body": "<h1>Validation</h1>", "template_state": state}
    assert confluence.set_template_state(client, "1", state) == 1
    assert confluence.get_template_state(client, "1") == {"value": state, "version": 1}
    outcome = confluence.update_template_page(client, "1", page)
    assert outcome == {
        "page_id": "1",
        "version": 7,
        "template_state_version": 2,
        "attachments": [],
    }
    assert client.properties["1"][confluence.TEMPLATE_STATE_PROPERTY]["version"] == {
        "number": 2
    }


def test_delete_page_trashes_and_optionally_purges():
    client = FakeConfluence()
    assert confluence.delete_page(client, "1") == {"page_id": "1", "state": "trashed"}
    assert confluence.delete_page(client, "1", purge=True)["state"] == "purged"
    assert client.calls[-1][2] == {"status": "trashed"}


def test_inspect_page_combines_versions_restrictions_and_attachments():
    client = FakeConfluence()
    client.restrictions["read"] = {
        "user": [{"type": "known", "username": "me"}],
        "group": [],
    }
    outcome = confluence.inspect_page(client, "1")
    assert outcome["page_id"] == "1" and outcome["version"] == 6
    assert outcome["versions"] == [1, 2, 3, 4, 5, 6]
    assert outcome["restrictions"]["read"]["users"] == ["me"]
    assert outcome["attachments"] == []
