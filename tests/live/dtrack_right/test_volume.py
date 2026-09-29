import io
import json

import pytest

from dtrack_right.backends.store import S3Store

pytestmark = pytest.mark.live


# >>> step 6: the Volume and the S3 prefix are the same folder, in both directions <<< #
def test_work_volume_and_work_prefix_are_the_same_place(need, workspace, tag, keep):
    s3_work, volume_work = need("DTRACK_LIVE_S3_WORK", "DTRACK_LIVE_VOLUME_WORK")
    s3 = S3Store(f"{s3_work}/_live/{tag}")
    s3.put_json("from_s3.json", {"writer": "boto3"})
    volume = f"{volume_work}/_live/{tag}"
    downloaded = workspace.files.download(f"{volume}/from_s3.json").contents.read()
    assert json.loads(downloaded) == {"writer": "boto3"}
    workspace.files.upload(
        f"{volume}/from_volume.json", io.BytesIO(b'{"writer": "files-api"}'), overwrite=True
    )
    assert s3.get_json("from_volume.json") == {"writer": "files-api"}
    listed = {item.name for item in workspace.files.list_directory_contents(volume)}
    assert listed >= {"from_s3.json", "from_volume.json"}
    if not keep:
        for name in ("from_s3.json", "from_volume.json"):
            workspace.files.delete(f"{volume}/{name}")


# >>> step 7: the report Volume is writable by Databricks and readable from here as S3 <<< #
def test_report_volume_maps_to_the_report_prefix(need, workspace, tag, keep):
    s3_report, volume_report = need("DTRACK_LIVE_S3_REPORT", "DTRACK_LIVE_VOLUME_REPORT")
    path = f"{volume_report}/_live/{tag}/probe.txt"
    workspace.files.upload(path, io.BytesIO(b"probe"), overwrite=True)
    assert S3Store(f"{s3_report}/_live/{tag}").get_bytes("probe.txt") == b"probe"
    if not keep:
        workspace.files.delete(path)
