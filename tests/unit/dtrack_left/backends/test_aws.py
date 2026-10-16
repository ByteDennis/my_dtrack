from types import SimpleNamespace as NS

import boto3
import pytest

from dtrack_left.backends import aws


def test_refresh_copies(aws_home):
    first = aws.refresh_profile("corp", "corp-winscp")
    assert first["target"] == "corp-winscp" and first["access_key"] == "ASIA...0001"
    assert 1700 < first["expires_in"] <= 1800 and first["file"] == str(aws_home / "credentials")
    static = boto3.Session(profile_name="corp-winscp").get_credentials()
    assert (static.method, static.token) == ("shared-credentials-file", "token1")
    source = boto3.Session(profile_name="corp").get_credentials()
    assert source.method == "custom-process"


def test_second_refresh(aws_home):
    path = aws_home / "credentials"
    path.write_bytes(
        b"[other]\r\naws_access_key_id = A\r\n# keep me\r\n\r\n"
        b"[corp-winscp]\r\naws_access_key_id = OLD\r\n\r\n[last]\r\nx = 1\r\n\r\n"
    )
    aws.refresh_profile("corp", "corp-winscp")
    aws.refresh_profile("corp", "corp-winscp")
    text = path.read_text()
    assert text.count("[corp-winscp]") == 1 and "OLD" not in text and "token2" in text
    assert text.startswith("[other]\naws_access_key_id = A\n# keep me\n\n[last]\nx = 1\n\n")
    assert not (aws_home / "credentials.tmp").exists() and path.stat().st_mode & 0o777 == 0o600


def test_refresh_refuses(aws_home):
    with pytest.raises(ValueError, match="shadow"):
        aws.refresh_profile("corp", "corp")
    assert not (aws_home / "credentials").exists()


def test_static_keys(aws_home):
    (aws_home / "credentials").write_text(
        "[plain]\naws_access_key_id = A\naws_secret_access_key = B\n"
    )
    outcome = aws.refresh_profile("plain", "plain-copy")
    assert outcome["expires"] is None and outcome["expires_in"] is None
    assert "aws_session_token" not in (aws_home / "credentials").read_text()
    with (aws_home / "config").open("a") as config:
        config.write("[profile empty]\nregion = us-east-1\n")
    with pytest.raises(RuntimeError, match="no AWS credentials for profile 'empty'"):
        aws.refresh_profile("empty", "empty-copy")


@pytest.mark.parametrize(
    ("expires_in", "delay"), [(None, 1200), (1800, 1200), (900, 600), (200, 60), (-5, 60)]
)
def test_next_delay(expires_in, delay):
    assert aws.next_delay(expires_in, 1200) == delay


def test_watch_refreshes():
    ticks = iter(range(40))
    outcomes = iter([OSError("proxy"), {"expires_in": 1800}, {"expires_in": None}])

    def refresh():
        outcome = next(outcomes)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    count = aws.watch(
        refresh, lambda: next(ticks) < 30, every=100, first=60, tick=10, sleep=lambda _: None
    )
    assert count == 2


def test_winscp_path(tmp_path, monkeypatch):
    exe = tmp_path / "WinSCP.exe"
    exe.write_text("")
    monkeypatch.setenv("DTRACK_WINSCP", str(exe))
    assert aws.winscp_path() == str(exe)
    monkeypatch.delenv("DTRACK_WINSCP")
    monkeypatch.setattr(aws, "WINSCP_PATHS", ())
    monkeypatch.setattr(aws.shutil, "which", lambda name: None)
    with pytest.raises(FileNotFoundError, match="DTRACK_WINSCP"):
        aws.winscp_path()
    launched = aws.launch_winscp("corp-s3", exe="W.exe", popen=lambda argv: NS(argv=argv))
    assert launched.argv == ["W.exe", "corp-s3"]


def test_whoami_reports(aws_home):
    moto = pytest.importorskip("moto")
    with moto.mock_aws():
        outcome = aws.whoami("corp")
    assert outcome["profile"] == "corp" and outcome["region"] == "us-east-1"
    assert outcome["account"] and outcome["arn"].startswith("arn:aws:")
    assert 1700 < outcome["expires_in"] <= 1800
