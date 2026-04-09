"""Unit tests for BWB_SINGULARITY_EXTRA_BINDS parsing (bind_mounts module)."""

from bwb.scheduling_service.executors.bind_mounts import singularity_extra_bind_cli_fragments


def test_empty_when_unset(monkeypatch):
    monkeypatch.delenv("BWB_SINGULARITY_EXTRA_BINDS", raising=False)
    s, d = singularity_extra_bind_cli_fragments()
    assert s == ""
    assert d == ""


def test_single_bind(monkeypatch):
    monkeypatch.setenv("BWB_SINGULARITY_EXTRA_BINDS", "/storage:/storage")
    s, d = singularity_extra_bind_cli_fragments()
    assert s == " -B /storage:/storage"
    assert d == " -v /storage:/storage"


def test_multiple_and_readonly(monkeypatch):
    monkeypatch.setenv(
        "BWB_SINGULARITY_EXTRA_BINDS",
        "/storage:/storage; /data2:/mnt/extra:ro ",
    )
    s, d = singularity_extra_bind_cli_fragments()
    assert "-B /storage:/storage" in s
    assert "-B /data2:/mnt/extra:ro" in s
    assert "-v /storage:/storage" in d
    assert "-v /data2:/mnt/extra:ro" in d


def test_ignores_bad_segments(monkeypatch):
    monkeypatch.setenv("BWB_SINGULARITY_EXTRA_BINDS", "nocolon;/ok:/dst")
    s, d = singularity_extra_bind_cli_fragments()
    assert "/ok:/dst" in s
    assert "nocolon" not in s
