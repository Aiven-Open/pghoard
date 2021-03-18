# Copyright (c) 2021 Aiven, Helsinki, Finland. https://aiven.io/
import os

import pytest

from pghoard.rohmu.delta.common import Progress, SnapshotHash


@pytest.mark.timeout(2)
def test_snapshot(snapshotter):
    with snapshotter.lock:
        # Start with empty
        assert snapshotter.snapshot(progress=Progress()) == 0
        src = snapshotter.src
        dst = snapshotter.dst
        assert not (dst / "foo").is_file()

        # Create files in src, run snapshot
        snapshotter.create_4foobar()
        ss2 = snapshotter.get_snapshot_state()

        assert (dst / "foo").is_file()
        assert (dst / "foo").read_text() == "foobar"
        assert (dst / "foo2").read_text() == "foobar"

        hashes = snapshotter.get_snapshot_hashes()
        assert len(hashes) == 1
        assert hashes == [
            SnapshotHash(hexdigest="c6479bce75c9a573ba073af83191c280721170793da6e9e9480201de94ab0654", size=900)
        ]

        while True:
            (src / "foo").write_text("barfoo")  # same length
            if snapshotter.snapshot(progress=Progress()) > 0:
                # Sometimes fails on first iteration(s) due to same mtime
                # (inaccurate timestamps)
                break
        ss3 = snapshotter.get_snapshot_state()
        assert ss2 != ss3
        assert snapshotter.snapshot(progress=Progress()) == 0
        assert (dst / "foo").is_file()
        assert (dst / "foo").read_text() == "barfoo"

        # Remove file from src, run snapshot
        for filename in ["foo", "foo2", "foobig", "foobig2"]:
            (src / filename).unlink()
            assert snapshotter.snapshot(progress=Progress()) > 0
            assert snapshotter.snapshot(progress=Progress()) == 0
            assert not (dst / filename).is_file()

        # Now shouldn't have any data hashes
        hashes_empty = snapshotter.get_snapshot_hashes()
        assert not hashes_empty

    with pytest.raises(AssertionError):
        snapshotter.snapshot(progress=Progress())

    with pytest.raises(AssertionError):
        snapshotter.get_snapshot_state()

    with pytest.raises(AssertionError):
        snapshotter.get_snapshot_hashes()


@pytest.mark.parametrize("test", [(os, "link", 1, 1), (None, "_snapshotfile_from_path", 3, 0)])
def test_snapshot_error_filenotfound(snapshotter, mocker, test):
    (obj, fun, exp_progress_1, exp_progress_2) = test

    def _not_really_found(*a, **kw):
        raise FileNotFoundError

    obj = obj or snapshotter
    mocker.patch.object(obj, fun, new=_not_really_found)
    (snapshotter.src / "foo").write_text("foobar")
    (snapshotter.src / "bar").write_text("foobar")
    with snapshotter.lock:
        progress = Progress()
        assert snapshotter.snapshot(progress=progress) == exp_progress_1
        progress = Progress()
        assert snapshotter.snapshot(progress=progress) == exp_progress_2
