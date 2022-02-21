import pipelime.filesystem.remotes as plr
from pipelime.sequences.readers.filesystem import UnderfolderReader
from pipelime.sequences.samples import FileSystemSample

from filecmp import cmp
from typing import Mapping


class TestRemotes:
    def _upload_download(
        self,
        temp_folder,
        srcdata_folder: str,
        remote: plr.BaseRemote,
        remote_base_path: str,
    ):
        # upload
        source_to_remote: Mapping[str, str] = {}
        for sample in UnderfolderReader(srcdata_folder, copy_root_files=False):
            assert isinstance(sample, FileSystemSample)
            for k, v in sample.filesmap.items():
                remote_url = remote.upload_file(v, remote_base_path)
                assert remote_url is not None
                source_to_remote[v] = remote_url

        # download and compare
        local_root = temp_folder / "local"
        local_root.mkdir(parents=True)

        if remote_base_path.startswith('/'):
            remote_base_path = remote_base_path[1:]

        for original, rm_url in source_to_remote.items():
            rm, rm_base_path, rm_name = plr.get_remote_and_paths(rm_url)
            assert isinstance(rm, type(remote))
            assert isinstance(rm_base_path, str)
            assert isinstance(rm_name, str)
            assert rm_base_path == remote_base_path

            local_file = local_root / rm_name
            assert rm.download_file(local_file, rm_base_path, rm_name)
            assert cmp(str(local_file), original, shallow=False)

    def test_file_remote(self, toy_dataset_small, tmp_path):
        file_remote = plr.create_remote("file", "localhost")
        assert file_remote is not None

        srcdata_folder = toy_dataset_small["folder"]

        remote_root = tmp_path / "remote"
        remote_root.mkdir(parents=True)
        remote_root = remote_root.as_posix()

        self._upload_download(tmp_path, srcdata_folder, file_remote, remote_root)

    def test_s3_remote(self, toy_dataset_small, tmp_path):
        file_remote = plr.create_remote("file", "localhost")
        assert file_remote is not None

        srcdata_folder = toy_dataset_small["folder"]

        remote_root = tmp_path / "remote"
        remote_root.mkdir(parents=True)
        remote_root = remote_root.as_posix()

        self._upload_download(tmp_path, srcdata_folder, file_remote, remote_root)
