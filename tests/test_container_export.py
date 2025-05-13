# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Unit tests for OpenRelik container_drift task."""

import base64
import json
import os
import tempfile
import unittest
from unittest.mock import patch, MagicMock, call, ANY
from uuid import uuid4

from openrelik_worker_common.file_utils import create_output_file

from src.container_export import (
    _unmount,
    export_container,
    export_all_containers,
    container_export,
)

mock_output_file_instance = MagicMock()
mock_output_file_instance.path = "/fake/output/container_abcdef.zip"
mock_output_file_instance.return_value = "fake_id_123"
mock_output_file_instance.to_dict.return_value = {
    "path": "/fake/output/container_abcdef.zip",
    "display_name": "container_abcdef",
    "id": "fake_id_123",
}

mock_log_file_instance = MagicMock()
mock_log_file_instance.path = "/fake/output/container_export.log"
mock_log_file_instance.to_dict.return_value = {
    "path": "/fake/output/container_export.log",
    "display_name": "container_export",
}


class TestContainerExport(unittest.TestCase):
    """Unit test for OpenRelik container export."""

    def setUp(self):
        self.input_file = MagicMock()
        self.input_file.path = "/fake/disk.raw"
        self.input_file.get.return_value = "disk_id_1"

        self.output_path = "/fake/output"
        self.log_file = mock_log_file_instance
        self.disk_mount_dir = "/mnt/disk"
        self.container_mount_dir = "/mnt/container_abcdef"
        self.container_id = "container_abcdef"
        self.workflow_id = "test_workflow_id"

    @patch("src.container_export.mount_container")
    @patch(
        "src.container_export.create_output_file",
        return_value=mock_output_file_instance,
    )
    @patch("src.container_export.os.path.splitext")
    @patch("src.container_export.shutil.make_archive")
    @patch("src.container_export._unmount")
    @patch("src.container_export.logger")
    def test_export_container_success(
        self,
        mock_logger,
        mock_unmount,
        mock_make_archive,
        mock_splitext,
        mock_create_output,
        mock_mount,
    ):
        """Test successful container export."""
        mounted_path = "/mnt/abcdef"
        mock_mount.return_value = mounted_path
        mock_splitext.return_value = ("/fake/output/container_abcdef", ".zip")

        result = export_container(
            self.input_file,
            self.output_path,
            self.log_file,
            self.disk_mount_dir,
            self.container_mount_dir,
            self.container_id,
        )

        mock_mount.assert_called_once_with(
            container_id=self.container_id,
            disk_mount_dir=self.disk_mount_dir,
            container_mount_dir=self.container_mount_dir,
        )

        mock_create_output.assert_called_once_with(
            self.output_path,
            display_name=self.container_id,
            data_type="container:export:zip",
            extension="zip",
            source_file_id="disk_id_1",
        )

        mock_splitext.assert_called_once_with(mock_output_file_instance.path)

        mock_make_archive.assert_called_once_with(
            "/fake/output/container_abcdef",
            "zip",
            root_dir=mounted_path,
            base_dir=".",
        )
        mock_unmount.assert_called_once_with(mounted_path, self.log_file)
        self.assertEqual(result, mock_output_file_instance)
        mock_logger.info.assert_any_call(
            "Successfully created archive for container %s at %s.zip",
            self.container_id,
            "/fake/output/container_abcdef",
        )

    @patch("src.container_export.os.path.join")
    @patch("src.container_export.os.makedirs")
    @patch("src.container_export.mount_all_containers")
    @patch("src.container_export.os.path.isdir")
    @patch("src.container_export.create_output_file")
    @patch("src.container_export.os.path.splitext")
    @patch("src.container_export.shutil.make_archive")
    @patch("src.container_export.subprocess.run")
    @patch("src.container_export.shutil.rmtree")
    @patch("src.container_export.logger")
    @patch("src.container_export.uuid4")
    def test_export_all_containers_success(
        self,
        mock_uuid,
        mock_logger,
        mock_rmtree,
        mock_unmount_run,
        mock_make_archive,
        mock_splitext,
        mock_create_output_file,
        mock_isdir,
        mock_mount_all_containers,
        mock_makedirs,
        mock_os_path_join,
    ):
        """Test successful all containers export."""
        mock_uuid.return_value = MagicMock(hex="abcdef")
        all_containers_mount_dir = "/mnt/all_abcdef"
        container_ids = ["container1", "container2"]

        mock_mount_all_containers.return_value = container_ids
        mock_isdir.return_value = True

        # Mock create_output_file to return different mocks per container
        mock_output_file_1 = MagicMock(path="/fake/output/all/container1.zip")
        mock_output_file_2 = MagicMock(path="/fake/output/all/container2.zip")
        mock_create_output_file.side_effect = [mock_output_file_1, mock_output_file_2]

        mock_splitext.side_effect = [
            ("/fake/output/all/container1", ".zip"),
            ("/fake/output/all/container2", ".zip"),
        ]
        mock_unmount_run.return_value = MagicMock(returncode=0, stderr="")

        result = export_all_containers(
            self.input_file,
            self.output_path,
            self.log_file,
            self.disk_mount_dir,
        )

        mock_unmount_run.assert_called_once_with(
            ["umount", os.path.join(all_containers_mount_dir, "*")],
            capture_output=True,
            check=False,
            text=True,
            timeout=120,
        )

        self.assertEqual(len(result), 2)
        self.assertIn(mock_output_file_1, result)
        self.assertIn(mock_output_file_2, result)


if __name__ == "__main__":
    unittest.main()
