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
import unittest
from unittest.mock import patch

from src.container_file_extract import extract_container_files
from src.container_file_extract import container_file_extract


class TestContainerFileExtract(unittest.TestCase):
    """Unit test for OpenRelik container file extract."""

    def setUp(self):
        self.workflow_id = "test-workflow-id"
        self.input_files = [{"path": "/tmp/fake/disk.img"}]

        self.output_path = "/tmp/fake/ouptput"
        self.temp_dir = os.path.join(self.output_path, "temp_dir")
        self.disk_image_path = os.path.join(self.temp_dir, "disk.img")
        self.log_file_path = os.path.join(
            self.output_path, "container_file_extract.log"
        )
        self.container_output_file = os.path.join(self.output_path, "passwd")

        os.makedirs(self.temp_dir, exist_ok=True)

    def tearDown(self):
        if os.path.exists(self.temp_dir):
            os.rmdir(self.temp_dir)

    @patch("src.container_file_extract.os.path.exists")
    @patch("src.container_file_extract.os.path.isfile")
    @patch("src.container_file_extract.shutil.copyfile")
    def test_extract_container_files(
        self, mock_copyfile, mock_path_isfile, mock_path_exists
    ):
        """Tests extracts contianer files function for success."""
        mock_path_isfile.return_value = True
        mock_path_exists.return_value = True

        log_file = {"path": self.log_file_path}
        filepaths = ["/etc/passwd"]

        output_files = extract_container_files(
            self.input_files[0], self.output_path, log_file, filepaths
        )
        output_file = output_files[0]

        self.assertEqual("passwd", output_file.get("display_name"))
        self.assertEqual("/etc/passwd", output_file.get("original_path"))

    @patch("src.container_file_extract.os.link")
    @patch("src.container_file_extract.mount_disk")
    @patch("src.container_file_extract.mount_container")
    @patch("src.container_file_extract.extract_container_files")
    @patch("src.container_file_extract.subprocess.run")
    @patch("src.container_file_extract.shutil.rmtree")
    def test_container_file_extract(
        self,
        mock_rmtree,
        mock_subprocess_run,
        mock_extract_container_files,
        mock_mount_container,
        mock_mount_disk,
        mock_os_link,
    ):
        """Tests container_file_extract function."""
        task_config = {
            "container_id": "test_container",
            "filepaths": "/etc/passwd",
        }

        mock_mount_disk.return_value = "/mnt"
        mock_mount_container.return_value = "/tmp/mnt"

        mock_extract_container_files.return_value = [
            {
                "uuid": "0d9437173e094637a8b0e7f045b0bbf9",
                "display_name": "passwd",
                "extension": "",
                "data_type": "container:file:extract",
                "path": "/tmp/fake/ouptput/0d9437173e094637a8b0e7f045b0bbf9",
                "original_path": "/etc/passwd",
                "source_file_id": None,
            }
        ]

        result = container_file_extract(
            pipe_result=None,
            input_files=self.input_files,
            output_path=self.output_path,
            workflow_id=self.workflow_id,
            task_config=task_config,
        )

        decoded_result = base64.b64decode(result)
        data = json.loads(decoded_result)

        self.assertEqual(self.workflow_id, data.get("workflow_id"))

        display_names = []
        for output_file in data.get("output_files", []):
            display_name = output_file.get("display_name")
            if display_name:
                display_names.append(display_name)

        expected_display_names = ["passwd", "container_file_extract.log"]
        self.assertListEqual(expected_display_names, display_names)


if __name__ == "__main__":
    unittest.main()
