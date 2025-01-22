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
from unittest.mock import patch, mock_open, MagicMock

from src.container_list import container_list
from src.container_list import read_container_explorer_output


class TestContainerListTask(unittest.TestCase):
    """Unit test for OpenRelik container list task."""

    def setUp(self):
        self.workflow_id = "test-workflow-id"
        self.input_files = [{"path": "/tmp/fake/disk.img"}]

        self.output_path = "/tmp/fake/ouptput"
        self.temp_dir = os.path.join(self.output_path, "temp_dir")
        self.disk_image_path = os.path.join(self.temp_dir, "disk.img")
        self.log_file_path = os.path.join(self.output_path, "container_list.log")
        self.container_output_file = os.path.join(
            self.output_path, "container_list.json"
        )

        os.makedirs(self.temp_dir, exist_ok=True)

    def tearDown(self):
        if os.path.exists(self.temp_dir):
            os.rmdir(self.temp_dir)

    @patch("src.container_list.os.path.exists")
    @patch("src.container_list.json.loads")
    def test_read_container_explorer_output_success(
        self, mock_json_loads, mock_path_exists
    ):
        """Tests read_container_explorer_output function for success."""
        mock_path_exists.return_value = True
        mock_json_loads.return_value = [{"container": "test"}]

        mock_open_file = mock_open(read_data='[{"container": "test"}]')
        with patch("builtins.open", mock_open_file):
            result = read_container_explorer_output(self.container_output_file)
            self.assertEqual(result, [{"container": "test"}])

    def test_read_container_explorer_output_file_not_exist(self):
        """Tests read_container_explorer_output for failure."""

        result = read_container_explorer_output("/tmp/non/existent/path")
        self.assertIsNone(result)

    @patch("src.container_drift.get_input_files")
    @patch("src.container_drift.os.mkdir")
    @patch("src.container_drift.os.link")
    @patch("src.container_drift.os.path.exists")
    @patch("src.container_drift.subprocess.run")
    @patch("src.container_drift.shutil.rmtree")
    def test_container_list(
        self,
        mock_rmtree,
        mock_subprocess_run,
        mock_path_exists,
        mock_link,
        mock_mkdir,
        mock_get_input_files,
    ):
        """Tests listing containers."""
        mock_get_input_files.return_value = self.input_files
        mock_subprocess_run.return_value = MagicMock(returncode=0, status="success")
        mock_path_exists.return_value = True

        with patch(
            "src.container_list.read_container_explorer_output",
            return_value=[{"container": "test"}],
        ):
            result = container_list(
                pipe_result=None,
                input_files=self.input_files,
                output_path=self.output_path,
                workflow_id=self.workflow_id,
                task_config=None,
            )

            decoded_result = base64.b64decode(result)
            data = json.loads(decoded_result)

            workflow_id = data.get("workflow_id")
            self.assertEqual(self.workflow_id, workflow_id)

            expected_command = (
                "/opt/container-explorer/bin/ce [--docker-managed] --output json "
                "--image-root /mnt list containers"
            )
            command = data.get("command")
            self.assertEqual(expected_command, command)

            expected_display_names = ["container_list.json", "container_list.log"]

            display_names = []
            for output_file in data.get("output_files", []):
                display_name = output_file.get("display_name")
                if display_name:
                    display_names.append(display_name)

            self.assertListEqual(expected_display_names, display_names)


if __name__ == "__main__":
    unittest.main()
