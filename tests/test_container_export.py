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
from unittest.mock import patch

from uuid import uuid4

from openrelik_worker_common.file_utils import create_output_file

from src.container_export import (
    create_container_disk,
    export_containers,
    container_export,
)


class TestContainerExport(unittest.TestCase):
    """Unit test for OpenRelik container export."""

    def setUp(self):
        self.workflow_id = "test-workflow-id"
        self.input_files = [{"path": "/tmp/fake/disk.img"}]

        self.output_path = os.path.join("/tmp", uuid4().hex[:6], "output")

        self.temp_dir = os.path.join(self.output_path, "temp_dir")
        os.makedirs(self.temp_dir, exist_ok=True)

        self.disk_image_path = os.path.join(self.temp_dir, "disk.img")
        self.containers_mount_root = os.path.join("test_data", "containers")

        self.log_file = [
            {"path": os.path.join(self.output_path, "container_export.log")}
        ]

    def tearDown(self):
        if os.path.exists(self.temp_dir):
            os.rmdir(self.temp_dir)

    def test_create_container_disk(self):
        """Test create_container_disk."""

        source_dir = os.path.join("test_data", "containers", "sample_container")

        tempdir = tempfile.mkdtemp()
        image_path = os.path.join(tempdir, "container.img")
        if os.path.exists(image_path):
            os.remove(image_path)

        container_id = "sample_container"

        create_container_disk(
            source_container_dir=source_dir,
            container_disk_image=image_path,
            container_id=container_id,
        )

    def test_export_containers(self):
        """Test export containers."""
        container_ids = ["sample_container"]

        output_files = export_containers(
            input_file=self.input_files[0],
            output_path=self.output_path,
            log_file=self.log_file,
            container_ids=container_ids,
            containers_mount_root=self.containers_mount_root,
        )

        self.assertEqual(1, len(output_files))
        self.assertEqual("sample_container", output_files[0].get("display_name"))

    @patch("src.container_export.os.link")
    @patch("src.container_export.os.makedirs")
    @patch("src.container_export.shutil.rmtree")
    @patch("src.container_export.mount_disk")
    @patch("src.container_export.mount_all_containers")
    @patch("src.container_export.export_containers")
    @patch("src.container_export.subprocess.run")
    def test_container_export(
        self,
        mock_subprocess_run,
        mock_export_containers,
        mock_mount_all_containers,
        mock_mount_disk,
        mock_rmtree,
        mock_makedirs,
        mock_os_link,
    ):
        """Test entry point container_export."""
        task_config = {
            "container_ids": "all",
        }

        mock_output_files = []
        mock_output_file = create_output_file(
            self.output_path,
            display_name="sample_container",
            data_type="container:image:raw",
            source_file_id=self.input_files[0].get("id"),
        )
        mock_output_files.append(mock_output_file.to_dict())

        mock_export_containers.return_value = mock_output_files

        result = container_export(
            pipe_result=None,
            input_files=self.input_files,
            output_path=self.output_path,
            workflow_id=self.workflow_id,
            task_config=task_config,
        )

        decoded_result = base64.b64decode(result)
        data = json.loads(decoded_result)

        self.assertEqual(self.workflow_id, data.get("workflow_id"))


if __name__ == "__main__":
    unittest.main()
