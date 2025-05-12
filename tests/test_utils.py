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

"""Unit test for OpenRelik container utils."""

import logging
import os
import subprocess
import unittest

from unittest import mock

from src.utils import _mount_all_containerd_containers
from src.utils import _mount_all_docker_containers
from src.utils import _mount_containerd_container
from src.utils import _mount_docker_container
from src.utils import CE_BINARY
from src.utils import mount_disk


class TestContainerWorkerUtils(unittest.TestCase):
    """Test container worker utils."""

    def setUp(self):
        # Mock the logger used in the module.
        self.mock_logger = mock.MagicMock(specs=logging.Logger)
        self.patcher_logger = mock.patch("src.utils.logger", self.mock_logger)
        self.patcher_logger.start()

        # Mock subprocess.run
        self.mock_subprocess_run = mock.MagicMock(specs=subprocess.run)
        self.patcher_subprocess = mock.patch("subprocess.run", self.mock_subprocess_run)
        self.patcher_subprocess.start()

        # Mock os.path.exists
        self.mock_os_path_exists = mock.MagicMock(specs=os.path.exists)
        self.patcher_os_path_exists = mock.patch(
            "os.path.exists", self.mock_os_path_exists
        )
        self.patcher_os_path_exists.start()

        # Mock os.listdir
        self.mock_listdir = mock.MagicMock(specs="os.listdir")
        self.patcher_listdir = mock.patch("os.listdir", self.mock_listdir)
        self.patcher_listdir.start()

    def tearDown(self):
        self.patcher_logger.stop()
        self.patcher_subprocess.stop()
        self.patcher_os_path_exists.stop()
        self.patcher_listdir.stop()

    def test_mount_disk_success(self):
        """Test sucessful disk mount."""
        image_path = "/images/disk.img"
        mount_point = "/mnt/abcdef"
        self.mock_subprocess_run.return_value = mock.MagicMock(
            returncode=0, stdout="Mounted successfully", stderr=""
        )

        result = mount_disk(image_path, mount_point)
        self.assertEqual(result, mount_point)

        self.mock_subprocess_run.assert_called_once_with(
            ["mount", "-o", "ro,noload", image_path, mount_point],
            capture_output=True,
            check=False,
            text=True,
            timeout=60,
        )
        self.mock_logger.info.assert_any_call(
            "Successfully mounted disk %s at %s", image_path, mount_point
        )
        self.mock_logger.error.assert_not_called()

    def test_mount_disk_failure(self):
        """Test failed disk mount."""
        image_path = "/images/disk.img"
        mount_point = "/mnt/abcdef"
        self.mock_subprocess_run.return_value = mock.MagicMock(
            returncode=1, stdout="", stderr="mount: failed"
        )

        result = mount_disk(image_path, mount_point)
        self.assertIsNone(result)

        self.mock_subprocess_run.assert_called_once()
        self.mock_logger.error.assert_called_once_with(
            "Failed to mount disk %s. Return code: %d, Stderr: %s",
            image_path,
            1,
            "mount: failed",
        )

    def test_mount_containerd_container_success(self):
        """Test successful containerd container mount."""
        container_id = "abc123edf"
        container_root_dir = "/mnt/abcdef/var/lib/containerd"
        container_mount_dir = "/mnt/aabbcc"

        self.mock_os_path_exists.return_value = True
        self.mock_subprocess_run.return_value = mock.MagicMock(
            returncode=0,
            stdout="mounted container",
            stderr="",
        )

        result = _mount_containerd_container(
            container_id, container_root_dir, container_mount_dir
        )
        self.assertEqual(result, container_mount_dir)

        self.mock_subprocess_run.assert_called_once_with(
            [
                CE_BINARY,
                "--containerd-root",
                container_root_dir,
                "mount",
                container_id,
                container_mount_dir,
            ],
            capture_output=True,
            check=False,
            text=True,
            timeout=60,
        )
        self.mock_logger.info.assert_any_call(
            "Successfully mounted containerd container %s at %s",
            container_id,
            container_mount_dir,
        )

    def test_mount_docker_container_success(self):
        """Test successful Docker container mount."""
        container_id = "abc123def"
        container_root_dir = "/mnt/abcdef/var/lib/docker"
        container_mount_dir = "/mnt/aabbcc"

        self.mock_os_path_exists.return_value = True
        self.mock_subprocess_run.return_value = mock.MagicMock(
            returncode=0,
            stdout="mounted container",
            stderr="",
        )

        result = _mount_docker_container(
            container_id, container_root_dir, container_mount_dir
        )
        self.assertEqual(result, container_mount_dir)

        self.mock_subprocess_run.assert_called_once_with(
            [
                CE_BINARY,
                "--docker-managed",
                "--docker-root",
                container_root_dir,
                "mount",
                container_id,
                container_mount_dir,
            ],
            capture_output=True,
            check=False,
            text=True,
            timeout=60,
        )
        self.mock_logger.info.assert_any_call(
            "Successfully mounted Docker container %s at %s",
            container_id,
            container_mount_dir,
        )

    def test_mount_all_containerd_containers_success(self):
        """Test mounting all containerd containers."""
        container_root_dir = "/mnt/abcdef/var/lib/containerd"
        container_mount_dir = "/mnt/aabbcc"

        self.mock_subprocess_run.return_value = mock.MagicMock(
            returncode=0,
            stdout="",
            stderr="",
            timeout=60,
        )

        result = _mount_all_containerd_containers(
            container_mount_dir, container_root_dir
        )
        self.assertEqual(result, "containerd")

        self.mock_subprocess_run.assert_called_once_with(
            [
                CE_BINARY,
                "--support-container-data",
                "/opt/container-explorer/etc/supportcontainer.yaml",
                "--containerd-root",
                container_root_dir,
                "mount-all",
                container_mount_dir,
            ],
            capture_output=True,
            check=False,
            text=True,
            timeout=120,
        )
        self.mock_logger.info.assert_any_call(
            "Successfully ran mount-all for containerd from %s", container_root_dir
        )

    def test_mount_all_docker_containers_success(self):
        """Test mounting all Docker containers."""
        container_root_dir = "/mnt/abcdef/var/lib/docker"
        container_mount_dir = "/mnt/aabbcc"

        self.mock_subprocess_run.return_value = mock.MagicMock(
            returncode=0,
            stdout="",
            stderr="",
            timeout=60,
        )

        result = _mount_all_docker_containers(container_mount_dir, container_root_dir)
        self.assertEqual(result, "docker")

        self.mock_subprocess_run.assert_called_once_with(
            [
                CE_BINARY,
                "--docker-managed",
                "--docker-root",
                container_root_dir,
                "mount-all",
                container_mount_dir,
            ],
            capture_output=True,
            check=False,
            text=True,
            timeout=120,
        )
        self.mock_logger.info.assert_any_call(
            "Successfully ran mount-all for Docker containers from %s",
            container_root_dir,
        )


if __name__ == "__main__":
    unittest.main()
