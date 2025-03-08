"""Utility file for containers worker."""

# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import subprocess

from openrelik_worker_common.file_utils import OutputFile

CE_BINARY = "/opt/container-explorer/bin/ce"


def log_entry(log_file: OutputFile, message: str) -> None:
    """Appends logs line to a log file.

    Args:
        log_file: log file.
        message: log message.
    """
    with open(log_file.path, "a", encoding="utf-8") as log_writer:
        log_writer.write(message)
        log_writer.write("")


def mount_disk(image_path: str, mount_point: str) -> str:
    """Mounts disk image and returns the mount point.

    Args:
        image_path: Disk image path.
        mount_point: Path to mount the disk.

    Returns:
        Mount point or None
    """
    # Assumes the disk is ext4
    # TODO(rmaskey): Check filesystem before mounting disk.
    disk_mount_command = [
        "mount",
        "-o",
        "ro,noload",
        image_path,
        mount_point,
    ]

    process = subprocess.run(
        disk_mount_command, capture_output=True, check=False, text=True
    )
    if process.returncode == 0:
        return mount_point
    return None


def _mount_container(
    container_id: str, container_root_dir: str, container_mount_dir: str
) -> str:
    """Mounts specified container ID and returns the container mount point.

    Args:
        container_id: ID of the container to be mounted.
        container_root_dir: Absolute path of container root.
        container_mount_dir: Path to mount container.

    Returns:
        Path where container is mounted or None.
    """
    # Try mounting as containerd container
    containerd_mount_command = [
        CE_BINARY,
        "--containerd-root",
        container_root_dir,
        "mount",
        container_id,
        container_mount_dir,
    ]

    process = subprocess.run(
        containerd_mount_command, capture_output=True, check=False, text=True
    )
    if process.returncode == 0:
        return container_mount_dir

    # Try mounting as Docker container
    docker_mount_command = [
        CE_BINARY,
        "--docker-root",
        container_root_dir,
        "mount",
        container_id,
        container_mount_dir,
    ]

    process = subprocess.run(
        docker_mount_command, capture_output=True, check=False, text=True
    )
    if process.returncode == 0:
        return container_mount_dir

    return None


def mount_container(
    container_id: str,
    disk_mount_dir: str,
    container_mount_dir: str = None,
    container_root_dir: str = None,
) -> str:
    """Mounts specified container ID and returns the container mount point.

    Args:
        container_id: ID of the container to be mounted.
        disk_mount_dir: Mount point of the disk containing the container.
        container_mount_dir: Path to mount the container. If this value is not specified /tmp/mnt
            is used.
        container_root_dir: Absolute path of the container root directory in the disk.
            If this value is not present, default directory is used for containerd and Docker.

    Returns:
        Path where container is mounted or None.
    """
    container_root_path = None

    if not container_mount_dir:
        container_mount_dir = "/tmp/mnt"

    # Mounting container located at custom directory.
    if container_root_dir:
        container_root_path = os.path.join(disk_mount_dir, container_root_dir)

        _container_mount_dir = _mount_container(
            container_id, container_root_path, container_mount_dir
        )
        if _container_mount_dir:
            return _container_mount_dir

        # If custom container_root_dir is provided, we are not going to check
        # the default locations for Docker and containerd paths.
        return None

    # Attempt mounting as containerd container.
    container_root_path = os.path.join(disk_mount_dir, "var", "lib", "containerd")
    _container_mount_dir = _mount_container(
        container_id, container_root_path, container_mount_dir
    )
    if container_mount_dir:
        return _container_mount_dir

    # Attempt mounting as Docker container.
    container_root_path = os.path.join(disk_mount_dir, "var", "lib", "docker")
    _container_mount_dir = _mount_container(
        container_id, container_root_dir, container_mount_dir
    )
    if container_mount_dir:
        return _container_mount_dir

    return None


def get_directory_size(directory: str) -> int:
    """Calculates the total size of a directory in bytes.

    Args:
        directory: the path of the directory.

    Returns:
        The total size of the directory in bytes, or zero if directory does not exist.
    """
    total_size = 0

    try:
        for root, dirs, files in os.walk(directory):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                try:
                    total_size += os.path.getsize(file_path)
                except OSError:
                    print(f"Unable to get size of {file_path}")
                    continue
        return total_size
    except FileNotFoundError:
        print(f"Directory {directory} not found")
        return 0
    except PermissionError:
        print(f"Permission denied for directory {directory}")
        return 0


def create_disk_image(path: str, volume_name: str, size: int) -> None:
    """Creates a disk image and formats to EXT4.

    Args:
        path: Path of the disk to be created.
        volume_name: Volume name to set for the disk.
        size: Size of the disk in MB.

    Returns:
        The path of the disk image, or None.
    """
    command = f"dd if=/dev/zero of={path} bs=1M count={size} status=none"
    subprocess.run(command, check=False, shell=True, capture_output=True)

    command = f"mkfs.ext4 -q -L {volume_name} {path}"
    subprocess.run(command, check=False, shell=True, capture_output=True)

    return path
