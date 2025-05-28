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

"""Utility for exporting containers."""

import logging
import os
import subprocess

from typing import List, Optional

from openrelik_worker_common.file_utils import OutputFile

logger = logging.getLogger(__name__)

CE_BINARY = "/opt/container-explorer/bin/ce"


def log_entry(log_file: OutputFile, message: str) -> None:
    """Appends logs line to a log file.

    Args:
        log_file: log file.
        message: log message.
    """
    try:
        with open(log_file.path, "a", encoding="utf-8") as log_writer:
            log_writer.write(message)
            log_writer.write("\n")
    except Exception as e:
        logger.error("Failed to write to log file %s: %s", log_file.path, e)
        logger.info("Original log message: %s", message)


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
    logger.info("Attempting to mount disk %s at %s", image_path, mount_point)
    logger.debug("Mount command: %s", " ".join(disk_mount_command))

    try:
        process = subprocess.run(
            disk_mount_command,
            capture_output=True,
            check=False,
            text=True,
            timeout=60,
        )
        if process.returncode == 0:
            logger.info("Successfully mounted disk %s at %s", image_path, mount_point)
            return mount_point
        else:
            logger.error(
                "Failed to mount disk %s. Return code: %d, Stderr: %s",
                image_path,
                process.returncode,
                process.stderr.strip(),
            )
            return None
    except subprocess.TimeoutExpired:
        logger.error("Timeout expired while mounting disk %s", image_path)
        return None
    except Exception as e:
        logger.error(
            "Exception occurred while mounting disk %s: %s",
            image_path,
            e,
            exc_info=True,
        )
        return None

def unmount_disk(mount_point: str, log_file: Optional[OutputFile] = None) -> None:
    """Safely unmounts a given mount points."""
    if not mount_point or not os.path.ismount(mount_point):
        logger.debug("Skipping unmount for non-mount point %s", mount_point)
        return None

    logger.info("Attempting to unmount: %s", mount_point)
    unmount_command = ["umount", mount_point]
    try:
        process = subprocess.run(
            unmount_command,
            capture_output=True,
            check=False,
            text=True,
            timeout=60,
        )
        if process.returncode == 0:
            logger.info("Successfully unmounted: %s", mount_point)
        else:
            message = (
                f"Failed to unmount: {mount_point}. Return code: {process.returncode}, "
                f"Stderr: {process.stderr.strip()}"
            )
            logger.error(message)
            if log_file:
                log_entry(log_file, message)
    except subprocess.TimeoutExpired:
        logger.error("Timeout expired while unmounting: %s", mount_point)
    except Exception as e:
        logger.error(
            "Exception occurred while unmounting: %s: %s", mount_point, e, exc_info=True
        )
        if log_file:
            log_entry(
                log_file, f"Exception occurred while unmounting: {mount_point}: {e}"
            )

    return None


def _mount_containerd_container(
    container_id: str, container_root_dir: str, container_mount_dir: str
) -> str | None:
    """Mounts specified containerd container and returns the container mount point.

    Args:
        container_id: ID of the container to be mounted.
        container_root_dir: Absolute path of container root.
        container_mount_dir: Path to mount container.

    Returns:
        Path where container is mounted or None.
    """
    containerd_mount_command = [
        CE_BINARY,
        "--containerd-root",
        container_root_dir,
        "mount",
        container_id,
        container_mount_dir,
    ]
    logger.info(
        "Attempting to mount containerd container %s from %s to %s",
        container_id,
        container_root_dir,
        container_mount_dir,
    )
    logger.debug("Containerd mount command: %s", " ".join(containerd_mount_command))

    try:
        process = subprocess.run(
            containerd_mount_command,
            capture_output=True,
            check=False,
            text=True,
            timeout=60,
        )
        if process.returncode == 0:
            logger.info(
                "Successfully mounted containerd container %s at %s",
                container_id,
                container_mount_dir,
            )
            return container_mount_dir
        else:
            logger.warning(
                "Failed to mount as containerd container %s from %s. Stderr: %s",
                container_id,
                container_root_dir,
                process.stderr.strip(),
            )
    except subprocess.TimeoutExpired:
        logger.error(
            "Timeout expired while mounting containerd container %s from %s",
            container_id,
            container_root_dir,
        )
    except Exception as e:
        logger.error(
            "Exception occurred while mounting containerd container %s from %s: %s",
            container_id,
            container_root_dir,
            e,
            exc_info=True,
        )

    return None


def _mount_docker_container(
    container_id: str, container_root_dir: str, container_mount_dir: str
) -> str | None:
    """Mounts specified containerd container and returns the container mount point.

    Args:
        container_id: ID of the container to be mounted.
        container_root_dir: Absolute path of container root.
        container_mount_dir: Path to mount container.

    Returns:
        Path where container is mounted or None.
    """
    docker_mount_command = [
        CE_BINARY,
        "--docker-managed",
        "--docker-root",
        container_root_dir,
        "mount",
        container_id,
        container_mount_dir,
    ]
    logger.info(
        "Attempting to mount Docker container %s from %s to %s",
        container_id,
        container_root_dir,
        container_mount_dir,
    )
    logger.debug("Docker mount command: %s", " ".join(docker_mount_command))

    try:
        process = subprocess.run(
            docker_mount_command,
            capture_output=True,
            check=False,
            text=True,
            timeout=60,
        )
        if process.returncode == 0:
            logger.info(
                "Successfully mounted Docker container %s at %s",
                container_id,
                container_mount_dir,
            )
            return container_mount_dir
        else:
            logger.error(
                "Failed to mount as Docker container %s from %s. Return code: %d, Stderr: %s",
                container_id,
                container_root_dir,
                process.returncode,
                process.stderr.strip(),
            )
    except subprocess.TimeoutExpired:
        logger.error(
            "Timeout expired while mounting Docker container %s from %s",
            container_id,
            container_root_dir,
        )
    except Exception as e:
        logger.error(
            "Exception occurred while mounting Docker container %s from %s: %s",
            container_id,
            container_root_dir,
            e,
            exc_info=True,
        )

    return None


def _mount_container(
    container_id: str, container_root_dir: str, container_mount_dir: str
) -> str | None:
    """Mounts specified container ID and returns the container mount point.

    Args:
        container_id: ID of the container to be mounted.
        container_root_dir: Absolute path of container root.
        container_mount_dir: Path to mount container.

    Returns:
        Path where container is mounted or None.
    """
    if not os.path.exists(container_root_dir):
        logger.debug(
            "Container root directory %s does not exist, skipping mount attempt.",
            container_root_dir,
        )
        return None

    # Try mounting as containerd container
    returned_container_mount_dir = _mount_containerd_container(
        container_id, container_root_dir, container_mount_dir
    )
    if returned_container_mount_dir:
        return returned_container_mount_dir

    # Try mounting as Docker container
    returned_container_mount_dir = _mount_docker_container(
        container_id, container_root_dir, container_mount_dir
    )
    if returned_container_mount_dir:
        return returned_container_mount_dir

    return None


def mount_container(
    container_id: str,
    disk_mount_dir: str,
    container_mount_dir: str,
    container_root_dir: Optional[str] = None,
) -> str | None:
    """Mounts specified container ID and returns the container mount point.

    Args:
        container_id: ID of the container to be mounted.
        disk_mount_dir: Mount point of the disk containing the container.
        container_mount_dir: Path to mount the container.
        container_root_dir: Absolute path of the container root directory in the disk.
            If this value is not present, default directory is used for containerd and Docker.

    Returns:
        Path where container is mounted or None.
    """
    logger.info("Attempting to mount container ID: %s", container_id)

    container_root_path = None

    # Mounting container located at custom directory.
    if container_root_dir:
        container_root_path = os.path.join(disk_mount_dir, container_root_dir)
        logger.info("Using custom container root path: %s", container_root_path)

        _container_mount_dir = _mount_container(
            container_id, container_root_path, container_mount_dir
        )
        if _container_mount_dir:
            return _container_mount_dir

        # If custom container_root_dir is provided, we are not going to check
        # the default locations for Docker and containerd paths.
        logger.error(
            "Failed to mount container %s from custom path %s",
            container_id,
            container_root_path,
        )
        return None

    # Attempt mounting as containerd container.
    container_root_path = os.path.join(disk_mount_dir, "var", "lib", "containerd")
    logger.info("Trying default containerd root path: %s", container_root_path)
    _container_mount_dir = _mount_container(
        container_id, container_root_path, container_mount_dir
    )
    if _container_mount_dir:
        return _container_mount_dir
    logger.info(
        "Mount attempt failed for default containerd path %s", container_root_path
    )

    # Attempt mounting as Docker container.
    container_root_path = os.path.join(disk_mount_dir, "var", "lib", "docker")
    logger.info("Trying default Docker root path: %s", container_root_path)
    _container_mount_dir = _mount_container(
        container_id, container_root_path, container_mount_dir
    )
    if _container_mount_dir:
        return _container_mount_dir
    logger.info("Mount attempt failed for default Docker path %s", container_root_path)

    logger.error("Failed to mount container %s using default paths.", container_id)
    return None


def _mount_all_containerd_containers(
    container_mount_dir: str, container_root_dir: str
) -> str | None:
    """Mounts all containerd containers and returns container type.

    Args:
        container_mount_dir: Root directory where containers will be mounted under subdirectories.
        container_root_dir: Container root directory.

    Returns:
        "containerd" if successful else None.
    """
    containerd_mount_command = [
        CE_BINARY,
        "--support-container-data",
        "/opt/container-explorer/etc/supportcontainer.yaml",
        "--containerd-root",
        container_root_dir,
        "mount-all",
        container_mount_dir,
    ]
    logger.info(
        "Attempting to mount all containerd containers from %s to %s",
        container_root_dir,
        container_mount_dir,
    )

    logger.debug("Containerd mount-all command: %s", " ".join(containerd_mount_command))

    try:
        process = subprocess.run(
            containerd_mount_command,
            capture_output=True,
            check=False,
            text=True,
            timeout=120,
        )
        if process.returncode == 0:
            logger.info(
                "Successfully ran mount-all for containerd from %s", container_root_dir
            )
            return "containerd"
        else:
            logger.warning(
                "Failed mount-all for containerd from %s. Stderr: %s",
                container_root_dir,
                process.stderr.strip(),
            )
    except subprocess.TimeoutExpired:
        logger.warning(
            "Timeout expired while mounting all containerd containers from %s",
            container_root_dir,
        )
    except Exception as e:
        logger.warning(
            "Exception occurred while mounting all containerd containers from %s: %s",
            container_root_dir,
            e,
            exc_info=True,
        )

    return None


def _mount_all_docker_containers(
    container_mount_dir: str, container_root_dir: str
) -> str | None:
    """Mounts all Docker containers and returns container type.

    Args:
        container_mount_dir: Root directory where containers will be mounted under subdirectories.
        container_root_dir: Container root directory.

    Returns:
        "docker" if successful else None.
    """
    docker_mount_command = [
        CE_BINARY,
        "--docker-managed",
        "--docker-root",
        container_root_dir,
        "mount-all",
        container_mount_dir,
    ]
    logger.info(
        "Attempting to mount all for Docker containers from %s to %s",
        container_root_dir,
        container_mount_dir,
    )
    logger.debug("Docker mount-all command: %s", " ".join(docker_mount_command))

    try:
        process = subprocess.run(
            docker_mount_command,
            capture_output=True,
            check=False,
            text=True,
            timeout=120,
        )
        if process.returncode == 0:
            logger.info(
                "Successfully ran mount-all for Docker containers from %s",
                container_root_dir,
            )
            return "docker"
        else:
            logger.warning(
                "Failed mount-all for Docker continers from %s. Stderr: %s",
                container_root_dir,
                process.stderr.strip(),
            )
    except subprocess.TimeoutExpired:
        logger.warning(
            "Timeout expired during moun-all for Docker containers from %s",
            container_root_dir,
        )
    except Exception as e:
        logger.warning(
            "Exception during mount-all for Docker containers from %s: %s",
            container_root_dir,
            e,
            exc_info=True,
        )

    return None


def _mount_all_containers(
    container_mount_dir: str, container_root_dir: str
) -> str | None:
    """Mounts all containers and returns the list of containers.

    Args:
        container_mount_dir: Root directory where containers will be mounted under subdirectories.
        container_root_dir: Container root directory.
    """
    if not os.path.exists(container_root_dir):
        logger.debug(
            "Container root directory %s does not exist, skipping mount-all attempt.",
            container_root_dir,
        )
        return None

    # Try mounting as containerd container
    returned_container_type = _mount_all_containerd_containers(
        container_mount_dir, container_root_dir
    )
    if returned_container_type:
        return returned_container_type

    # Try mounting as Docker container
    returned_container_type = _mount_all_docker_containers(
        container_mount_dir, container_root_dir
    )
    if returned_container_type:
        return returned_container_type

    return None


def mount_all_containers(
    path: str,
    container_mount_dir: str,
    container_root_dir: Optional[str] = None,
    log_file: Optional[OutputFile] = None,
) -> List[str]:
    """Mounts all containers and returns the list of containers.

    Args:
        path: Mount point of the disk containing the containers.
        container_mount_dir: Root directory where containers will be mounted under subdirectories.
        container_root_dir: Custom container root directory. i.e. other than /var/lib/docker and
                /var/lib/containerd.

    Returns:
        List of mounted container IDs.
    """
    mounted_something = False
    container_root_path = None

    logger.info(
        "Attempting to mount all containers from disk mounted at %s into %s",
        path,
        container_mount_dir,
    )

    # Mounting custom container root
    if container_root_dir:
        container_root_path = os.path.join(path, container_root_dir)
        logger.info("Trying custom container root path: %s", container_root_path)

        container_type = _mount_all_containers(container_mount_dir, container_root_path)
        if not container_type:
            logger.warning(
                "Failed to mount containers from custom path %s", container_root_path
            )
            if log_file:
                log_entry(
                    log_file,
                    f"Error mounting conainers from container root {container_root_dir}",
                )
        else:
            mounted_something = True
            logger.info(
                "Successfully mounted containers (type: %s) from custom path %s",
                container_type,
                container_root_path,
            )
    else:
        # Trying mounting containerd containers from default path
        container_root_path = os.path.join(path, "var", "lib", "containerd")
        logger.info("Trying default containerd root path: %s", container_root_path)

        container_type = _mount_all_containers(container_mount_dir, container_root_path)
        if container_type:
            logger.info(
                "Successfully mounted containers (type: %s) from default path %s",
                container_type,
                container_root_path,
            )
            mounted_something = True
        else:
            logger.info(
                "Failed or no containers found at default containerd path %s",
                container_root_path,
            )
            if log_file:
                log_entry(log_file, "Error mounting containerd containers")

        # Try mounting Docker containers from default path
        container_root_path = os.path.join(path, "var", "lib", "docker")
        logger.info("Trying default Docker root path: %s", container_root_path)

        container_type = _mount_all_containers(container_mount_dir, container_root_path)
        if container_type:
            logger.info(
                "Sucessfully mounted containers (type: %s) from default path %s",
                container_type,
                container_root_path,
            )
            mounted_something = True
        else:
            logger.info(
                "Failed or no containers found at default Docker path %s",
                container_root_path,
            )
            if log_file:
                log_entry(log_file, "Error mounting Docker containers")

    if not mounted_something:
        logger.warning("Failed to mount any containers using available paths")
        return []

    try:
        mounted_dirs = os.listdir(container_mount_dir)
        logger.info(
            "Found directories in mount point %s: %d",
            container_mount_dir,
            len(mounted_dirs),
        )
        return mounted_dirs
    except FileNotFoundError:
        logger.error(
            "Container mount directory %s not found after mount attempts.",
            container_mount_dir,
        )
        return []
    except Exception as e:
        logger.error(
            "Error listing directory %s: %s", container_mount_dir, e, exc_info=True
        )
        return []
