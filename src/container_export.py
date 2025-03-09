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
"""Exports containers as raw disk images."""

import os
import shutil
import subprocess

from typing import List
from uuid import uuid4

from openrelik_worker_common.file_utils import create_output_file, OutputFile
from openrelik_worker_common.task_utils import (
    create_task_result,
    get_input_files,
)

from .app import celery
from .utils import (
    create_disk_image,
    get_directory_size,
    log_entry,
    mount_all_containers,
    mount_container,
    mount_disk,
)

# Task name used to register and route the task to the correct queue.
TASK_NAME = "openrelik-worker-containers.tasks.container_export"

# Task metadata for registration in the core system.
TASK_METADATA = {
    "display_name": "ContainerExplorer: Container Export",
    "description": "Export one or more containers as raw disk images",
    "task_config": [
        {
            "name": "container_ids",
            "label": "A list of containers IDs to export",
            "description": "A comma separated container IDs to export, or all",
            "type": "Text",
            "required": True,
        },
    ],
}


def create_container_disk(
    source_container_dir: str, container_disk_image: str, container_id: str
) -> None:
    """Creates a container disk image from provided path.

    Args:
        source_container_dir: Container root directory.
        container_disk_image: Container disk image.
        container_id: ID of the container.
    """
    container_size = get_directory_size(source_container_dir)
    if not container_size:
        raise ValueError("Expecting container size to be greather than zero")
    size = int(container_size / (1024 * 1024)) + 10

    create_disk_image(
        path=container_disk_image,
        volume_name=container_id,
        size=size,
    )

    target_mount_point = os.path.join("/mnt", uuid4().hex[:6])
    if not os.path.exists(target_mount_point):
        os.makedirs(target_mount_point)

    command = f"mount -o rw {container_disk_image} {target_mount_point}"
    subprocess.run(command, capture_output=True, shell=True, check=True)

    shutil.copytree(
        source_container_dir,
        target_mount_point,
        symlinks=True,
        ignore_dangling_symlinks=True,
        dirs_exist_ok=True,
    )

    subprocess.run(f"umount {target_mount_point}", check=True, shell=True)

    shutil.rmtree(target_mount_point)


def export_containers(
    input_file: OutputFile,
    output_path: str,
    log_file: OutputFile,
    container_ids: List[str],
    containers_mount_root: str,
) -> List[OutputFile]:
    """Export containers as raw disk images.

    Args:
        input_file: Input file.
        output_path: Path of output directory.
        log_file: Log file.
        container_ids: Containers to export.

    Returns:
        A list of output files.
    """
    output_files = []
    export_container_ids = []

    if "all" in container_ids:
        container_dirs = os.listdir(containers_mount_root)
        export_container_ids = container_dirs
    else:
        export_container_ids = container_ids

    for container_id in export_container_ids:
        source_container_dir = os.path.join(containers_mount_root, container_id)

        container_output_file = create_output_file(
            output_path,
            display_name=container_id,
            data_type="container:image:raw",
            source_file_id=input_file.get("id"),
        )

        create_container_disk(
            source_container_dir=source_container_dir,
            container_disk_image=container_output_file.path,
            container_id=container_id,
        )

        output_files.append(container_output_file.to_dict())

    return output_files


@celery.task(bind=True, name=TASK_NAME, metadata=TASK_METADATA)
def container_export(
    self,
    pipe_result: str = None,
    input_files: list = None,
    output_path: str = None,
    workflow_id: str = None,
    task_config: dict = None,
) -> str:
    """Export containers as raw disk images.

    Args:
        pipe_result: Base64-encoded result from the previous Celery task, if any.
        input_files: List of input file dictionaries (unused if pipe_result exists).
        output_path: Path to the output directory.
        workflow_id: ID of the workflow.
        task_config: User configuration for the task.

    Returns:
        Base64-encoded dictionary containing task results.
    """
    input_files = get_input_files(pipe_result, input_files or [])
    output_files = []

    container_ids = task_config.get("container_ids").split(",")

    temp_dir = os.path.join(output_path, uuid4().hex)
    os.makedirs(temp_dir)

    # Log file to capture logs.
    log_file = create_output_file(
        output_path,
        extension="log",
        display_name="container_export",
    )

    for input_file in input_files:
        # Mount disk image to /mnt on worker
        filename = os.path.basename(input_file.get("path"))
        disk_image_path = os.path.join(temp_dir, filename)

        os.link(input_file.get("path"), disk_image_path)

        # Disk image mount point
        disk_mount_dir = os.path.join("/mnt", uuid4().hex[:6])
        if not os.path.exists(disk_mount_dir):
            os.makedirs(disk_mount_dir)

        _disk_mount_dir = mount_disk(disk_image_path, disk_mount_dir)
        if not _disk_mount_dir:
            log_entry(log_file, f"Unable to mount {input_file.get('id')}")
            continue

        # Directory where containers will be mounteds.
        containers_mount_root = os.path.join("/mnt", uuid4().hex[:6])
        if not os.path.join(containers_mount_root):
            os.makedirs(containers_mount_root)

        mount_all_containers(
            path=disk_mount_dir,
            mount_point=containers_mount_root,
        )

        container_images = export_containers(
            input_file=input_file,
            output_path=output_path,
            log_file=log_file,
            container_ids=container_ids,
            containers_mount_root=containers_mount_root,
        )

        if container_images:
            output_files.extend(container_images)
        else:
            log_entry(log_file, f"No container images for {input_file.get('id')}")

        # Clean up
        ## Clean up containers
        command = f"umount {containers_mount_root}/*"
        subprocess.run(command, capture_output=False, check=False, shell=True)
        shutil.rmtree(containers_mount_root)

        command = f"umount {disk_mount_dir}"
        subprocess.run(command, capture_output=False, check=True)
        shutil.rmtree(disk_mount_dir)

    # Clean disk
    if temp_dir:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

    # Add log file to output
    output_files.append(log_file.to_dict())

    return create_task_result(
        output_files=output_files,
        workflow_id=workflow_id,
    )
