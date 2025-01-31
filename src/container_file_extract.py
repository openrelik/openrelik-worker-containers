"""Exports files and directory archive from container."""

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
from .utils import log_entry, mount_disk, mount_container

# Task name used to register and route the task to the correct queue.
TASK_NAME = "openrelik-worker-containers.tasks.container_file_extract"

# Task metadata for registration in the core system.
TASK_METADATA = {
    "display_name": "ContainerExplorer: Container File Extract",
    "description": "Extract files from a container",
    "task_config": [
        {
            "name": "container_id",
            "label": "Select the container ID for file extraction",
            "description": "Select one or more file from container",
            "type": "Text",
            "required": True,
        },
        {
            "name": "filepaths",
            "label": "Select filenames to extract",
            "description": "A comma separated list of filenames to extract",
            "type": "text",
            "required": True,
        },
    ],
}

# Note: Not using temp directory.
# Mounting overlay can run into issues with long path to layers.
DISK_MOUNT_DIR = "/mnt"
CONTAINER_MOUNT_DIR = "/tmp/mnt"


def extract_container_files(
    input_file: OutputFile,
    output_path: OutputFile,
    log_file: OutputFile,
    filepaths: List[str],
) -> List[OutputFile]:
    """Extracts container files.

    Args:
        input_file: Input file.
        output_path: Path of output directory.
        log_file: Log file.
        filepaths: Absolute path of files that are extracted.
    """
    output_files = []

    for filepath in filepaths:
        container_filepath = os.path.join(CONTAINER_MOUNT_DIR, filepath[1:])
        if not os.path.exists(container_filepath):
            log_entry(log_file, f"File {filepath} does not exist in container.")
            continue

        if os.path.isfile(container_filepath):
            output_file = create_output_file(
                output_path,
                display_name=os.path.split(filepath)[1],
                original_path=filepath,
                data_type="container:file:extract",
                source_file_id=input_file.get("id"),
            )

            shutil.copyfile(container_filepath, output_file.path)

            output_files.append(output_file.to_dict())

        if os.path.isdir(container_filepath):
            archive_name = f"{os.path.basename(container_filepath)}"

            output_file = create_output_file(
                output_path,
                display_name=archive_name,
                extension="zip",
                original_path=filepath,
                data_type="container:file:extract",
                source_file_id=input_file.get("id"),
            )

            archive_base = os.path.splitext(output_file.path)[0]
            shutil.make_archive(
                archive_base,
                "zip",
                root_dir=os.path.dirname(container_filepath),
                base_dir=os.path.basename(container_filepath),
            )

            output_files.append(output_file.to_dict())

    return output_files


@celery.task(bind=True, name=TASK_NAME, metadata=TASK_METADATA)
def container_file_extract(
    self,
    pipe_result: str = None,
    input_files: list = None,
    output_path: str = None,
    workflow_id: str = None,
    task_config: dict = None,
) -> str:
    """Run Container Explorer on input files.

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

    container_id = task_config.get("container_id")
    filepaths = task_config.get("filepaths").split(",")

    temp_dir = os.path.join(output_path, uuid4().hex)
    os.mkdir(temp_dir)

    # Create directory to mount container
    if not os.path.exists(CONTAINER_MOUNT_DIR):
        os.mkdir(CONTAINER_MOUNT_DIR)

    # Log file to capture logs.
    log_file = create_output_file(
        output_path,
        extension="log",
        display_name="container_file_extract",
    )

    for input_file in input_files:
        # Mount disk image to /mnt on worker
        filename = os.path.basename(input_file.get("path"))
        disk_image_path = os.path.join(temp_dir, filename)

        os.link(input_file.get("path"), disk_image_path)
        disk_mount_dir = mount_disk(disk_image_path, DISK_MOUNT_DIR)
        if not disk_mount_dir:
            log_entry(log_file, f"Unable to mount {input_file.get('id')}")
            continue

        container_mount_point = mount_container(
            container_id=container_id,
            disk_mount_dir=disk_mount_dir,
            container_mount_dir=CONTAINER_MOUNT_DIR,
        )

        if container_mount_point:
            container_output_files = extract_container_files(
                input_file, output_path, log_file, filepaths
            )
            if container_output_files:
                output_files.extend(container_output_files)

            container_unmount_command = ["umount", container_mount_point]
            subprocess.run(container_unmount_command, capture_output=False, check=True)
        else:
            log_entry(log_file, f"Unable to mount container {container_id}.")

        # Clean up
        disk_unmount_command = ["umount", disk_mount_dir]
        subprocess.run(disk_unmount_command, capture_output=False, check=True)

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
