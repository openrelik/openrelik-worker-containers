# Copyright 2024 Google LLC
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

import json
import os
import shutil
import subprocess

from typing import List, Dict
from uuid import uuid4

from openrelik_worker_common.file_utils import create_output_file
from openrelik_worker_common.task_utils import (
    create_task_result,
    get_input_files,
)

from .app import celery

# Task name used to register and route the task to the correct queue.
TASK_NAME = "openrelik-worker-containers.tasks.container_list"

# Task metadata for registration in the core system.
TASK_METADATA = {
    "display_name": "ContainerExplorer: List Containers",
    "description": "List containerd and Docker containers",
}

CE_BINARY = "/opt/container-explorer/bin/ce"
DISK_MOUNT_DIR = "/mnt"
CONTAINERD_ROOT_DIR = os.path.join(DISK_MOUNT_DIR, "var", "lib", "containerd")
DOCKER_ROOT_DIR = os.path.join(DISK_MOUNT_DIR, "var", "lib", "docker")


def read_container_explorer_output(path: str) -> List[Dict]:
    """Reads Container Explorer's JSON output and returns list.

    Args:
        path: JSON output file produced by Container Explorer.

    Returns:
        Returns the JSON output file produced by Container Explorer.
    """
    if not os.path.exists(path):
        return None

    data = None
    with open(path, "r", encoding="utf-8") as file_handler:
        try:
            data = json.loads(file_handler.read())
        except (FileNotFoundError, json.decoder.JSONDecodeError):
            return None
    return data


@celery.task(bind=True, name=TASK_NAME, metadata=TASK_METADATA)
def container_list(
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

    temp_dir = os.path.join(output_path, uuid4().hex)
    os.mkdir(temp_dir)

    # Log file to capture logs.
    log_file = create_output_file(
        output_path,
        extension="log",
        display_name="container_list",
    )

    for input_file in input_files:
        filename = os.path.basename(input_file.get("path"))
        disk_image_path = os.path.join(temp_dir, filename)

        os.link(input_file.get("path"), disk_image_path)

        mount_command = [
            "mount",
            "-o",
            "ro,noload",
            disk_image_path,
            DISK_MOUNT_DIR,
        ]

        process = subprocess.run(
            mount_command, capture_output=True, check=False, text=True
        )
        if process.returncode != 0:
            raise RuntimeError(
                "Error mounting disk image ", input_file.get("path"), process.stdout
            )

        # Before proceeding further check if container directories exist.
        if not CONTAINERD_ROOT_DIR and not DOCKER_ROOT_DIR:
            with open(log_file.path, "a", encoding="utf-8") as log_writer:
                log_writer.write("Container root directories does not exist")
                log_writer.write("")

            output_files.append(log_file.to_dict())

            unmount_command = [
                "umount",
                DISK_MOUNT_DIR,
            ]
            subprocess.run(unmount_command, capture_output=False, check=True)

            return create_task_result(
                output_files=output_files,
                workflow_id=workflow_id,
            )

        # container_info holds containers information.
        container_info = []

        # Listing containerd containers
        if os.path.exists(CONTAINERD_ROOT_DIR):
            containerd_output_file = create_output_file(
                output_path,
                display_name="containerd_list",
                extension="json",
            )

            containerd_command = [
                CE_BINARY,
                "--image-root",
                DISK_MOUNT_DIR,
                "--output-file",
                containerd_output_file.path,
                "--output",
                "json",
                "list",
                "containers",
            ]

            process = subprocess.run(
                containerd_command, capture_output=True, check=False, text=True
            )
            if process.returncode == 0:
                containerd_output = read_container_explorer_output(
                    containerd_output_file.path
                )
                if containerd_output:
                    container_info.extend(containerd_output)
            else:
                with open(log_file.path, "a", encoding="utf-8") as log_writer:
                    log_writer.write(
                        f"Error listing containerd containers. {process.stdout}"
                    )
                    log_writer.write("")

        # Listing Docker containers
        if os.path.exists(DOCKER_ROOT_DIR):
            docker_output_file = create_output_file(
                output_path,
                display_name="docker_list",
                extension="json",
            )

            docker_command = [
                CE_BINARY,
                "--docker-managed",
                "--image-root",
                DISK_MOUNT_DIR,
                "--output-file",
                docker_output_file.path,
                "--output",
                "json",
                "list",
                "containers",
            ]

            process = subprocess.run(
                docker_command, capture_output=True, check=False, text=True
            )
            if process.returncode == 0:
                docker_output = read_container_explorer_output(docker_output_file.path)
                if docker_output:
                    container_info.extend(docker_output)
            else:
                with open(log_file.path, "a", encoding="utf-8") as log_writer:
                    log_writer.write(
                        f"Error listing Docker containers. {process.stdout}"
                    )
                    log_writer.write("")

        # Clean up
        unmount_disk = ["umount", DISK_MOUNT_DIR]
        subprocess.run(unmount_disk, capture_output=False, check=True)

    # Combined containerd and Docker container listing and pretty output.
    if container_info:
        combined_output = create_output_file(
            output_path,
            display_name="container_list",
            extension="json",
        )

        with open(combined_output.path, "w", encoding="utf-8") as file_writer:
            json.dump(container_info, file_writer, indent=4)

        output_files.append(combined_output.to_dict())

    # Clean disk
    if temp_dir:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

    if not output_files:
        with open(log_file.path, "a", encoding="utf-8") as log_writer:
            log_writer.write("Container listing did not generate output")
            log_writer.write("")

    # Add log file to output
    output_files.append(log_file.to_dict())

    return create_task_result(
        output_files=output_files,
        workflow_id=workflow_id,
        command=(
            "/opt/container-explorer/bin/ce [--docker-managed] --output json"
            " --image-root /mnt list containers"
        ),
    )
