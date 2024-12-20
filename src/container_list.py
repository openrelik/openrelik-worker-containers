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

    for input_file in input_files:
        filename = os.path.basename(input_file.get("path"))
        disk_image_path = os.path.join(temp_dir, filename)

        os.link(input_file.get("path"), disk_image_path)

        mount_command = ["mount", "-o", "ro,noload", disk_image_path, "/mnt"]

        process = subprocess.run(
            mount_command, capture_output=True, check=False, text=True
        )
        if process.returncode != 0:
            raise RuntimeError(
                "Error mounting disk image ", input_file.get("path"), process.stdout
            )

        containerd_output_file = create_output_file(
            output_path,
            display_name="containerd_list",
            extension="json",
        )

        containerd_command = [
            CE_BINARY,
            "--image-root",
            "/mnt",
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
        if process.returncode != 0:
            raise RuntimeError("Listing containerd containers failed. ", process.stdout)

        # Docker runtime
        docker_output_file = create_output_file(
            output_path, display_name="docker_list", extension="json"
        )

        docker_command = [
            CE_BINARY,
            "--docker-managed",
            "--image-root",
            "/mnt",
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
        if process.returncode != 0:
            raise RuntimeError("Listing Docker containers failed. ", process.stdout)

        # Clean up
        unmount_disk = ["umount", "/mnt"]
        subprocess.run(unmount_disk, capture_output=False, check=True)

        # combined and pretty output
        drifts = []

        with open(containerd_output_file.path, "r", encoding="utf-8") as f:
            data = json.loads(f.read())
            drifts.extend(data)

        with open(docker_output_file.path, "r", encoding="utf-8") as f:
            data = json.loads(f.read())
            drifts.extend(data)

        combined_output = create_output_file(
            output_path, display_name="container_list", extension="json"
        )

        with open(combined_output.path, "w", encoding="utf-8") as f:
            json.dump(drifts, f, indent=4)

        output_files.append(combined_output.to_dict())

    # Clean disk
    if temp_dir:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

    if not output_files:
        raise RuntimeError("Listing containers did not generate output")

    return create_task_result(
        output_files=output_files,
        workflow_id=workflow_id,
        command=(
            "/opt/container-explorer/bin/ce [--docker-managed] --output json"
            " --image-root /mnt list containers"
        ),
        meta={},
    )
