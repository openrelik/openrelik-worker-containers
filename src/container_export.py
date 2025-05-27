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

"""Exports container to a `.img` disk image or `.tar.gz` archive."""

import logging
import os
import shutil
import subprocess

from typing import Any, Dict, List
from uuid import uuid4

from openrelik_worker_common.file_utils import create_output_file, OutputFile
from openrelik_worker_common.task_utils import (
    create_task_result,
    get_input_files,
)

from .app import celery
from .utils import CE_BINARY, log_entry, mount_disk, unmount_disk

# Set up logging
logger = logging.getLogger(__name__)

# Task name used to register and route the task to the correct queue.
TASK_NAME = "openrelik-worker-containers.tasks.container_export"

# Task metadata for registration in the core system.
TASK_METADATA = {
    "display_name": "Containers: Export Container",
    "description": "Export container to a raw disk image or archive.",
    "task_config": [
        {
            "name": "container_id",
            "label": "Container ID to export",
            "description": "Comma separated container IDs to export",
            "type": "Text",
            "required": False,
        },
        {
            "name": "export_image",
            "label": "Export container as disk image",
            "type": "checkbox",
        },
        {
            "name": "export_archive",
            "label": "Export container as archive",
            "type": "checkbox",
        },
    ],
}


def export_container(
    input_file: Dict[str, Any],
    output_path: str,
    log_file: OutputFile,
    disk_mount_dir: str,
    container_id: str,
    task_config: Dict[str, str],
) -> List[OutputFile]:
    """Exports container as disk image or an archive file."""
    logger.info("Attempting to export container ID: %s", container_id)

    container_export_dir = os.path.join(output_path, uuid4().hex)
    os.mkdir(container_export_dir)
    logger.debug("Created container export directory %s", container_export_dir)

    export_command = [
        CE_BINARY,
        "--image-root",
        disk_mount_dir,
        "export",
        container_id,
        container_export_dir,
    ]

    # Container-Explorer binary supports exporting as disk image and .tar.gz archive.
    # Building the export type based on user selection.
    if task_config.get("export_image"):
        export_command.append("--image")
    if task_config.get("export_archive"):
        export_command.append("--archive")

    # Using default if user did not selection an export type.
    if "--image" not in export_command and "--archive" not in export_command:
        export_command.append("--image")

    output_files = []
    logger.debug(
        "Running container-explorer export command %s", " ".join(export_command)
    )

    process = subprocess.run(
        export_command, capture_output=True, check=False, text=True
    )
    if process.returncode == 0:
        exported_containers = os.listdir(container_export_dir)
        for exported_container in exported_containers:
            logger.debug(
                "Exported container %s in export directory %s",
                exported_container,
                container_export_dir,
            )

            output_file = create_output_file(
                output_path,
                display_name=exported_container,
                data_type="image",
                extension=exported_container.split(".")[-1],
                source_file_id=input_file.get("id"),
            )

            # Converting ContainerExplorer generated output file to OpenRelik compatible name
            # and location.
            shutil.move(
                os.path.join(container_export_dir, exported_container), output_file.path
            )

            # Fix double extension in display_name
            output_file.display_name = exported_container

            logger.info(f"Exporting container {container_id} as {output_file.path}")
            log_entry(
                log_file, f"Exporting container {container_id} as {exported_container}"
            )

            output_files.append(output_file)

        # Clean up temporary folder
        shutil.rmtree(container_export_dir)
        logger.debug("Deleted container export directory %s", container_export_dir)

    else:
        log_entry(
            log_file,
            f"Error exporting container {container_id} in disk "
            f"{os.path.basename(input_file.get('path', ''))}",
        )

    return output_files


def export_all_containers(
    input_file: Dict[str, Any],
    output_path: str,
    log_file: OutputFile,
    disk_mount_dir: str,
    task_config: Dict[str, str],
) -> List[OutputFile]:
    """Exports all containers disk image (.img) or archive (.tar.gz)."""
    logger.info(
        "Attempting to export all containers on disk mounted at %s", disk_mount_dir
    )

    container_export_dir = os.path.join(output_path, uuid4().hex)
    os.mkdir(container_export_dir)
    logger.debug("Created container export directory %s", container_export_dir)

    export_command = [
        CE_BINARY,
        "--image-root",
        disk_mount_dir,
        "export-all",
        container_export_dir,
    ]

    # Container-Explorer binary supports exporting as disk image and .tar.gz archive.
    # Building the export type based on user selection.
    if task_config.get("export_image"):
        export_command.append("--image")
    if task_config.get("export_archive"):
        export_command.append("--archive")

    # Using default if user did not selection an export type.
    if "--image" not in export_command and "--archive" not in export_command:
        export_command.append("--image")

    output_files = []

    logger.debug(
        "Running container-explorer export command %s", " ".join(export_command)
    )

    process = subprocess.run(
        export_command, capture_output=True, check=False, text=True
    )
    if process.returncode == 0:
        exported_containers = os.listdir(container_export_dir)
        for exported_container in exported_containers:
            logger.debug(
                "Exported container %s in export directory %s",
                exported_container,
                container_export_dir,
            )

            output_file = create_output_file(
                output_path,
                display_name=exported_container,
                data_type="image",
                extension=exported_container.split(".")[-1],
                source_file_id=input_file.get("id"),
            )

            # Converting ContainerExplorer generated output file to OpenRelik compatible name
            # and location.
            shutil.move(
                os.path.join(container_export_dir, exported_container), output_file.path
            )

            # Fix display_name double extension
            output_file.display_name = exported_container

            container_id = ""
            if ".img" in exported_container:
                container_id = exported_container.split(".img")[0]
            elif ".tar.gz" in exported_container:
                container_id = exported_container.split(".tar.gz")[0]

            logger.info(f"Exporting container {container_id} as {output_file.path}")
            log_entry(
                log_file, f"Exporting container {container_id} as {exported_container}"
            )

            output_files.append(output_file)

        # Clean up container output directory
        shutil.rmtree(container_export_dir)
        logger.debug("Removing container export directory %s", container_export_dir)
    else:
        log_entry(
            log_file,
            f"Error exporting all containers in disk "
            f"{os.path.basename(input_file.get('path', ''))}",
        )

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
    """Export containers as zip files.

    Args:
        pipe_result: Base64-encoded result from the previous Celery task, if any.
        input_files: List of input file dictionaries (unused if pipe_result exists).
        output_path: Path to the output directory.
        workflow_id: ID of the workflow.
        task_config: User configuration for the task.

    Returns:
        Base64-encoded dictionary containing task results.
    """
    task_id = self.request.id
    logger.info(
        "Starting container export task ID: %s, Workflow ID: %s", task_id, workflow_id
    )

    final_output_files = []
    temp_dir = None
    disk_mount_dir = None
    returned_disk_mount_dir = None

    input_files = get_input_files(pipe_result, input_files or [])

    # Log file to capture logs.
    log_file = create_output_file(
        output_path,
        extension="log",
        display_name="container_export",
    )
    final_output_files.append(log_file.to_dict())

    if not input_files:
        message = "No input files provided."
        logger.warning(message)
        log_entry(log_file, message)

        return create_task_result(
            output_files=final_output_files,
            workflow_id=workflow_id,
        )

    container_ids = []
    container_ids_str = task_config.get("container_id", "")
    if container_ids_str:
        for container_id in container_ids_str.split(","):
            if not container_id:
                continue
            container_ids.append(container_id.strip())

    # Process each input file.
    for input_file in input_files:
        logger.info("Processing disk %s", input_file.get("id"))

        temp_dir = os.path.join(output_path, f"tmp{uuid4().hex[:6]}")
        os.makedirs(temp_dir, exist_ok=True)

        if container_ids:
            logger.info("Processing container IDs %s", ",".join(container_ids))
        else:
            logger.info("Processing all containers")

        disk_name = os.path.basename(input_file.get("path"))
        disk_image_path = os.path.join(temp_dir, disk_name)

        try:
            os.link(input_file.get("path"), disk_image_path)
            logger.debug(
                "Created disk link %s to %s", input_file.get("path"), disk_image_path
            )
        except OSError as e:
            message = (
                f"Failed to link input file {input_file.get('path')} to "
                f"{disk_image_path}: {e}"
            )
            logger.error(message)
            log_entry(log_file, message)
            raise Exception(message)

        # Mount disk image to /mnt/{uuid} on worker.
        disk_mount_dir = os.path.join("/mnt", uuid4().hex[:6])
        os.makedirs(disk_mount_dir)
        logger.debug("Created disk mount directory %s", disk_mount_dir)

        returned_disk_mount_dir = mount_disk(disk_image_path, disk_mount_dir)
        if not returned_disk_mount_dir:
            logger.error(
                "Error mounting disk %s to %s", input_file.get("id"), disk_mount_dir
            )
            log_entry(log_file, f"Error mounting disk {input_file.get('id')}")

            # Cleanup and skip
            shutil.rmtree(disk_mount_dir)
            continue

        export_files = []

        if not container_ids:
            all_export_files = export_all_containers(
                input_file, output_path, log_file, returned_disk_mount_dir, task_config
            )

            if all_export_files:
                export_files.extend(all_export_files)

            _log_message = f"Exported {len(all_export_files)} containers files."
            logger.info(_log_message)
            log_entry(log_file, _log_message)
        else:
            # Export specific containers
            for container_id in container_ids:
                container_export_files = export_container(
                    input_file,
                    output_path,
                    log_file,
                    disk_mount_dir,
                    container_id,
                    task_config,
                )

                if container_export_files:
                    export_files.extend(container_export_files)

            _log_message = f"Exported {len(export_files)} containers files."
            logger.info(_log_message)
            log_entry(log_file, _log_message)

        for export_file in export_files:
            final_output_files.append(export_file.to_dict())

        # Unmount disk
        if returned_disk_mount_dir:
            unmount_disk(returned_disk_mount_dir, log_file)
        elif disk_mount_dir and os.path.ismount(disk_mount_dir):
            unmount_disk(disk_mount_dir, log_file)

        if disk_mount_dir and os.path.exists(disk_mount_dir):
            try:
                if not os.path.ismount(disk_mount_dir):
                    shutil.rmtree(disk_mount_dir)
            except OSError as e:
                logger.error(
                    "Error unmounting directory %s: %s", disk_mount_dir, str(e)
                )

        # Clean up
        if temp_dir:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

    return create_task_result(
        output_files=final_output_files,
        workflow_id=workflow_id,
    )
