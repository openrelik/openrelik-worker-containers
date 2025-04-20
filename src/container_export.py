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

"""Exports container to a tar.gz file."""

import logging
import os
import shutil
import subprocess

from typing import List, Optional
from uuid import uuid4

from openrelik_worker_common.file_utils import create_output_file, OutputFile
from openrelik_worker_common.task_utils import (
    create_task_result,
    get_input_files,
)

from .app import celery
from .utils import (
    log_entry,
    mount_all_containers,
    mount_container,
    mount_disk,
)

# Set up logging
logger = logging.getLogger(__name__)

# Task name used to register and route the task to the correct queue.
TASK_NAME = "openrelik-worker-containers.tasks.container_export"

# Task metadata for registration in the core system.
TASK_METADATA = {
    "display_name": "ContainerExplorer: Container Export",
    "description": "Export container to a tar.gz file.",
    "task_config": [
        {
            "name": "container_id",
            "label": "Container ID to export",
            "description": "Comma separated container IDs to export",
            "type": "Text",
            "required": False,
        },
    ],
}


def _unmount(mount_point: str, log_file: Optional[OutputFile] = None) -> None:
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
            message = (f"Failed to unmount: {mount_point}. Return code: {process.returncode}, "
                       f"Stderr: {process.stderr.strip()}")
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

def export_container(
    input_file: OutputFile,
    output_path: str,
    log_file: OutputFile,
    disk_mount_dir: str,
    container_mount_dir: str,
    container_id: str,
) -> OutputFile | None:
    """Exports container to a .zip file."""
    logger.info("Attempting to export container ID: %s", container_id)

    returned_container_mount_dir = None
    try:
        returned_container_mount_dir = mount_container(
            container_id=container_id,
            disk_mount_dir=disk_mount_dir,
            container_mount_dir=container_mount_dir,
        )

        if not returned_container_mount_dir:
            message = f"Unable to mount container ID: {container_id}"
            logger.error(message)
            log_entry(log_file, message)
            return None
        logger.info(
            "Container %s mounted at %s", container_id, returned_container_mount_dir
        )

        output_file = create_output_file(
            output_path,
            display_name=container_id,
            data_type="container:export:zip",
            extension="zip",
            source_file_id=input_file.get("id"),
        )
        logger.info(
            "Created output file object for %s: %s", container_id, output_file.path
        )

        archive_base = os.path.splitext(output_file.path)[0]
        shutil.make_archive(
            archive_base,
            "zip",
            root_dir=returned_container_mount_dir,
            base_dir=".",
        )
        logger.info(
            "Successfully created archive for container %s at %s.zip",
            container_id,
            archive_base,
        )

        return output_file
    except Exception as e:
        message = f"Error during export of container {container_id}: {e}"
        logger.error(message)
        log_entry(log_file, message)
        return None
    finally:
        if returned_container_mount_dir:
            _unmount(returned_container_mount_dir, log_file)


def export_all_containers(
    input_file: OutputFile, output_path: str, log_file: OutputFile, disk_mount_dir: str
) -> List[OutputFile]:
    """Exports all containers to a .zip files."""
    logger.info(
        "Attempting to export all containers from disk mounted at %s", disk_mount_dir
    )

    # Create a unique temporary directory within `/mnt` for mounting all containers.
    # This avoids potential conflicts if multiple tasks run concurrently.
    all_containers_mount_root = os.path.join("/mnt", f"all_{uuid4().hex[:6]}")
    logger.debug(
        "Creating temporary root mount directory for all containers: %s",
        all_containers_mount_root,
    )
    try:
        os.makedirs(all_containers_mount_root)
    except OSError as e:
        message = f"Failed to create directory {all_containers_mount_root}: {e}"
        logger.error(message)
        log_entry(log_file, message)
        return []

    container_ids = []
    output_files = []

    try:
        container_ids = mount_all_containers(
            path=disk_mount_dir,
            container_mount_dir=all_containers_mount_root,
            log_file=log_file,
        )

        if not container_ids:
            message = "Mounting all containers did not find or mount any containers"
            logger.warning(message)
            log_entry(log_file, "Unable to mount all containers.")
            return []

        logger.info(
            "Found and mounted %d containers under %s",
            len(container_ids),
            all_containers_mount_root,
        )

        for container_id in container_ids:
            logger.info("Processing container ID: %s", container_id)
            container_specific_mount_path = os.path.join(
                all_containers_mount_root, container_id
            )

            if not os.path.isdir(container_specific_mount_path):
                logger.warning(
                    "Expected directory %s not found for container %s, skipping.",
                    container_specific_mount_path,
                    container_id,
                )
                continue

            try:
                output_file = create_output_file(
                    output_path,
                    display_name=container_id,
                    data_type="container:export:zip",
                    extension="zip",
                    source_file_id=input_file.get("id"),
                )
                logger.info(
                    "Created output file object for %s: %s",
                    container_id,
                    output_file.path,
                )

                archive_base = os.path.splitext(output_file.path)[0]
                os.makedirs(os.path.dirname(archive_base), exist_ok=True)

                shutil.make_archive(
                    archive_base,
                    "zip",
                    root_dir=all_containers_mount_root,
                    base_dir=container_id,
                )
                logger.info(
                    "Successfully created archive for container %s at %s.zip",
                    container_id,
                    archive_base,
                )
                output_files.append(output_file)

            except Exception as e:
                message = f"Error during export of container {container_id}: {e}"
                logger.error(message, exc_info=True)
                log_entry(log_file, message)

        logger.info("Finished processing all found containers")

    except Exception as e:
        message = "Error during export of all containers."
        logger.error(message, exc_info=True)
        log_entry(log_file, message)

    finally:
        logger.info("Starting cleanup for export_all_containers")
        if container_ids:
            unmount_all_command = [
                "umount",
                os.path.join(all_containers_mount_root, "*"),
            ]
            logger.info(
                "Attempting to unmount all containers under: %s",
                all_containers_mount_root,
            )
            try:
                process = subprocess.run(
                    unmount_all_command,
                    capture_output=True,
                    check=False,
                    text=True,
                    timeout=120,
                )
                if process.returncode == 0:
                    logger.info(
                        "Successfully unmounted all containers under %s",
                        all_containers_mount_root,
                    )
                else:
                    logger.warning(
                        "Failed to unmount all containers under %s. Stderr: %s",
                        all_containers_mount_root,
                        process.stderr.strip(),
                    )
                    for container_id in container_ids:
                        _unmount(
                            os.path.join(all_containers_mount_root, container_id),
                            log_file,
                        )
            except Exception as e:
                logger.info(
                    "Attempting to unmount each container under %s",
                    all_containers_mount_root,
                )
                for container_id in container_ids:
                    _unmount(
                        os.path.join(all_containers_mount_root, container_id), log_file
                    )

        if os.path.exists(all_containers_mount_root):
            try:
                shutil.rmtree(all_containers_mount_root)
            except OSError as e:
                message = (
                    "Could not remove temporary mount directories under "
                    f"{all_containers_mount_root}: {e}"
                )
                logger.error(message)
                log_entry(log_file, message)

    logger.info("Returning %d output files", len(output_files))
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
    task_id = self.request.id
    logger.info(
        "Starting container export task ID: %s, Workflow ID: %s", task_id, workflow_id
    )

    final_output_files = []
    temp_dir = None
    disk_mount_dir = None
    returned_disk_mount_dir = None

    try:
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

        # We will only be processing one disk as container IDs are unique to each disk.
        input_file = input_files[0]

        temp_dir = os.path.join(output_path, f"temp_{uuid4().hex[:6]}")
        os.makedirs(temp_dir, exist_ok=True)

        container_ids = []
        container_ids_str = task_config.get("container_id", "") if task_config else ""
        if container_ids_str:
            container_ids = [
                cid.strip() for cid in container_ids_str.split(",") if cid.strip()
            ]

        if container_ids:
            logger.info("Processing specific container IDs")
        else:
            logger.info("Processing all containers")

        # Mount disk image to /mnt on worker.
        disk_name = os.path.basename(input_file.get("path"))
        disk_image_path = os.path.join(temp_dir, disk_name)

        try:
            os.link(input_file.get("path"), disk_image_path)
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

        returned_disk_mount_dir = mount_disk(disk_image_path, disk_mount_dir)
        if not returned_disk_mount_dir:
            message = f"Unable to mount {input_file.get('id')} to {disk_mount_dir}"
            logger.error(message)
            log_entry(log_file, message)

            shutil.rmtree(disk_mount_dir)

            return create_task_result(
                output_files=final_output_files,
                workflow_id=workflow_id,
            )

        export_files = []

        if not container_ids:
            export_files = export_all_containers(
                input_file,
                output_path,
                log_file,
                disk_mount_dir=returned_disk_mount_dir,
            )
            if export_files:
                message = f"Successfully exported {len(export_files)} containers."
                logger.info(message)
                log_entry(log_file, message)
            else:
                message = "No containers were exported."
                logger.warning(message)
                log_entry(log_file, message)
        else:
            # Export specific containers
            successful_exports = 0
            for container_id in container_ids:
                container_mount_dir = os.path.join("/mnt", uuid4().hex[:6])
                try:
                    os.makedirs(container_mount_dir)

                    export_file = export_container(
                        input_file,
                        output_path,
                        log_file,
                        disk_mount_dir=returned_disk_mount_dir,
                        container_mount_dir=container_mount_dir,
                        container_id=container_id,
                    )

                    if export_file:
                        export_files.append(export_file)
                        successful_exports += 1
                    else:
                        message = f"Unable to export container {container_id}."
                        logger.warning(message)
                        log_entry(log_file, message)

                except Exception as e:
                    message = (
                        f"Unexpected error exporting container {container_id}: {e}"
                    )
                    logger.error(message)
                finally:
                    if os.path.exists(container_mount_dir):
                        try:
                            if os.path.ismount(container_mount_dir):
                                _unmount(container_mount_dir, log_file)
                            shutil.rmtree(container_mount_dir)
                        except OSError as e:
                            message = (
                                "Could not remove temporary mount directory "
                                f"{container_mount_dir}: {e}"
                            )
                            logger.error(message)

            # final_output_files.append(export_file.to_dict())
            message = (
                "Finished processing requested containers. Successfully exported: "
                f"{successful_exports}/{len(container_ids)}"
            )
            logger.info(message)
            log_entry(log_file, message)

        for export_file in export_files:
            final_output_files.append(export_file.to_dict())

    except Exception as e:
        message = f"Unexpected error during container export: {e}"
        logger.error(message)
    finally:
        if returned_disk_mount_dir:
            _unmount(returned_disk_mount_dir, log_file)
        elif disk_mount_dir and os.path.ismount(disk_mount_dir):
            _unmount(disk_mount_dir, log_file)

        if disk_mount_dir and os.path.exists(disk_mount_dir):
            try:
                if not os.path.ismount(disk_mount_dir):
                    shutil.rmtree(disk_mount_dir)
            except OSError as e:
                message = (
                    f"Could not remove temporary mount directory {disk_mount_dir}: {e}"
                )
                logger.error(message)

        # Clean up
        if temp_dir:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

    return create_task_result(
        output_files=final_output_files,
        workflow_id=workflow_id,
    )
