# OpenRelik worker for Containers

OpenRelik Containers Worker is responsible for analyzing disk images containing containerd or Docker containers. It receives tasks via Celery to analyze disk images or specific container IDs found within those images.

## Features

- Lists Containers:
  - Discovers Docker and containerd containers located on disk.
  - Uses default storage paths (`/var/lib/docker` or `/var/lib/containerd`) by default.
  - Allows specifying custom container root directories (e.g., `/data/containers/docker/`).

- Show Container Drift:
  - Detects modifications made to running containers.
  - Identifies changes such as configuration alterations, malware additions, or file deletions.

- Export Container Files:
  - Specific Files/Directories: Exports designated files or folders from a container for use by other OpenRelik workers.

- Filesystem Archive Functionality:
  - Exports the complete container filesystem as a `.zip` archive for OpenRelik processing.
  - Processes only the first disk if multiple are provided as input due to unique container IDs.
  - The export timestamp is recorded as the filesystem birth timestamp within the archive.


## Prerequisites

The following software is required to build local image.

- Docker
- Docker Compose
- Git

## Installation

OpenRelik containers worker can be installed by using the pre-build Docker image or building a
local Docker image.

**Note on Privileges:** This worker requires `privileged` mode and `SYS_ADMIN` capabilities to perform necessary mounting operations (e.g., mounting disk images, container layers via FUSE or loop devices). Be aware of the security implications of granting these privileges.


### Using Pre-built Docker Image

Update the `docker-compose.yml` to include `openrelik-worker-containers`.

```yaml
openrelik-worker-containers:
  container_name: openrelik-worker-containers
  image: ghcr.io/openrelik/openrelik-worker-containers:${OPENRELIK_WORKER_CONTAINERS_VERSION}
  platform: linux/amd64
  privileged: true
  cap_add:
    - SYS_ADMIN
  restart: always
  environment:
    - REDIS_URL=redis://openrelik-redis:6379
  volumes:
    - ./data:/usr/share/openrelik/data
  command: "celery --app=src.app worker --task-events --concurrency=2 --loglevel=INFO -Q openrelik-worker-containers"
```

### Building Local Image

1. Clone `openrelik-worker-containers`.

    ```shell
    git clone https://github.com/openrelik/openrelik-worker-containers
    ```

2. Build a Docker container.

    Container Explorer image used in the container is linux/amd64 binary, and the Docker container for
    `openrelik-worker-containers` needs to be `linux/amd64` as well.

    ```shell
    cd openrelik-worker-containers
    docker build --platform linux/amd64 -t openrelik-worker-containers:latest .
    ```

3. Update the `docker-compose.yml` to include `openrelik-worker-containers`.

    ```yaml
    openrelik-worker-containers:
      container_name: openrelik-worker-containers
      image: openrelik-worker-containers:latest
      platform: linux/amd64
      privileged: true
      cap_add:
        - SYS_ADMIN
      restart: always
      environment:
        - REDIS_URL=redis://openrelik-redis:6379
      volumes:
        - ./data:/usr/share/openrelik/data
      command: "celery --app=src.app worker --task-events --concurrency=2 --loglevel=INFO -Q openrelik-worker-containers"
    ```

4. Run `openrelik-worker-containers`.

    ```shell
    docker compose up -d openrelik-worker-containers
    ```

5. Run the following command to review `openrelik-worker-containers` logs.

    ```shell
    docker logs -f openrelik-worker-containers
    ```

    **Note**: Update `docker-compose.yml` to view `openrelik-worker-containers` debug logs.

    ```yaml
    openrelik-worker-containers:
      container_name: openrelik-worker-containers
      image: openrelik-worker-containers:latest
      platform: linux/amd64
      privileged: true
      cap_add:
        - SYS_ADMIN
      restart: always
      environment:
        - REDIS_URL=redis://openrelik-redis:6379
      volumes:
        - ./data:/usr/share/openrelik/data
      command: "celery --app=src.app worker --task-events --concurrency=2 --loglevel=DEBUG -Q openrelik-worker-containers"
    ```