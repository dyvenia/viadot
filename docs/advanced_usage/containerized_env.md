# Containerized development environment

Currently, viadot ships with three images:

- `viadot-lite` - includes the core `viadot2` library and system dependencies
- `viadot-azure` - includes `viadot2` with the `azure` extra, as well as Azure-related OS dependencies
- `viadot-aws` - includes `viadot2` with the `aws` extra, as well as AWS-related OS dependencies

You can use these images to avoid installing any OS dependencies on your local machine (for example, `msodbcsql17` and `mssql-tools` used by the `SQLServer` source).

## Setup

Spin up your container of choice with `docker compose`:

```bash
docker compose -f docker/docker-compose.yml up -d viadot-<extra>
```

For example, to start the `viadot-aws` container:

```bash
docker compose -f docker/docker-compose.yml up -d viadot-aws
```

## Usage

### Attaching to the container

Once you have a container running, use an IDE like VSCode to attach to it. Alternatively, you can also attach to the container using the CLI:

```bash
docker exec -it viadot-<distro> bash
```

### Building a custom image locally

If you need to build a custom image locally, you can do so using standard Docker commands. For example:

```bash
docker build --target viadot-<distro> --tag viadot-<distro>:<your_tag> -f docker/Dockerfile .
```

### See also

For more information on working with Docker containers and images, see [Docker documentation](https://docs.docker.com/reference/cli/docker/).
