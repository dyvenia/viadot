Using docker containers during development is not longer required. If you already installed [Rye](https://rye.astral.sh/) all of dependecies should be installed.
If you still want to use containers you have to build it using `docker-compose`.

Currently there are tree available containers to build:

- `viadot-lite` - It has installed default dependencies and supports only non-cloud-specific sources.
- `viadot-azure` - It has installed default and viadot-azure dependencies. Supports Azure-based sources and non-cloud-specific ones.
- `viadot-aws` - It has installed default and aws-azure dependencies. Supports AWS-based sources and non-cloud-specific ones.

### Bulding of containers

All the following commands must be running in `viadot/docker/` path in repository.
To build all available containers, run the following command:

```bash
docker compose up -d 
```
If you want to build a specific one, add its name at the end of the command:

```bash
docker compose up -d viadot-azure
```

### Building docker images

All necessary Docker images are released in `ghcr.io` and are included in the `docker-compose.yml` file, but if you want to create your own custom Docker image, follow the following instructions.
In the repository, we have three Dockerfiles:

- `viadot-lite.Dockerfile`
- `viadot-azure.Dockerfile`
- `viadot-aws.Dockerfile`

To build an image, you have to be in root directory of the repository and run the following command with selected Dockerfile:

```bash
docker build -t <name of your image>:<version of your image> -f docker/<selected Dockerfile> .
```


### Start of work inside the container 

```bash
docker exec -it viadot-azure bash
```