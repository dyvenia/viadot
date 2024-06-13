Using docker containers during development is not longer required. If you already installed [Rye](https://rye.astral.sh/) all of dependecies should be installed. If you still want to use containers you have to build it using `docker-compose`.

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

### Start of work inside the container 

```bash

docker exec -it viadot-azure bash

```