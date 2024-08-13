# Containerized development environment

Currently, viadot ships with three images:

- `viadot-lite` - includes the core `viadot2` library and system dependencies
- `viadot-azure` - includes `viadot2` with the `azure` extra, as well as Azure-related OS dependencies
- `viadot-aws` - includes `viadot2` with the `aws` extra, as well as AWS-related OS dependencies

You can use these images to avoid installing any OS dependencies on your local machine (for example, `msodbcsql17` and `mssql-tools` used by the `SQLServer` source).

## Setup

Spin up your container of choice with `docker compose`:

```bash
docker compose up -d viadot-<extra>
```

For example, to start the `viadot-aws` container:

```bash
docker compose up -d viadot-aws
```

## Usage

TODO - describe attaching with VSCode, updating images, etc.
