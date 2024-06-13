## Config File

Credentials and other settings for various **sources** are stored in a file named `config.yaml`. A credential file needs to be written in `YAML` format. A typical credentials file looks like this:

```yaml
sources:

  - exchange_rates:
      class: ExchangeRates
      credentials:
        api_key: "api123api123api123"

  - sharepoint:
      class: Sharepoint
      credentials:
        site: "site.sharepoint.com"
        username: "user@email.com"
        password: "password"
```

In the above, Exchange Rates and Sharepoint are `config_keys`. These config settings are fed to the `ExchangeRates()` or `Sharepoint()` sources.

For example, this is how to use the `ExchangeRates()` configuration stanza from the config file.

```python
# initiates the ExchangeRates() class with the exchange_rates configs
rates = ExchangeRates(config_key="exchange_rates")
```

The above will pass all the configurations, including secrets like passwords, to the class. This avoids having to write secrets or configs in the code.

### Storing the file locally

Currently, only local files are supported. Make sure to store the file at the correct path. 

* On Linux the path is `~/.config/viadot/config.yaml`

## Rye
Rye is a comprehensive project and package management solution for Python. Rye provides a unified experience to install and manage Python installations, `pyproject.toml` based projects, dependencies and virtualenvs seamlessly. It's designed to accommodate complex projects and monorepos and to facilitate global tool installations.

If you are working as a developer in viadot repository, this tool is required to be setup. All instructions about how to install and use rye tool can be found [here](https://rye.astral.sh/).


## Docker containers
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