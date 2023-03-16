## Config File

Credentials and other settings for various **sources** are stored in a file named `config.yaml`. A credential file needs to be written in `YAML` format. A typical credentials file looks like so:

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

In the above Exchange Rates and Sharepoint are config_keys. These config settings are fed to the `ExchangeRates()` or `Sharepoint()` Sources.

For example, this is how to use the `ExchangeRates()` configuration stanza from the config file.

```python
# initiates the ExchangeRates() class with the exchange_rates configs
rates = ExchangeRates(config_key="exchange_rates")
```

The above will pass all the configurations, including secrets like passwords, to the class. This avoids having to write secrets or configs in the code.

### Storing the file locally

Currently only local files are supported. Make sure to store the file in the correct path. 

* On Linux the path is `/home/viadot/.config/viadot/config.yaml`

## Docker container

A necessary step to take before working with the viadot library is to build a Docker container. We assume that you have [Docker](https://www.docker.com/) installed. From the root directory of the repository, run the following command, which will build the Docker image.

```bash
docker build --file docker/Dockerfile .
```

Once the images have been created, the container needs to be built. To do this, run the following command in `/docker` path. 

```bash

docker compose up -d 

```

## Start of work inside the container 

```bash

docker exec -it viadot bash

```
