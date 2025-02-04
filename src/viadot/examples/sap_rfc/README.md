# SAP RFC example

This is an example environment for running the `SAPRFC` connector.

Note that we refer to a `sap_netweaver_rfc` folder in the Dockerfile. This is the folder containing the proprietary SAP NetWeaver driver that would have to be obtained and installed by the user.

## Running SAPRFC

Clone the viadot, enter the sap_rfc folder, and build the image:

```console
git clone --branch main https://github.com/dyvenia/viadot.git && \
cd viadot/viadot/examples/sap_rfc && \
docker build -t viadot:sap_rfc . --no-cache
```

Spin it up with the provided `docker-compose`

```console
docker compose -f docker/docker-compose.yml up -d
```

You can now open up Jupyter Lab at `localhost:5678`.

## Config File

Credentials and other settings are stored as `~/.config/viadot/config.yaml`. A config file needs to be written in yaml format. A typical config file looks like so:

```yaml
version: 1

sources:
  - sharepoint_prod:
      class: Sharepoint
      credentials:
        site: "my_company.sharepoint.com"
        username: "my_user"
        password: "my_pw"
```

## Running tests

To run tests, run pytest:

```console
docker exec -it viadot_saprfc_lab pytest tests/integration/test_sap_rfc.py
```
