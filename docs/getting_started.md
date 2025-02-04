# Getting started guide

## Prerequisites

We use [Rye](https://rye.astral.sh/). You can install it like so:

```console
curl -sSf https://rye.astral.sh/get | bash
```

## Installation

!!! note

    `viadot2` installation requires installing some Linux libraries, which may be complex for less technical users. For those users, we recommend using the [viadot container](./advanced_usage/containerized_env.md).

### OS dependencies

#### Core

`viadot2` depends on some Linux system libraries. You can install them in the following way:

```console
sudo apt update -q && \
  yes | apt install -q gnupg unixodbc && \
  curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
  curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
  sudo apt update -q && \
  sudo  apt install -q libsqliteodbc && \
  ACCEPT_EULA=Y apt install -q -y msodbcsql17=17.10.1.1-1 && \
  ACCEPT_EULA=Y apt install -q -y mssql-tools=17.10.1.1-1 && \
  echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc
```

Next, copy the SQL Server config from `docker/odbcinst.ini` file into your `/etc/odbcinst.ini` file.

```console
cat docker/odbcinst.ini | sudo tee -a /etc/odbcinst.ini
```

#### SAP connector

In order to work with the SAP connector, you must also install the [SAP NetWeaver RFC SDK](https://support.sap.com/en/product/connectors/nwrfcsdk.html). You must have a SAP license in order to be able to download and use the driver.

To install the driver, copy the `nwrfcsdk` folder to `/usr/local/sap/`. Then, also copy the `nwrfcsdk.conf` file to `/etc/ld.so.conf.d/`:

```console
sudo cp -R ./sap_netweaver_rfc/ /usr/local/sap/
sudo cp ./sap_netweaver_rfc/nwrfcsdk.conf /etc/ld.so.conf.d/
```

Next, configure the `SAPNWRFC_HOME` env variable, eg. by adding the following entry in your `~/.bashrc` file:

```bash
export SAPNWRFC_HOME=/usr/local/sap/nwrfcsdk
```

Make sure to reload the shell:

```console
source ~/.bashrc
```

Next, configure the RFC driver with:

```console
sudo ldconfig
```

Finally, you can install `pyrfc` by installing the viadot `sap` extra:

```console
rye sync --features=sap
```

### Library

```console
git clone https://github.com/dyvenia/viadot.git && \
  cd viadot && \
  rye sync
```

!!! note

    Since `viadot` does not have an SDK, both adding new sources and flows requires **contributing your code to the library**. Hence, we install the library from source instead of just using `pip install`. However, installing `viadot2` with `pip install` is still possible:

    ```console
    pip install viadot2
    ```

    or, with the `azure` [extra](https://github.com/dyvenia/viadot/blob/main/pyproject.toml) as an example:

    ```console
    pip install viadot2[azure]
    ```

    Note that the system dependencies **are not** installed via `pip` and must be installed separately a package manager such as `apt`.

## Next steps

Head over to the [developer guide](./developer_guide/index.md) to learn how to use `viadot` to build data connectors and jobs.
