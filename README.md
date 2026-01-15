# Viadot

[![Rye](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/rye/main/artwork/badge.json)](https://rye.astral.sh)
[![formatting](https://img.shields.io/badge/style-ruff-41B5BE?style=flat)](https://img.shields.io/badge/style-ruff-41B5BE?style=flat)

---

**Documentation**: <a href="https://dyvenia.github.io/viadot/" target="_blank">https://viadot.docs.dyvenia.com</a>

**Source Code**: <a href="https://github.com/dyvenia/viadot/tree/main" target="_blank">https://github.com/dyvenia/viadot/tree/main</a>

---

A simple data ingestion library to guide data flows from some places to other places.

## Getting Data from a Source

Viadot supports several API and RDBMS sources, private and public. Currently, we support the UK Carbon Intensity public API and base the examples on it.

```python
from viadot.sources.uk_carbon_intensity import UKCarbonIntensity

ukci = UKCarbonIntensity()
ukci.query("/intensity")
df = ukci.to_df()

print(df)
```

**Output:**
| | from | to | forecast | actual | index |
| ---: | :---------------- | :---------------- | -------: | -----: | :------- |
| 0 | 2021-08-10T11:00Z | 2021-08-10T11:30Z | 211 | 216 | moderate |

The above `df` is a pandas `DataFrame` object. It contains data downloaded by `viadot` from the Carbon Intensity UK API.

## Loading data to a destination

Depending on the destination, `viadot` provides different methods of uploading data. For instance, for databases, this would be bulk inserts. For data lakes, it would be file uploads.

For example:

```python hl_lines="2 8-9"
from viadot.sources import UKCarbonIntensity
from viadot.sources import AzureDataLake

ukci = UKCarbonIntensity()
ukci.query("/intensity")
df = ukci.to_df()

adls = AzureDataLake(config_key="my_adls_creds")
adls.from_df(df, "my_folder/my_file.parquet")
```

## Getting started

### Prerequisites

We use [Rye](https://rye-up.com/). You can install it like so:

```console
curl -sSf https://rye.astral.sh/get | bash
```

### Installation

```console
pip install viadot2
```

### Running the Docker Environment

The project provides the `run.sh` script located in the `docker/` directory to initialize Docker environment with optional configuration for image tags and target platform.

Make sure the script is executable.

```bash
chmod +x docker/run.sh
```

#### Usage
```bash
docker/run.sh [-t <image_tag>] [-p <platform>]
```

* -t <image_tag> - set Docker image tag (default: latest)
* -p <platfrom> - set the target platform for Docker (default: linux/amd64)
    - This default ensures compatibility on macOS (Apple Silicon). Optionally, you can provide a different platform.


#### Examples

Run with default settings:
```bash
docker/run.sh
```

Run with a specific image tag and platform:
amd64```bash
docker/run.sh -t dev -p linux/amd64
```


### Configuration

In order to start using sources, you must configure them with required credentials. Credentials can be specified either in the viadot config file (by default, `$HOME/.config/viadot/config.yaml`), or passed directly to each source's `credentials` parameter.

You can find specific information about each source's credentials in [the documentation](https://viadot.docs.dyvenia.com/references/sources/sql_sources).

### Next steps

Check out the [documentation](https://viadot.docs.dyvenia.com) for more information on how to use `viadot`.
