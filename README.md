# Viadot

[![build status](https://github.com/dyvenia/viadot/actions/workflows/build.yml/badge.svg)](https://github.com/dyvenia/viadot/actions/workflows/build.yml)
[![formatting](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![codecov](https://codecov.io/gh/Trymzet/dyvenia/branch/main/graph/badge.svg?token=k40ALkXbNq)](https://codecov.io/gh/Trymzet/dyvenia)

---

**Documentation**: <a href="https://dyvenia.github.io/viadot/" target="_blank">https://dyvenia.github.io/viadot/</a>

**Source Code**: <a href="https://github.com/dyvenia/viadot" target="_blank">https://github.com/dyvenia/viadot</a>

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
curl -sSf https://rye-up.com/get | bash
```

### Installation

```console
git clone https://github.com/dyvenia/viadot.git -b 2.0 && \
  cd viadot && \
  rye sync
```

### Configuration

In order to start using sources, you must configure them with required credentials. Credentials can be specified either in the viadot config file (by default, `$HOME/.config/viadot/config.yaml`), or passed directly to each source's `credentials` parameter.

You can find specific information about each source's credentials in [the documentation](https://dyvenia.github.io/viadot/references/sql_sources/).
