# Viadot
[![build status](https://github.com/dyvenia/viadot/actions/workflows/build.yml/badge.svg)](https://github.com/dyvenia/viadot/actions/workflows/build.yml)
[![formatting](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![codecov](https://codecov.io/gh/Trymzet/dyvenia/branch/main/graph/badge.svg?token=k40ALkXbNq)](https://codecov.io/gh/Trymzet/dyvenia)
---

**Documentation**: <a href="https://dyvenia.github.io/viadot/tree/2.0" target="_blank">https://dyvenia.github.io/viadot/tree/2.0</a>

**Source Code**: <a href="https://github.com/dyvenia/viadot/tree/2.0" target="_blank">https://github.com/dyvenia/viadot/tree/2.0</a>

---

A simple data ingestion library to guide data flows from some places to other places.

## Getting Data from a Source

Viadot supports several API and RDBMS sources, private and public. An example of the use of the UK Carbon Intensity API.

```python
from viadot.sources.uk_carbon_intensity import UKCarbonIntensity

ukci = UKCarbonIntensity()
ukci.query("/intensity")
df = ukci.to_df()

print(df)
```

**Output:**

|      | from              | to                | forecast | actual | index    |
| ---: | :---------------- | :---------------- | -------: | -----: | :------- |
|    0 | 2021-08-10T11:00Z | 2021-08-10T11:30Z |      211 |    216 | moderate |

The above `df` is a pandas `DataFrame` object. It contains data downloaded by `viadot` from the Carbon Intensity UK API.

## Loading Data to a Source
Depending on the source, `viadot` provides different methods of uploading data. For instance, for SQL sources, this would be bulk inserts. For data lake sources, it would be a file upload. For ready-made pipelines including data validation steps using `dbt`, see [prefect-viadot](https://github.com/dyvenia/prefect-viadot).


## Getting started
### Prerequisites
We assume that you have [Docker](https://www.docker.com/) installed.

### Installation
Clone the `2.0` branch, and set up and run the environment:
  ```sh
  git clone https://github.com/dyvenia/viadot.git -b 2.0 && \
    cd viadot/docker && \
    sh update.sh  && \
    sh run.sh && \
    cd ../
  ```

### Configuration
In order to start using sources, you must configure them with required credentials. Credentials can be specified either in the viadot config file (by default, `$HOME/.config/viadot/config.yaml`), or passed directly to each source's `credentials` parameter.

You can find specific information about each source's credentials in [the documentation](https://dyvenia.github.io/viadot/references/sql_sources/).