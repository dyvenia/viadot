# Viadot

[![build status](https://github.com/dyvenia/viadot/actions/workflows/build.yml/badge.svg)](https://github.com/dyvenia/viadot/actions/workflows/build.yml)
[![formatting](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![codecov](https://codecov.io/gh/Trymzet/dyvenia/branch/main/graph/badge.svg?token=k40ALkXbNq)](https://codecov.io/gh/Trymzet/dyvenia)
[![Rye](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/rye/main/artwork/badge.json)](https://rye.astral.sh)


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

|     | from              | to                | forecast | actual | index    |
| --: | ----------------- | :---------------- | -------: | -----: | :------- |
|   0 | 2021-08-10T11:00Z | 2021-08-10T11:30Z |      211 |    216 | moderate |

The above `df` is a pandas `DataFrame` object. It contains data downloaded by `viadot` from the Carbon Intensity UK API.

## Loading Data to a Source

Depending on the source, `viadot` provides different methods of uploading data. For instance, for SQL sources, this would be bulk inserts. For data lake sources, it would be a file upload. For ready-made pipelines including data validation steps using `dbt`, see [prefect-viadot](https://github.com/dyvenia/prefect-viadot).
