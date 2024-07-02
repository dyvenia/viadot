# Viadot

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

Depending on the source, `viadot` provides different methods of uploading data. For instance, for SQL sources, this would be bulk inserts. For data lake sources, it would be a file upload.
