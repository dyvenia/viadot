# Viadot
[![build status](https://github.com/dyvenia/viadot/actions/workflows/build/badge.svg)](https://github.com/dyvenia/viadot/actions/workflows/build)
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
ukci.to_df()
```

The above code pulls data from the API to a pandas `DataFrame`.

## Loading Data to a Source

For creating SQlite database and uploading table with data (pandas DataFrame as input) use Insert class.

```python
from viadot.tasks.sqlite_tasks import Insert
insert = Insert()
insert.run(table_name=TABLE_NAME, dtypes=dtypes, db_path=database_path, df=df, if_exists="replace")
```


## Running tests
```
run.sh
docker exec -it viadot_testing bash
cd tests/ && pytest .
```

## Running flows locally
```
run.sh
FLOW_NAME=supermetrics_to_azure_sql; python -m viadot.flows.$FLOW_NAME
```
