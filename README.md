# Viadot
<p>
<a href="https://github.com/psf/black" target="_blank">
    <img src="https://img.shields.io/badge/code%20style-black-000000.svg" alt="Build">
</a>
</p>

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

For creating SQlite database and uploading table with data (pandas DataFrame as input) use LoadDF class.

```python
from viadot.tasks.sqlite_tasks import LoadDF
table = LoadDF()
table.run(table_name=TABLE_NAME, dtypes=dtypes, db_path=database_path, df=df, if_exists="replace")
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
