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
df
```

**Output:**
|    | from              | to                |   forecast |   actual | index    |
|---:|:------------------|:------------------|-----------:|---------:|:---------|
|  0 | 2021-08-10T11:00Z | 2021-08-10T11:30Z |        211 |      216 | moderate |

The above `df` is a python pandas `DataFrame` object. The above df contains data downloaded from viadot from the Carbon Intensity UK API.

## Loading Data to a Source
Depending on the source, viadot provides different methods of uploading data. For instance, for SQL sources, this would be bulk inserts. For data lake sources, it would be a file upload. We also provide ready-made pipelines including data validation steps using Great Expectations.

An example of loading data into SQLite from a pandas `DataFrame` using the `SQLiteInsert` Prefect task:

```python
from viadot.tasks import SQLiteInsert

insert_task = SQLiteInsert()
insert_task.run(table_name=TABLE_NAME, dtypes=dtypes, db_path=database_path, df=df, if_exists="replace")
```

## Set up

__Note__: If you're running on Unix, after cloning the repo, you may need to grant executable privileges to the `update.sh` and `run.sh` scripts: 
```
sudo chmod +x viadot/docker/update.sh && \
sudo chmod +x viadot/docker/run.sh
```

### a) user
Clone the `main` branch, enter the `docker` folder, and set up the environment:
```
git clone https://github.com/dyvenia/viadot.git && \
cd viadot/docker && \
./update.sh
```

Run the enviroment:
```
./run.sh
```

### b) developer
Clone the `dev` branch, enter the `docker` folder, and set up the environment:
```
git clone -b dev https://github.com/dyvenia/viadot.git && \
cd viadot/docker && \
./update.sh -t dev
```

Run the enviroment:
```
./run.sh -t dev
```

Install the library in development mode (repeat for the `viadot_jupyter_lab` container if needed):
```
docker exec -it viadot_testing pip install -e . --user
```

## Running tests

To run tests, log into the container and run pytest:
```
docker exec -it viadot_testing bash
pytest
```

## Running flows locally

You can run the example flows from the terminal:
```
docker exec -it viadot_testing bash
FLOW_NAME=hello_world; python -m viadot.examples.$FLOW_NAME
```

However, when developing, the easiest way is to use the provided Jupyter Lab container available in the browser at `http://localhost:9000/`.

## Executing Spark jobs
### Setting up
To begin using Spark, you must first declare the environmental variables as follows:
```
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_API_TOKEN = os.getenv("DATABRICKS_API_TOKEN")
DATABRICKS_ORG_ID = os.getenv("DATABRICKS_ORG_ID")
DATABRICKS_PORT = os.getenv("DATABRICKS_PORT")
DATABRICKS_CLUSTER_ID = os.getenv("DATABRICKS_CLUSTER_ID")
```

Alternatively, you can also create a file called `.databricks-connect` in the root directory of viadot and add the required variables there. It should follow the following format:
```
{
  "host": "",
  "token": "",
  "cluster_id": "",
  "org_id": "",
  "port": ""
}
```
To retrieve the values, follow step 2 in this [link](https://docs.microsoft.com/en-us/azure/databricks/dev-tools/databricks-connect)

### Executing Spark functions
To begin using Spark, you must first create a Spark Session: `spark = SparkSession.builder.appName('session_name').getOrCreate()`. `spark` will be used to access all the Spark methods. Here is a list of commonly used Spark methods (WIP):
* `spark.createDataFrame(df)`: Create a Spark DataFrame from a Pandas DataFrame
* `sparkdf.write.saveAsTable("schema.table")`: Takes a Spark DataFrame and saves it as a table in Databricks.
* Ensure to use the correct schema, as it should be created and specified by the administrator
* `table = spark.sql("select * from schema.table")`: example of a simple query ran through Python


## How to contribute

1. Fork repository if you do not have write access
2. Set up locally
3. Test your changes with `pytest`
4. Submit a PR. The PR should contain the following:
    - new/changed functionality
    - tests for the changes
    - changes added to `CHANGELOG.md`
    - any other relevant resources updated (esp. `viadot/docs`)

The general flow of working for this repository in case of forking:
1. Pull before making any changes
2. Create a new branch with 
```
git checkout -b <name>
```
3. Make some work on repository
4. Stage changes with 
```
git add <files>
```
5. Commit the changes with 
```
git commit -m <message>
``` 
__Note__: See out Style Guidelines for more information about commit messages and PR names

6. Fetch and pull the changes that could happen while working with 
```
git fetch <remote> <branch>
git checkout <remote>/<branch>
```
7. Push your changes on repostory using 
```
git push origin <name>
```
8. Use merge to finish your push to repository 
```
git checkout <where_merging_to>
git merge <branch_to_merge>
```

Please follow the standards and best practices used within the library (eg. when adding tasks, see how other tasks are constructed, etc.). For any questions, please reach out to us here on GitHub.


### Style guidelines
- the code should be formatted with Black using default settings (easiest way is to use the VSCode extension)
- commit messages should:
    - begin with an emoji
    - start with one of the following verbs, capitalized, immediately after the summary emoji: "Added", "Updated", "Removed", "Fixed", "Renamed", and, sporadically, other ones, such as "Upgraded", "Downgraded", or whatever you find relevant for your particular situation
    - contain a useful description of what the commit is doing

## Set up Black for development in VSCode
Your code should be formatted with Black when you want to contribute. To set up Black in Visual Studio Code follow instructions below. 
1. Install `black` in your environment by writing in the terminal:
```
pip install black
```
2. Go to the settings - gear icon in the bottom left corner and select `Settings` or type "Ctrl" + ",".
3. Find the `Format On Save` setting - check the box.
4. Find the `Python Formatting Provider` and select "black" in the drop-down list.
5. Your code should auto format on save now.
