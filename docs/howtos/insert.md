## Loading Data to a Source

For creating SQlite database and uploading table with data use Insert class.  
Using of run function on Insert class instance create database in directory specified by path parameter and will complete sql table by pandas DataFrame.

```python
from viadot.tasks.sqlite_tasks import Insert
insert = Insert()
insert.run(table_name=TABLE_NAME, dtypes=dtypes, db_path=database_path, df=df, if_exists="replace")
```
