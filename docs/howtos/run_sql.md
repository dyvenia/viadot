## Loading Data to a Source

For getting pandas DataFrame from sql query use SQLtoDF class.
Get path to database and sql file as an arguments.

```python
from viadot.tasks.sqlite_tasks import SQLtoDF
sql = SQLtoDF(db_path=database_path, sql_path=sql_path)
df_from_task = sql.run()
```
