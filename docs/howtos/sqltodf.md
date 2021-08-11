## Loading Data to DF

For getting pandas DataFrame from sql query use SQLtoDF class.  
During connection to an SQLite database, SQLite automatically creates
the new database in directory specified by path parameter.  
To return a query, use the sql file, passing its path to the class constructor.  
Using of run function on SQLtoDF class instance return DataFrame object.


```python
from viadot.tasks.sqlite_tasks import SQLtoDF
sql = SQLtoDF(db_path=database_path, sql_path=sql_path)
df_from_task = sql.run()
```
