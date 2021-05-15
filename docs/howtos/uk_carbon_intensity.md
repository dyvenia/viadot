# Using StatsToCSV and StatsToExcel class

For downloading UK Carbon Intensity Statistics to a csv or excel file, use StatsToCSV or StatsToExcel class.

## Initialize the StatsToCSV and StatsToExcel
```python
# initiates the StatsToCSV and StatsToExcel class with the UK Carbon Intensity statistics
from viadot.tasks.open_apis.uk_carbon_intensity import StatsToExcel, StatsToCSV

statistics_csv = StatsToCSV()
statistics_excel = StatsToExcel()
```
## Generation of csv or excel file
Next, run task with csv or excel file generation.  

```python
statistics_csv.run("out.csv")
statistics_excel.run("out.xlsx")
```

This function require parameter filename.  
Optional, use days_back parameter to download statistics for this time.  
By default it is 10 days, but You can use for example 30, `run(filename, 30)`.

## Running tests

To run tests of StatsToCSV and StatsToExcel class go into tests/prefect directory and run command:
`pytest`
