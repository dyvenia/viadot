# Coping files within the same Azure Data Lake

To copy file within Azure Data Lake please use `ADLSContainerToContainer` class
:::viadot.flows.ADLSContainerToContainer 

```python
from viadot.flows import ADLSContainerToContainer

flow = ADLSContainerToContainer(
    "Copy file flow example",
    from_path="source_file_path",
    to_path="destination_file",
    adls_sp_credentials_secret="ADLS_secret",
    valut_name ="valut_name"))

flow.run()
```
