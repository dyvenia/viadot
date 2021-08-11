# Coping files between different Azure Data Lakes generations

To copy files from 2 different Azure data lakes please use `ADLSGen1ToGen2` class (flow). 
:::viadot.flows.ADLSGen1ToGen2

```python
from viadot.flows import ADLSGen1ToGen2

flow = ADLSGen1ToGen2(
    "Copy file flow example",
    gen1_path="source_file_path",
    gen2_path="destination_file_path",
    local_file_path="Where the gen1 file should be downloaded.",
    overwrite=True,
    gen1_sp_credentials_secret="GEN_1_Secret",
    gen2_sp_credentials_secret="GEN_2_Secret",
    valut_name ="valut_name")

flow.run()
```

