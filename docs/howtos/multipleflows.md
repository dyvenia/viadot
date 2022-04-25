# MultipleFlows


The user can define a list of flows and their projects (list of lists) that are to be launched in the order set by him. Flowys must be registered with the prefect. If one of the subflows fails, the next ones fail and the main flow status will fail.


Example :
```
from viadot.flows.multiple_flows import  MultipleFlows

list = [
        ["Flow of flows 1 test", "dev"],
        ["Flow of flows 2 - working", "dev"],
        ["Flow of flows 3", "dev"],
    ]

flow = MultipleFlows(name="test", flows_list=list)

flow.run()

```


::: viadot.flows.multiple_flows.MultipleFlows