## Running tasks

To run a task, first initialize it, and then use the `run()` method to execute it:

```python
from viadot.tasks import SupermetricsToDF

task = SupermetricsToDF()  # initialize
task_params = {}  # parameters to be passed to the task
task.run(**task_params)  # run
```

All tasks have inline docstrings, so you can use the standard shortcut `alt + tab` to see the docstring in your IDE (eg. Visual Studio) -- both for
the initialization, as well as the `run()` method (although we recommend to only pass parameters inside the `run()` method and so only check its docstring).