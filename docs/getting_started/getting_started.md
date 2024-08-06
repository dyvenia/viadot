# Getting started guide

## Prerequisites

We use [Rye](https://rye-up.com/). You can install it like so:

```console
curl -sSf https://rye-up.com/get | bash
```

## Installation

```console
git clone https://github.com/dyvenia/viadot.git -b 2.0 && \
  cd viadot && \
  rye sync
```

!!! note

    Since `viadot` does not have an SDK, both adding new sources and flows requires **contributing your code to the library**. Hence, we install the library from source instead of just using `pip install`. However, installing `viadot2` with `pip install` is still possible:

    ```console
    pip install viadot2
    ```

    or, with the `azure` [extra](https://github.com/dyvenia/viadot/blob/2.0/pyproject.toml) as an example:

    ```console
    pip install viadot2[azure]
    ```

## Next steps

Head over to the [developer guide](../developer_guide/index.md) to learn how to use `viadot` to build data connectors and jobs.
