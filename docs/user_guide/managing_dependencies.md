Viadot 2 introduces a new way of managing dependencies, which is the [`rye`](https://rye.astral.sh/) tool based on the `pyproject.toml` file. We decided to split the dependency into groups. Each group contains dependencies related to a specific cloud service provider. 

## Adding dependencies

!!! note
    Always, for managing dependencies, use rye tool. The tool will automatically upgrade `.lock` files, which are crucial for building Docker images.

### Adding main dependencies

If you are adding dependencies that are going to be used in source and are not cloud-provider-specific, like, for example, [Pandas](https://pandas.pydata.org/docs/user_guide/index.html), you should
add it to the main dependencies. 

To add main dependence, you can use the following command, which will update `pyproject.toml` and `.lock` files.

```
rye add "pandas>=2.2.2"
```

After adding main dependence, we have to run the `rye sync` command to update all `.lock` files.

### Adding group specific dependencies

To add cloud-provider-specific dependencies to your project, you have to add a flag to the `rye add` command, which will add it to a specific group of dependencies. In this example, we're adding a `azure-core==1.30.1` package, so we have to add it to the `viadot-azure` group of dependencies in `pyproject.toml`.

```
rye add "azure-core==1.30.1" --optional viadot-azure
```

After adding optional dependence, to update all related `.lock` files, we have to run the Python script `sync-dependencies.py` with a specified group of dependencies as a parameter. The script is located in the root directory of the repository.

```bash
python3 sync-dependecies.py -dg viadot-azure
```