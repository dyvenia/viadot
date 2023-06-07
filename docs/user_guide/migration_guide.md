# viadot sources, tasks & flows migration workflow
This guide aims to assist the developers in migrating the sources, tasks and flows from viadot 1.0 to their respective new repos/branches.

## Migrating a source from viadot 1.0 to viadot 2.0
The process involves refactoring and modifying the existing sources in viadot 1.0 to work properly on [viadot 2.0](https://github.com/dyvenia/viadot/tree/2.0). This process include but are not limited to the following steps:
### Decoupling Prefect from the source
One of the aims of the migration is to completely decouple viadot 2.0 from prefect.
This includes to the following actions:

- Removing `prefect` imports and replacing them if needed

- Properly replacing any uses of `prefect.signals` with `viadot.signals` or native Python error handling

- Removing unused/unnecessary imports

- Replacing the `prefect` logger with the regular Python logger

### Modify the credentials logic to use pydantic models
Here's a [guide](https://medium.com/mlearning-ai/improve-your-data-models-with-pydantic-f9f10ca66f26) on how to implement pydantic models. Alternatively, you can also take a look at any of the sources already migrated to [viadot 2.0](https://github.com/dyvenia/viadot/tree/2.0/viadot/sources) as a reference.

Allow the source to take credentials from `config.yml` or `credentials.json` via a `config_key` parameter passed on to the source.
This can be a dictionary of credentials that the source can use.
### Migrating current tests
The tests that are still valid and applicable should be migrated from viadot 1.0 to viadot 2.0. This might include modifying the existing tests.
### Improving the test coverage
It is possibe that the test coverage of the existing tests is not sufficient. Please ensure that the test coverage is high enough (~80%) and create new tests if needed.
## Creating PRs
### Creating a PR to viadot 2.0 for the migrated sources
Please create a PR on the [viadot repo](https://github.com/dyvenia/viadot), where the PR should merge into the `2.0` branch.
