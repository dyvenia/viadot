# viadot sources, tasks & flows migration workflow

This guide aims to assist the developers in migrating the sources, tasks and flows from viadot 1.0 to their respective new repos/branches.

## 1. Migrating a source from viadot 1.0 to viadot 2.0

The process involves refactoring and modifying the existing sources in viadot 1.0 to work properly on [viadot 2.0](https://github.com/dyvenia/viadot/tree/2.0). This process include but are not limited to the following steps:

### a. Decoupling Prefect from the source

One of the aims of the migration is to completely decouple viadot 2.0 from prefect. Instead of having prefect bundled in with viadot, we have removed the prefect portion of viadot and moved it to another repo called [prefect-viadot](https://github.com/dyvenia/prefect-viadot/).
This includes but not limited to the following actions:

#### 1. Removing `prefect` imports and replacing them if needed

#### 2. Properly replacing any uses of `prefect.signals` with `viadot.signals` or native Python error handling

#### 3. Removing unused/unnecessary imports

#### 4. Replacing the `prefect` logger with the regular Python logger

In short, your aim is to remove the Prefect components completely, while making sure the source works correctly.

### Modify the credentials logic to use pydantic models

Here's a [guide](https://medium.com/mlearning-ai/improve-your-data-models-with-pydantic-f9f10ca66f26) on how to implement pydantic models. Alternatively, you can also take a look at any of the sources already migrated to [viadot 2.0](https://github.com/dyvenia/viadot/tree/2.0/viadot/sources) as a reference.

### Allow the source to take credentials from `config.yml` or `credentials.json` via a `config_key` parameter passed on to the source.

This can be a dictionary of credentials that the source can use.

### b. Migrating current tests

The tests that are still valid and applicable should be migrated from viadot 1.0 to viadot 2.0. This might include modifying the existing tests.

### c. Improving the test coverage

It is possibe that the test coverage of the existing tests is not sufficient. Please ensure that the test coverage is high enough (~80%) and create new tests if needed.

## 2. Migrating the task(s) & flow(s) related to the source from viadot 1.0 to prefect-viadot

After the source is migrated and tested successfully, the next step would be to migrate any related flows to [prefect-viadot](https://github.com/dyvenia/prefect-viadot/). Generally speaking, the steps involved with this phase are very similar to the ones in the source migration section, so you can follow the same steps listed there. There might be some cases where some steps are not applicable (ex. some flows don't take any credentials). In these cases, you may skip whatever is not applicable.

## 3. Creating PRs

### a. Creating a PR to viadot 2.0 for the migrated sources

Please create a PR on the [viadot repo](https://github.com/dyvenia/viadot), where the PR should merge into the `2.0` branch.

### b. Creating a PR to prefect-viadot for the migrated tasks & flows

Please create a PR on the [prefect-viadot repo](https://github.com/dyvenia/prefect-viadot), where the PR should merge into the `main` branch.
