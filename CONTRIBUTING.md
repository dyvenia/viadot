# How to contribute to `viadot2`

## Installation & set up

Follow instructions in the [documentation](./docs/getting_started.md) to set up your development environment.

### VSCode

For an enhanced experience, we provide the extensions, settings, and tasks for VSCode in the `.vscode` folder.

1. Install recommended extensions

   ```console
   bash .vscode/install_extensions.sh
   ```

2. Open the project in VSCode

   ```console
    code .
   ```

## Pre-commit hooks

We use pre-commit hooks to ensure that the code as well as non-code text (such as JSON, YAML, and Markdown) is formatted and linted before committing. First, install `pre-commit`:

```console
rye install pre-commit
```

To install `viadot`'s pre-commit hooks, run the following command:

```console
pre-commit install
```

## Running tests

### Unit tests

To run unit tests, simply execute:

```console
pytest tests/unit
```

### Integration tests

For integration tests, you need to set up some environment variables and viadot config.

**NOTE**: Configs used internally within dyvenia are stored in our Passbolt instance.

#### Environment variables

You can find all used environment variables in the [tests' dotenv file](./tests/.env.example). The env file must be located at `tests/.env`.

#### Config file

You can find all used viadot config entries in the [example config file](./config.yaml.example).

## Style guidelines

### Code style

Code should be formatted and linted with [ruff](https://docs.astral.sh/ruff/). All settings are defined in the [pyproject file](pyproject.toml).

The easiest way to format your code as you go is to use the VSCode extension and the provided VSCode settings - your code will be automatically formatted and linted on each save, and the linter will highlight areas in your code which need fixing.

Additionally, the pre-commit hook runs `ruff check`, so you can also wait till you commit to receive the formatting/linting feedback.

### Commit messages style

Commit messages should:

- begin with an emoji (we recommend using the [gitmoji](https://marketplace.visualstudio.com/items?itemName=seatonjiang.gitmoji-vscode) VSCode extension (it's already included in viadot's `.vscode/extensions.list`))
- start with one of the following verbs, capitalized, immediately after the summary emoji: "Add", "Update", "Remove", "Fix", "Rename", and, sporadically, other ones, such as "Upgrade", "Downgrade", or whatever you find relevant for your particular situation
- contain a useful summary of what the commit is doing

  See [this article](https://www.freecodecamp.org/news/how-to-write-better-git-commit-messages/) to understand the basics of commit naming.

## Submitting a PR

1. [Fork the repo](https://github.com/dyvenia/viadot/fork)
2. Uncheck the "Copy the `main` branch only" box
3. Follow the setup outlined above
4. Checkout a new branch

   ```console
   # Make sure that your base branch is `2.0`.
   git switch 2.0 && git checkout -b <name>
   ```

5. Add your changes
6. Sync your fork with the `dyvenia` repo

   ```console
   git remote add upstream https://github.com/dyvenia/viadot.git
   git fetch upstream 2.0
   git switch 2.0
   git rebase upstream/2.0
   ```

7. Push the changes to your fork

   ```console
   git push --force
   ```

8. [Submit a PR](https://github.com/dyvenia/viadot/compare/2.0...main) into the `2.0` branch.

   Make sure to read & check all relevant checkboxes in the PR description!

## Maintainers-only

### Releasing a new version

#### Bump package version

Before creating a release, either add a commit with a version bump to the last PR included in the release, or create a specific release PR. To bump package version, simply run:

```console
rye version major.minor.patch
```

for example:

```console
rye version 2.1.0
```

This will update the version in `pyproject.toml` accordingly.

**NOTE**: Make sure to follow [semantic versioning](https://semver.org/).

#### Release

Once the new version PR is merged to `2.0`, publish a version tag:

```bash
viadot_version=v2.1.0
git switch 2.0 && \
  git pull && \
  git tag -a $viadot_version -m "Release $viadot_version" && \
  git push origin $viadot_version
```

Pushing the tag will trigger the release workflow, which will:

- create a release on GitHub
- publish the package to PyPI
- publish Docker images to ghcr.io

### Running actions

You can execute actions manually with [GitHub CLI](https://cli.github.com/manual/):

```console
gh workflow run <workflow_name>.yml
```

If you need to pass parameters to the workflow, you can do so with the `--json` flag:

```console
echo '{"name":"scully", "greeting":"hello"}' | gh workflow run workflow.yml --json
```
