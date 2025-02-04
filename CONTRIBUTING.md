# How to contribute to `viadot`

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

### Installing pre-commit

We use pre-commit hooks to ensure that the code as well as non-code text (such as JSON, YAML, and Markdown) is formatted and linted before committing. First, install `pre-commit`:

```console
rye install pre-commit
```

### Installing viadot's pre-commit hooks

To install `viadot`'s pre-commit hooks, run the following command:

```console
pre-commit install
```

### Working with style

The best way to fix pre-commit errors is to **avoid them** in the first place. In the case of Python checks, the easiest way is to [set Ruff as your formatter in VSode](#code-style). The formatter will automatically format whatever it can, and the linter will highlight the areas that need fixing, so you get the feedback immediately as you're writing the code.

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

Alternatively, the pre-commit hook runs `ruff check`, so you can also wait till you commit to receive the formatting/linting feedback.

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
   # Make sure that your base branch is `main`.
   git switch main && git checkout -b <name>
   ```

5. Add your changes
6. Sync your fork with the `dyvenia` repo

   ```console
   git remote add upstream https://github.com/dyvenia/viadot.git
   git fetch upstream main
   git switch main
   git rebase upstream/main
   ```

7. Push the changes to your fork

   ```console
   git push --force
   ```

8. [Submit a PR](https://github.com/dyvenia/viadot/compare/yourbranch...main) into the `main` branch.

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

Once the modified `pyproject.toml` is merged to `main`, a version tag will be [automatically created](https://github.com/dyvenia/viadot/blob/main/.github/workflows/detect-and-tag-new-version.yml), and the [release workflow](https://github.com/dyvenia/viadot/blob/main/.github/workflows/cd.yml) will be triggered.

The release workflow will:

- create a [release](https://github.com/dyvenia/viadot/releases) on GitHub with auto-generated changelog
- publish [the package](https://pypi.org/project/viadot2/) to PyPI
- publish [Docker images](https://github.com/orgs/dyvenia/packages?repo_name=viadot) to ghcr.io

### Running actions

You can execute actions manually with [GitHub CLI](https://cli.github.com/manual/):

```console
gh workflow run <workflow_name>.yml
```

If you need to pass parameters to the workflow, you can do so with the `--json` flag:

```console
echo '{"name":"scully", "greeting":"hello"}' | gh workflow run workflow.yml --json
```

### Developing & debugging actions

To works on actions, you can use [act](https://github.com/nektos/act).

```console
act -W .github/workflows/detect-and-tag-new-version.yml -s GITHUB_TOKEN="$(gh auth token)"
```

**NOTE** for actions that implicitly `{{ github.token }}`, you need to pass your token as the `GITHUB_TOKEN` act secret, as shown in the example above.
