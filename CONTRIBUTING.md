# How to contribute to `viadot2`

## Installation & set up

Follow instructions in the [README](./README.md#getting-started) to set up your development environment.

### VSCode

For an enhanced experience, we provide the extensions, settings, and tasks for VSCode in the `.vscode` folder.

1. Install the extensions

   ```console
   cd .vscode && sh install_extensions.sh && cd ..
   ```

2. Open the project in VSCode

   ```console
    code .
   ```

### Docker container

Due to `viadot` depending on a few Linux libraries, the easiest way to work with `viadot` is to use the provided Docker image.

`viadot` comes with a `docker-compose.yml` config with pre-configured containers for the core library, as well as each extra (azure, aws). In order to start a container, run `docker compose up -d viadot-<distro>`, for example:

```bash
docker compose up -d viadot-aws
```

If you need to build a custom image locally, run:

```bash
docker build --target viadot-<distro> --tag viadot-<distro>:<your_tag> -f docker/Dockerfile .
```

Then, make sure to update your docker-compose accordingly, so that it's using your local image.

Once you have a container running, use an IDE like VSCode to attach to it. Alternatively, you can also attach to the container using the CLI:

```bash
docker exec -it viadot-<distro> bash
```

## Running tests

### Environment variables

To run tests, you may need to set up some environment variables or the viadot config. You can find all the required environment variables in the [tests' dotenv file](./tests/.env.example), and all the required viadot config settings in the [config file](./config.yaml.example). We're working on making this process easier, so only one of these can be used.

In order to execute tests locally, run

```console
pytest tests/
```

## Pre-commit hooks

We use pre-commit hooks to ensure that the code (as well as non-code text files, such as JSON, YAML, and Markdown files) is formatted and linted before committing. First, install `pre-commit`:

```console
rye install pre-commit
```

To install `viadot`'s pre-commit hooks, run the following command:

```console
pre-commit install
```

## Style guidelines

- Code should be formatted and linted with [ruff](https://docs.astral.sh/ruff/) using default settings. The easiest way to accomplish this is to use the VSCode extension and the provided VSCode settings. Additionally, the pre-commit hook will take care of this, as well as formatting non-python files.
- Commit messages should:
  - begin with an emoji
  - start with one of the following verbs, capitalized, immediately after the summary emoji: "Add", "Update", "Remove", "Fix", "Rename", and, sporadically, other ones, such as "Upgrade", "Downgrade", or whatever you find relevant for your particular situation
  - contain a useful summary of what the commit is doing
    See [this article](https://www.freecodecamp.org/news/how-to-write-better-git-commit-messages/) to understand basics of naming commits

## Submitting a PR

1. [Fork the repo](https://github.com/dyvenia/viadot/fork)
2. [Install](./README.md#installation) and [configure](./README.md#configuration) `viadot`

   **Note**: In order to run tests, you will also need to install dev dependencies in the `viadot_2` container with `docker exec -u root -it viadot_2 sh -c "pip install -r requirements-dev.txt"`

3. Checkout a new branch

   ```console
   git checkout -b <name>
   ```

   Make sure that your base branch is `2.0`!

4. Add your changes

   **Note**: See out Style Guidelines for more information about commit messages and PR names

5. Test the changes locally

   ```console
   docker exec -it viadot_2 sh -c "pytest"
   ```

6. Sync your fork with the `dyvenia` repo

   ```console
   git remote add upstream https://github.com/dyvenia/viadot.git
   git fetch upstream 2.0
   git checkout 2.0
   git rebase upstream/2.0
   ```

7. Push the changes to your fork

   ```console
   git push --force
   ```

8. [Submit a PR](https://github.com/dyvenia/viadot/compare) into the `2.0` branch.

   Make sure to read & check all relevant checkboxes in the PR template!

## Releasing a new version

### Bump package version

Before creating a release, either add a commit with a version bump to the last PR included in the release, or create a specific release PR. To bump package version, simply run:

```console
rye version major.minor.patch
```

This will update the version in `pyproject.toml` accordingly.

**NOTE**: Make sure to follow [semantic versioning](https://semver.org/).

### Release

Once the new version PR is merged to `main`, publish a version tag:

```bash
viadot_version=v2.1.0
git switch main && \
  git pull && \
  git tag -a $viadot_version -m "Release $viadot_version" && \
  git push origin $viadot_version
```

Pushing the tag will trigger the release workflow, which will:

- create a release on GitHub
- publish the package to PyPI
- publish Docker images to ghcr.io
