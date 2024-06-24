# How to contribute to `viadot`

## Setting up the environment

Follow the instructions in the [README](./README.md) to set up your development environment.

### Rye

Rye is a comprehensive project and package management solution for Python. Rye provides a unified experience to install and manage Python installations, `pyproject.toml` based projects, dependencies and virtualenvs seamlessly. It's designed to accommodate complex projects and monorepos and to facilitate global tool installations.

If you are working as a developer in viadot repository, this tool is required to be setup. All instructions about how to install and use rye tool can be found [here](https://rye.astral.sh/).


### VSCode

We provide the extensions, settings, and tasks for VSCode in the `.vscode` folder.

1. Install the extensions

   ```console
   cd .vscode && sh install_extensions.sh && cd ..
   ```

2. Open the project in VSCode

   ```console
    code .
   ```

### Development Docker container

#### Bulding of containers

To build all available containers, run the following command:

**NOTE**: All the following commands must be execured from within the `viadot/docker/` directory. 

```bash
docker compose up -d 
```
If you want to build a specific one, add its name at the end of the command:

```bash
docker compose up -d viadot-azure
```

#### Building docker images

All necessary Docker images are released in `ghcr.io` and are included in the `docker-compose.yml` file, but if you want to create your own custom Docker image, follow the following instructions.

In the repository, we have three possible images to build:

- `viadot-lite`
- `viadot-azure`
- `viadot-aws`

To build an image, you have to be in the root directory of the repository and run the following command with selected target:

```bash
docker build --target viadot-azure -t <name of your image>:<version of your image> -f docker/Dockerfile .
```


#### Start of work inside the container 

```bash
docker exec -it viadot-azure bash
```

### Environment variables

To run tests, you may need to set up some environment variables or the viadot config. You can find all the required environment variables in the [tests' dotenv file](./tests/.env.example), and all the required viadot config settings in the [config file](./config.yaml.example). We're working on making this process easier, so only one of these can be used.

### Pre-commit hooks

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

In order to release a new version, either add a commit with a version bump to the last PR, or create a specific release PR. To bump the package version, simply run:

```console
rye version x.y.z
```

Make sure to follow [semantic versioning](https://semver.org/).

The merge to `2.0` automatically publishes the `viadot:2.0-latest` image.

If required, you can manually [deploy the package to PyPI](https://github.com/dyvenia/viadot/actions/workflows/publish_to_pypi.yml) or [publish the image with another tag](https://github.com/dyvenia/viadot/actions/workflows/docker-publish.yml) (such as a version tag).
