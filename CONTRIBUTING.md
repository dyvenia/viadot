# How to contribute

## Setting up the environment
### VSCode
We provide the extensions, settings, and tasks for VSCode in the `.vscode` folder.
1. Install the extensions
    ```sh
    cd .vscode && sh install_extensions.sh && cd ..
    ```
2. Open the project in VSCode
   ```sh
    code .
   ```
3. Open terminals  
In VSCode, run `Ctrl+Shift+B` to open two terminal windows: a local `bash` one and a `viadot_2` container one.

### Environment variables
To run tests, you may need to set up some environment variables or the viadot config. You can find all the required environment variables in the [tests' dotenv file](./tests/.env.example), and all the required viadot config settings in the [config file](./config.yaml.example). We're working on making this process easier, so only one of these can be used.


## Style guidelines
- code should be formatted with `black` using default settings (easiest way is to use the VSCode extension)
- imports should be sorted using `isort`
- commit messages should:
    - begin with an emoji
    - start with one of the following verbs, capitalized, immediately after the summary emoji: "Add", "Update", "Remove", "Fix", "Rename", and, sporadically, other ones, such as "Upgrade", "Downgrade", or whatever you find relevant for your particular situation
    - contain a useful summary of what the commit is doing
    See [this article](https://www.freecodecamp.org/news/how-to-write-better-git-commit-messages/) to understand basics of naming commits


## Submitting a PR
1. [Fork the repo](https://github.com/dyvenia/viadot/fork)
2. [Install](./README.md#installation) and [configure](./README.md#configuration) `viadot`  
    __Note__: In order to run tests, you will also need to install dev dependencies in the `viadot_2` container with `pip install requirements-dev.txt`
3. Checkout a new branch
    ```sh
    git checkout -b <name>
    ```
    Make sure that your base branch is `2.0`!
4. Add your changes  
    __Note__: See out Style Guidelines for more information about commit messages and PR names
5. Test the changes locally  
   ```sh
   docker exec -it viadot_2 sh -c "pytest"
   ```
6. Sync your fork with the `dyvenia` repo
    ```sh
    git remote add upstream https://github.com/dyvenia/viadot.git
    git fetch upstream 2.0
    git checkout 2.0
    git rebase upstream/2.0
    ```
7. Push the changes to your fork
    ```sh
    git push --force
    ```
8. [Submit a PR](https://github.com/dyvenia/viadot/compare) into the `2.0` branch.
Make sure to read & check all relevant checkboxes in the PR template!