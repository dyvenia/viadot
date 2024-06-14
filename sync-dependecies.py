import argparse
import os
import subprocess

AVAILABLE_DEPENDENCIES_GROUP = ["viadot-azure", "viadot-aws", "viadot-lite"]


def sync_specified_file(dependecies_group: str) -> None:
    """Sync dependencies specified by `dependencies_group` using `rye`.

    The function has to do some workaround operations until `rye` provides
    the possibility to have a few `.lock` files and update the specified one.
    There is a feature request for this. https://github.com/astral-sh/rye/issues/1138

    Args:
        dependencies_group (str): Group of dependencies to sync.
    """
    os.remove("requirements.lock")
    if dependecies_group == "viadot-lite":
        command = ["rye", "sync"]
    else:
        command = ["rye", "sync", "--features", dependecies_group]

    # Generates new requirements.lock file with only the specified group installed.
    subprocess.run(command)

    # Open and read the content of the source file.
    with open("requirements.lock", "r") as f:
        content = f.read()

    # Open the destination file in write mode and overwrite with source content.
    with open(f"requirements-{dependecies_group}.lock", "w") as f:
        f.write(content)

    os.remove("requirements.lock")
    command_after_update = ["rye", "sync", "--all-features"]

    # Generates new requirements.lock file with all groups installed.
    subprocess.run(command_after_update)


def get_dependecies_group() -> str:
    """Returns the dependencies group specified via command-line arguments.

    Returns:
        str: The dependencies group specified via command-line.
    """
    parser = argparse.ArgumentParser(description="Lock files syncing script")
    parser.add_argument(
        "-dg",
        "--dependecies-group",
        help="Gorup of dependencies to sync",
        required=True,
    )
    args = vars(parser.parse_args())
    dependecies_group = args["dependecies_group"]
    return dependecies_group


def main(dependecies_group: str):
    """Syncs files for a specified dependencies group.

    Args:
        dependencies_group (str): Group of dependencies to synchronize.

    Raises:
        ValueError: If dependencies_group is not in AVAILABLE_DEPENDENCIES_GROUP.
    """
    if dependecies_group in AVAILABLE_DEPENDENCIES_GROUP:
        sync_specified_file(dependecies_group=dependecies_group)
    else:
        raise ValueError(
            f"{dependecies_group} is not a valid dependency group. Please choose one of the following: {AVAILABLE_DEPENDENCIES_GROUP}"
        )


if __name__ == "__main__":
    dependecies_group = get_dependecies_group()
    main(dependecies_group=dependecies_group)
