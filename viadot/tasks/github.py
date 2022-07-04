# Adapted from Prefect 1.0's utils/git.py

from dulwich.index import build_index_from_tree
from dulwich.porcelain import Repo, clone
from prefect import task
from prefect.logging import get_run_logger


def branch_or_tag_ref(branch_name: str = None, tag: str = None) -> bytes:
    """
    Get the branch or tag ref for the current repo.
    """
    if branch_name is not None:
        return f"refs/remotes/origin/{branch_name}".encode("utf-8")
    elif tag is not None:
        return f"refs/tags/{tag}".encode("utf-8")
    raise ValueError(
        "Either `tag` or `branch_name` must be specified to get a tree id."
    )


def get_tree_id_for_branch_or_tag(
    repo: Repo,
    ref: bytes = None,
    commit: str = None,
) -> str:
    """
    Gets the tree id for relevant branch or tag.
    """
    if commit is not None:
        return repo[commit.encode("utf-8")]
    return repo[repo.get_refs()[ref]].tree


def checkout_ref(repo: Repo, ref: bytes = None, commit: str = None) -> None:
    """
    Checkout a specific ref from the repo.
    """

    build_index_from_tree(
        repo.path,
        repo.index_path(),
        repo.object_store,
        get_tree_id_for_branch_or_tag(repo, ref=ref, commit=commit),
    )


@task
def github_clone_task(
    git_clone_url: str,
    branch_name: str = None,
    tag: str = None,
    commit: str = None,
    clone_depth: int = 1,
    max_retries: int = 3,
):
    logger = get_run_logger()

    repo = clone(source=git_clone_url, depth=clone_depth)
    repo_name = git_clone_url.split("/")[-1]
    logger.info(f"Successfully cloned the '{repo_name}' repository.")

    if branch_name is not None or tag is not None:
        ref = branch_or_tag_ref(branch_name=branch_name, tag=tag)
        checkout_ref(repo, ref=ref, commit=commit)
    return True
