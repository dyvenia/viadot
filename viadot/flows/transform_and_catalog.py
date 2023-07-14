import os
import shutil
from typing import Dict, List

from prefect import Flow, task

from viadot.tasks import CloneRepo, DBTTask, AzureKeyVaultSecret


@task
def _cleanup_repo(dbt_repo_dir_name: str) -> None:
    """
    Remove a repo folder.

    Args:
        dbt_repo_dir_name (str): The name of the temporary folder.
    """
    shutil.rmtree(dbt_repo_dir_name, ignore_errors=True)  # Delete folder on run


@task
def upload_metadata_draft(recipe_path: str = None) -> None:
    """Upload metadata do choosen destination (placeholder)."""
    return recipe_path


class TransformAndCatalog(Flow):
    """Build specified dbt model(s) and upload the generated metadata to DataHub or other destination."""

    def __init__(
        self,
        dbt_project_path: str,
        name: str = "Transform and Catalog",
        dbt_repo_url: str = None,
        dbt_repo_url_secret: str = None,
        dbt_repo_branch: str = None,
        token: str = None,
        token_secret: str = None,
        local_dbt_repo_path: str = None,
        dbt_selects: Dict[str, str] = None,
        dbt_target: str = None,
        stateful: bool = False,
        *args,
        **kwargs,
    ) -> List[str]:
        """
        Build specified dbt model(s) and upload the generated metadata to DataHub or other destination.

        Args:
            dbt_project_path (str): The path to the dbt project (the directory containing
                the `dbt_project.yml` file).
            name (str): The name of the Flow. Defaults to "Transform and Catalog".
            dbt_repo_url (str, optional): The URL for cloning the dbt repo with relevant dbt project. Defaults to None.
            dbt_repo_url_secret (str, optional): Alternatively to above, the secret containing `dbt_repo_url`.
                Defaults to None.
            dbt_repo_branch (str, optional): The branch of the dbt repo to use. Defaults to None.
            token (str, optional): The personal access token. Defaults to None.
            token_secret (str, optional): The name of the secret storing the token. Defaults to None.
            local_dbt_repo_path (str, optional): The path where to clone the repo to. Defaults to None.
            dbt_selects (dict, optional): Valid [dbt node selection](https://docs.getdbt.com/reference/node-selection/syntax)
                expressions. Valid keys are `run`, `test`, and `source_freshness`. The testselect expression is taken
                from run's, as long as run select is provided. Defaults to None.
            dbt_target (str): The dbt target to use. If not specified, the default dbt target (as specified in `profiles.yaml`)
                will be used. Defaults to None.
            stateful (bool, optional): Whether only the models should be rebuilt only if modified.
                See [dbt docs](https://docs.getdbt.com/guides/legacy/understanding-state). Defaults to False.

        Returns:
            List[str]: Lines from stdout of the `upload_metadata` task as a list.

        Examples:
            # Build a single model
            ```python
            import os
            from viadot.flows import TransformAndCatalog

            my_dbt_project_path = os.path.expanduser("~/dbt/my_dbt_project")
            my_datahub_recipe_path = os.path.expanduser("~/dbt/catalog/recipe.yaml")
            my_dbt_repo_url = "[repo_url]"

            flow = TransformAndCatalog(
                name="DBT flow",
                dbt_project_path=my_dbt_project_path,
                # datahub_recipe_path=my_datahub_recipe_path,
                dbt_repo_url=my_dbt_repo_url,
                dbt_selects={"run": "my_model"}
            )
            flow.run()
            ```

            Some common `dbt_select` patterns:
            - build a model and all its downstream dependencies: `dbt_select="my_model+"`
            - build all models in a directory: `dbt_select="models/my_project"`
            ```
        """
        self.dbt_project_path = dbt_project_path
        self.dbt_repo_url = dbt_repo_url
        self.dbt_repo_url_secret = dbt_repo_url_secret
        self.dbt_repo_branch = dbt_repo_branch
        self.token = token
        self.token_secret = token_secret
        self.local_dbt_repo_path = local_dbt_repo_path
        self.dbt_selects = dbt_selects
        self.dbt_target = dbt_target
        self.stateful = stateful

        super().__init__(*args, name=name, **kwargs)
        self.gen_flow()

    @staticmethod
    def slugify(name):
        return name.replace(" ", "_").lower()

    def gen_flow(self) -> Flow:
        azure_secret_task = AzureKeyVaultSecret()
        dbt_repo_url = self.dbt_repo_url or azure_secret_task.bind(
            self.dbt_repo_url_secret
        )
        local_dbt_repo_path = (
            os.path.expandvars(self.local_dbt_repo_path)
            if self.local_dbt_repo_path is not None
            else "tmp_dbt_repo_dir"
        )
        clone_repo = CloneRepo(url=dbt_repo_url)
        clone = clone_repo.bind(
            url=dbt_repo_url,
            checkout_branch=self.dbt_repo_branch,
            token=self.token,
            token_secret=self.token_secret,
            path=local_dbt_repo_path,
            flow=self,
        )

        # dbt CLI does not handle passing --target=None
        dbt_target_option = (
            f"-t {self.dbt_target}" if self.dbt_target is not None else ""
        )

        # dbt_task = DBTTask(name="dbt_task_clean")

        if self.stateful:
            source_freshness_upstream = clone
        else:
            # Clean up artifacts from previous runs (`target/` dir and packages)
            dbt_clean_up = DBTTask(name="dbt_task_clean").bind(
                project_path=self.dbt_project_path, command="clean", flow=self
            )

            pull_dbt_deps = DBTTask(name="dbt_task_deps").bind(
                project_path=self.dbt_project_path, command="deps", flow=self
            )
            source_freshness_upstream = pull_dbt_deps

        # Source freshness
        # Produces `sources.json`
        source_freshness_select = self.dbt_selects.get("source_freshness")
        source_freshness_select_safe = (
            f"-s {source_freshness_select}"
            if source_freshness_select is not None
            else ""
        )

        source_freshness = DBTTask(name="dbt_task_source_freshness").bind(
            project_path=self.dbt_project_path,
            command=f"source freshness {source_freshness_select_safe} {dbt_target_option}",
            flow=self,
        )

        run_select = self.dbt_selects.get("run")
        run_select_safe = f"-s {run_select}" if run_select is not None else ""
        run = DBTTask(name="dbt_task_run").bind(
            project_path=self.dbt_project_path,
            command=f"run {run_select_safe} {dbt_target_option}",
            flow=self,
        )

        # Generate docs
        # Produces `catalog.json`, `run-results.json`, and `manifest.json`
        generate_catalog_json = DBTTask(name="dbt_task_docs_generate").bind(
            project_path=self.dbt_project_path,
            command=f"docs generate {dbt_target_option} --no-compile",
            flow=self,
        )

        test_select = self.dbt_selects.get("test", run_select)
        test_select_safe = f"-s {test_select}" if test_select is not None else ""
        test = DBTTask(name="dbt_task_test").bind(
            project_path=self.dbt_project_path,
            command=f"test {test_select_safe} {dbt_target_option}",
            flow=self,
        )

        # Upload build metadata to DataHub
        upload_metadata_draft.bind(recipe_path="self.datahub_recipe_path", flow=self)

        _cleanup_repo.bind(local_dbt_repo_path, flow=self)

        dbt_clean_up.set_upstream(clone, flow=self)
        pull_dbt_deps.set_upstream(dbt_clean_up, flow=self)
        source_freshness.set_upstream(source_freshness_upstream, flow=self)
        run.set_upstream(source_freshness, flow=self)
        generate_catalog_json.set_upstream(run, flow=self)
        test.set_upstream(generate_catalog_json, flow=self)
        upload_metadata_draft.set_upstream(test, flow=self)
        _cleanup_repo.set_upstream(upload_metadata_draft, flow=self)
