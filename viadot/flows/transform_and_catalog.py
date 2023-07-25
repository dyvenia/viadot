import os
from pathlib import Path
import shutil
from typing import Dict, List, Union

from prefect import Flow, task
from prefect.tasks.shell import ShellTask

from viadot.tasks import CloneRepo, AzureKeyVaultSecret, LumaIngest


@task
def _cleanup_repo(dbt_repo_dir_name: str) -> None:
    """
    Remove a repo folder.

    Args:
        dbt_repo_dir_name (str): The name of the temporary folder.
    """
    shutil.rmtree(dbt_repo_dir_name, ignore_errors=True)  # Delete folder on run


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
        metadata_dir_path: Union[str, Path] = None,
        luma_endpoint: str = "http://localhost/api/v1/dbt",
        luma_endpoint_secret: str = None,
        vault_name: str = None,
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
            metadata_dir_path (Union[str, Path]): The path to the directory containing metadata files.
                In the case of dbt, it's dbt project's `target` directory, which contains dbt artifacts
                (`sources.json`, `catalog.json`, `manifest.json`, and `run_results.json`). Defaults to None.
            luma_endpoint (str, optional): The endpoint of the Luma ingestion API. Defaults to "http://localhost/api/v1/dbt".
            luma_endpoint_secret (str, optional): The name of the secret storing the luma_endpoint. Defaults to None.
            vault_name (str, optional): The name of the vault from which to obtain the secrets. Defaults to None.

        Returns:
            List[str]: Lines from stdout of the `upload_metadata` task as a list.

        Examples:
            # Build a single model
            ```python
            import os
            from viadot.flows import TransformAndCatalog

            my_dbt_project_path = os.path.expanduser("~/dbt/my_dbt_project")
            my_datahub_recipe_path = os.path.expanduser("~/dbt/catalog/recipe.yaml")

            flow = TransformAndCatalog(
                name="DBT flow",
                dbt_project_path=my_dbt_project_path,
                dbt_repo_url=my_dbt_repo_url,
                token=my_token,
                dbt_selects={"run": "my_model"}
                metadata_dir_path="target",
                luma_endpoint="https://luma.dyvenia.lan/api/v1/dbt"
            )
            flow.run()
            ```

            Some common `dbt_select` patterns:
            - build a model and all its downstream dependencies: `dbt_select="my_model+"`
            - build all models in a directory: `dbt_select="models/my_project"`
            ```
        """
        # DBTTask
        self.dbt_project_path = dbt_project_path
        self.dbt_target = dbt_target
        self.dbt_selects = dbt_selects

        self.stateful = stateful

        # CloneRepo
        self.dbt_repo_url = dbt_repo_url
        self.dbt_repo_url_secret = dbt_repo_url_secret
        self.dbt_repo_branch = dbt_repo_branch
        self.token = token
        self.token_secret = token_secret
        self.local_dbt_repo_path = local_dbt_repo_path

        # LumaIngest
        self.metadata_dir_path = metadata_dir_path
        self.luma_endpoint = luma_endpoint
        self.luma_endpoint_secret = luma_endpoint_secret
        self.vault_name = vault_name

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

        if self.stateful:
            source_freshness_upstream = clone
        else:
            # Clean up artifacts from previous runs (`target/` dir and packages)

            dbt_clean_up = ShellTask(
                name="dbt_task_clean",
                command=f"dbt clean",
                helper_script=f"cd {self.dbt_project_path}",
                return_all=True,
                stream_output=True,
            ).bind(flow=self)

            pull_dbt_deps = ShellTask(
                name="dbt_task_deps",
                command=f"dbt deps",
                helper_script=f"cd {self.dbt_project_path}",
                return_all=True,
                stream_output=True,
            ).bind(flow=self)
            # source_freshness_upstream = pull_dbt_deps

        # Source freshness
        # Produces `sources.json`
        # source_freshness_select = self.dbt_selects.get("source_freshness")
        # source_freshness_select_safe = (
        #     f"-s {source_freshness_select}"
        #     if source_freshness_select is not None
        #     else ""
        # )

        # source_freshness = DBTTask(name="dbt_task_source_freshness").bind(
        #     project_path=self.dbt_project_path,
        #     command=f"source freshness {source_freshness_select_safe} {dbt_target_option}",
        #     flow=self,
        # )
        # source_freshness = ShellTask(
        #     name="dbt_task_source_freshness",
        #     command=f"dbt source freshness {source_freshness_select_safe} {dbt_target_option}",
        #     helper_script=f"cd {self.dbt_project_path}",
        #     return_all=True,
        #     stream_output=True,
        # ).bind(flow=self)

        run_select = self.dbt_selects.get("run")
        run_select_safe = f"-s {run_select}" if run_select is not None else ""

        run = ShellTask(
            name="dbt_task_run",
            command=f"dbt run {run_select_safe} {dbt_target_option}",
            helper_script=f"cd {self.dbt_project_path}",
            return_all=True,
            stream_output=True,
        ).bind(flow=self)

        # Generate docs
        # Produces `catalog.json`, `run-results.json`, and `manifest.json`
        generate_catalog_json = ShellTask(
            name="dbt_task_docs_generate",
            command=f"dbt docs generate {dbt_target_option} --no-compile",
            helper_script=f"cd {self.dbt_project_path}",
            return_all=True,
            stream_output=True,
        ).bind(flow=self)

        test_select = self.dbt_selects.get("test", run_select)
        test_select_safe = f"-s {test_select}" if test_select is not None else ""

        test = ShellTask(
            name="dbt_task_test",
            command=f"dbt test {test_select_safe} {dbt_target_option}",
            helper_script=f"cd {self.dbt_project_path}",
            return_all=True,
            stream_output=True,
        ).bind(flow=self)

        # Upload build metadata to Luma
        path_expanded = os.path.expandvars(self.metadata_dir_path)
        metadata_dir_path = Path(path_expanded)

        upload_metadata_luma = LumaIngest(
            name="luma_ingest_task",
            metadata_dir_path=metadata_dir_path,
            endpoint=self.luma_endpoint,
            credentials_secret=self.luma_endpoint_secret,
            vault_name=self.vault_name,
        ).bind(flow=self)

        _cleanup_repo.bind(local_dbt_repo_path, flow=self)

        dbt_clean_up.set_upstream(clone, flow=self)
        pull_dbt_deps.set_upstream(dbt_clean_up, flow=self)
        # source_freshness.set_upstream(pull_dbt_deps, flow=self)
        run.set_upstream(pull_dbt_deps, flow=self)
        generate_catalog_json.set_upstream(run, flow=self)
        test.set_upstream(generate_catalog_json, flow=self)
        upload_metadata_luma.set_upstream(test, flow=self)
        _cleanup_repo.set_upstream(upload_metadata_luma, flow=self)
