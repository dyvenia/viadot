import logging
import os
from typing import Any, Dict, Optional, Tuple

import pandas as pd
import prefect
from prefect.artifacts import create_markdown
from prefect.engine import signals
from prefect.tasks.great_expectations import RunGreatExpectationsValidation
from prefect.utilities.tasks import defaults_from_attrs

import great_expectations as ge
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DatasourceConfig,
    FilesystemStoreBackendDefaults,
)
from great_expectations.validation_operators.types.validation_operator_result import (
    ValidationOperatorResult,
)


# Simplifies the setup and adds logging to the standard Prefect task
class RunGreatExpectationsValidation(RunGreatExpectationsValidation):
    """
    Task for running data validation with Great Expectations on a pandas DataFrame.
    See https://docs.prefect.io/api/latest/tasks/great_expectations.html#rungreatexpectationsvalidation
    for full documentation.

    Args:
        expectations_path (str): The path of your Great Expectations project, eg. `/home/viadot/my_flow`
        df (pd.DataFrame): The DataFrame to validate.
    """

    def __init__(
        self,
        df: pd.DataFrame = None,
        expectations_path: str = None,
        evaluation_parameters: Dict[str, Any] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.df = df
        self.expectations_path = expectations_path
        self.evaluation_parameters = evaluation_parameters

    @staticmethod
    def _get_batch_kwargs(df: pd.DataFrame) -> dict:
        dataset = ge.from_pandas(df)
        return {"dataset": dataset, "datasource": "pandas"}

    @staticmethod
    def _get_ge_context_local(expectations_path: str) -> BaseDataContext:
        """
        This is configured to work with an in-memory pandas DataFrame.
        This setup allows us to run validations before (perhaps unnecessarily) writing any data
        to disk, as well as at any other stage.

        Currently using local storage.

        Args:
        expectations_path (str): The path to your Great Expectations project,
        eg. `/home/viadot/my_flow`. Expectation suites need to be placed inside the
        `expectations` folder, eg. `/home/viadot/my_flow/expectations/failure.json`.

        Returns:
            BaseDataContext: The GE context (ie. config) required to run the validations.
        """
        data_context_config = DataContextConfig(
            datasources={
                "pandas": DatasourceConfig(
                    class_name="PandasDatasource",
                    batch_kwargs_generators={},  # override the CSV default
                )
            },
            store_backend_defaults=FilesystemStoreBackendDefaults(expectations_path),
            validation_operators={
                "action_list_operator": {
                    "class_name": "ActionListValidationOperator",
                    "action_list": [
                        {
                            "name": "store_validation_result",
                            "action": {"class_name": "StoreValidationResultAction"},
                        },
                        {
                            "name": "store_evaluation_params",
                            "action": {"class_name": "StoreEvaluationParametersAction"},
                        },
                        {
                            "name": "update_data_docs",
                            "action": {"class_name": "UpdateDataDocsAction"},
                        },
                    ],
                }
            },
        )
        context = BaseDataContext(project_config=data_context_config)
        return context

    def run_new(
        self,
        checkpoint_name: str = None,
        context: "ge.DataContext" = None,
        assets_to_validate: list = None,
        batch_kwargs: dict = None,
        expectation_suite_name: str = None,
        context_root_dir: str = None,
        runtime_environment: Optional[dict] = None,
        run_name: str = None,
        run_info_at_end: bool = True,
        disable_markdown_artifact: bool = None,
        validation_operator: str = "action_list_operator",
        evaluation_parameters: dict = None,
    ):
        """Adds evaluation parameters to GE task"""

        runtime_environment = runtime_environment or dict()

        # Load context if not provided directly
        if not context:
            context = ge.DataContext(
                context_root_dir=context_root_dir,
                runtime_environment=runtime_environment,
            )

        # Check that the parameters are mutually exclusive
        if (
            sum(
                bool(x)
                for x in [
                    (expectation_suite_name and batch_kwargs),
                    assets_to_validate,
                    checkpoint_name,
                ]
            )
            != 1
        ):
            raise ValueError(
                "Exactly one of expectation_suite_name + batch_kwargs, assets_to_validate, or "
                "checkpoint_name is required to run validation."
            )

        # If assets are not provided directly through `assets_to_validate` then they need be loaded
        #   if a checkpoint_name is supplied, then load suite and batch_kwargs from there
        #   otherwise get batch from `batch_kwargs` and `expectation_suite_name`

        if not assets_to_validate:
            assets_to_validate = []
            if checkpoint_name:
                ge_checkpoint = context.get_checkpoint(checkpoint_name)

                for batch in ge_checkpoint["batches"]:
                    batch_kwargs = batch["batch_kwargs"]
                    for suite_name in batch["expectation_suite_names"]:
                        suite = context.get_expectation_suite(suite_name)
                        batch = context.get_batch(batch_kwargs, suite)
                        assets_to_validate.append(batch)
                validation_operator = ge_checkpoint["validation_operator_name"]
            else:
                assets_to_validate.append(
                    context.get_batch(batch_kwargs, expectation_suite_name)
                )

        # Run validation operator
        results = context.run_validation_operator(
            validation_operator,
            assets_to_validate=assets_to_validate,
            run_id={"run_name": run_name or prefect.context.get("task_slug")},
            evaluation_parameters=evaluation_parameters,
        )

        # Generate artifact markdown
        if not disable_markdown_artifact:
            run_info_at_end = True
            validation_results_page_renderer = (
                ge.render.renderer.ValidationResultsPageRenderer(
                    run_info_at_end=run_info_at_end
                )
            )
            rendered_document_content_list = (
                validation_results_page_renderer.render_validation_operator_result(
                    validation_operator_result=results
                )
            )
            markdown_artifact = " ".join(
                ge.render.view.DefaultMarkdownPageView().render(
                    rendered_document_content_list
                )
            )

            create_markdown(markdown_artifact)

        if not results.success:
            raise signals.FAIL(result=results)

        return results

    @defaults_from_attrs("df", "expectations_path", "evaluation_parameters")
    def run(self, df: pd.DataFrame = None, expectations_path: str = None, **kwargs):
        self.logger.warning(expectations_path)
        self.logger.warning(os.listdir("/home/viadot/"))
        self.logger.warning(os.listdir("/home/viadot/workstreams_vel/flows"))
        batch_kwargs = self._get_batch_kwargs(df)
        context = self._get_ge_context_local(expectations_path)

        self.logger.info("Beginning validation run...")

        try:
            results = self.run_new(  # TODO: change to super() once eval params are added to Prefect
                batch_kwargs=batch_kwargs,  # input data
                context=context,  # ~project config
                **kwargs,
            )
        except signals.FAIL as e:
            results = e.state.result

        # Show summary of results
        n_successful, n_expectations = self._get_stats_from_results(results)
        status = "success" if results.success else "failure"
        level = logging.INFO if results.success else logging.ERROR
        self.logger.log(
            msg=f"Validation finished with status '{status}'. {n_successful}/{n_expectations} test(s) passed.",
            level=level,
        )

        data_docs_path = os.path.join(
            expectations_path, "uncommitted", "data_docs", "local_site", "index.html"
        )
        self.logger.info(f"To explore the docs, open {data_docs_path} in a browser.")

        if not results.success:
            raise signals.FAIL(result=results)

        return results

    def _get_stats_from_results(
        self, result: ValidationOperatorResult
    ) -> Tuple[int, int]:
        result_identifier = result.list_validation_result_identifiers()[0]
        stats = result._list_validation_statistics()[result_identifier]
        n_successful = stats["successful_expectations"]
        n_expectations = stats["evaluated_expectations"]
        return n_successful, n_expectations
