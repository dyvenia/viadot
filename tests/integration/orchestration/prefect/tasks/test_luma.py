from loguru import logger
import pytest

from viadot.orchestration.prefect.tasks import luma_ingest_task


@pytest.mark.asyncio
async def test_luma_ingest_task_model_metadata(LUMA_URL):
    logs = await luma_ingest_task.fn(
        metadata_kind="model",
        metadata_dir_path="tests/resources/metadata/model",
        luma_url=LUMA_URL,
        logger=logger,
        raise_on_failure=False,
    )
    log = "\n".join(logs)
    success_message = "The request was successful!"

    assert success_message in log


@pytest.mark.asyncio
async def test_luma_ingest_task_model_run_metadata(LUMA_URL):
    logs = await luma_ingest_task.fn(
        metadata_kind="model_run",
        metadata_dir_path="tests/resources/metadata/model_run",
        luma_url=LUMA_URL,
        logger=logger,
        raise_on_failure=False,
    )
    log = "\n".join(logs)
    success_message = "The request was successful!"

    assert success_message in log


@pytest.mark.asyncio
async def test_luma_ingest_task_model_run_metadata_follow(LUMA_URL):
    logs = await luma_ingest_task.fn(
        metadata_kind="model_run",
        metadata_dir_path="tests/resources/metadata/model_run",
        luma_url=LUMA_URL,
        follow=True,
        logger=logger,
        raise_on_failure=False,
    )
    log = "\n".join(logs)
    success_message = "The request was successful!"

    assert success_message in log
