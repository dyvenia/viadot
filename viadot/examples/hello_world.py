import inspect
import os
from pathlib import Path

import prefect
from prefect import Flow, task
from prefect.run_configs import DockerRun
from prefect.storage import GitHub
from prefect.utilities import logging

file_path = inspect.getfile(lambda: None)
dir_path = Path(file_path).parent
# file_path = Path(__file__).resolve().parent
file_path = str(dir_path) + "/answer.txt"

logger = logging.get_logger(__name__)
logger.warning(file_path)
logger.warning(os.getcwd())
logger.warning(os.listdir())
logger.warning(os.listdir("/home"))


@task
def say_hello():
    logger = prefect.context.get("logger")
    logger.info("Hello!")


@task
def show_answer():
    logger = prefect.context.get("logger")
    with open(file_path, "r") as my_file:
        answer = my_file.read()
    logger.info(
        f"The answer to the Ultimate Question of Life, the Universe, and Everything is: {answer}"
    )


@task
def say_bye():
    logger = prefect.context.get("logger")
    logger.info("Bye!")


STORAGE = GitHub(
    repo="dyvenia/viadot",
    path="viadot/examples/hello_world.py",
    ref="0.2.3",
    access_token_secret="github_token",  # name of the Prefect secret with the GitHub token
)
RUN_CONFIG = DockerRun(
    image="prefecthq/prefect",
    env={"SOME_VAR": "value"},
    labels=["dev"],
)

with Flow("Hello, world!", storage=STORAGE, run_config=RUN_CONFIG) as flow:
    hello = say_hello()
    print_answer = show_answer()
    bye = say_bye()

    print_answer.set_upstream(hello, flow=flow)
    bye.set_upstream(print_answer, flow=flow)


if __name__ == "__main__":
    # flow.run()  # run locally
    flow.register(project_name="dev")  # deploy
