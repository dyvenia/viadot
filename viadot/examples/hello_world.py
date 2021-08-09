import prefect
from prefect import Flow, task
from prefect.run_configs import DockerRun
from prefect.storage import GitHub

from pathlib import Path

import inspect

file_path = inspect.getfile(lambda: None)
dir_path = Path(file_path).parent
# file_path = Path(__file__).resolve().parent

with open(str(dir_path) + "/answer.txt", "r") as my_file:
    answer = my_file.read()


@task
def say_hello():
    logger = prefect.context.get("logger")
    logger.info("Hello!")


@task
def show_answer():
    logger = prefect.context.get("logger")
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
    bye.set_upstream(answer, flow=flow)


if __name__ == "__main__":
    # flow.run()  # run locally
    flow.register(project_name="dev")  # deploy
