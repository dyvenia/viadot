import prefect
from prefect import task, Flow
from prefect.storage import GitHub
from prefect.run_configs import DockerRun


@task
def say_hello():
    logger = prefect.context.get("logger")
    logger.info("Hello!")


@task
def say_bye():
    logger = prefect.context.get("logger")
    logger.info("Bye!")


STORAGE = GitHub(
    repo="dyvenia/fiotto",
    path="fiotto/examples/hello_world.py",
    access_token_secret="github_token",  # name of the Prefect secret with the GitHub token
)
RUN_CONFIG = DockerRun(
    image="prefecthq/prefect",
    env={"SOME_VAR": "value"},
    labels=["testing"],
)

with Flow("Hello, world!", storage=STORAGE, run_config=RUN_CONFIG) as flow:
    hello = say_hello()
    bye = say_bye()
    bye.set_upstream(hello, flow=flow)


if __name__ == "__main__":
    flow.run()
