import os

os.system("clear")

import pytest

from viadot.sources import Mindful


@pytest.mark.init
def test_instance_mindful():
    instance = Mindful()
    assert instance


# instance = Mindful()
# print(instance)
# print(isinstance(instance, ))
