from viadot.flows import MultipleFlows
import logging


def test_multiple_flows_working(caplog):
    list = [
        ["Flow of flows 1 test", "dev"],
        ["Flow of flows 2 - working", "dev"],
        ["Flow of flows 3", "dev"],
    ]
    flow = MultipleFlows(name="test", flows_list=list)
    with caplog.at_level(logging.INFO):
        flow.run()
    assert "All of the tasks succeeded." in caplog.text


def test_multiple_flows_not_working(caplog):
    list = [
        ["Flow of flows 1 test", "dev"],
        ["Flow of flows 2 test - not working", "dev"],
        ["Flow of flows 3", "dev"],
    ]
    flow = MultipleFlows(name="test", flows_list=list)
    flow.run()
    assert "One of the flows has failed!" in caplog.text
