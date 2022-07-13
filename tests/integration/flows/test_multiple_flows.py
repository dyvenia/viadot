import logging

from viadot.flows import MultipleFlows


def test_multiple_flows_working(caplog):
    flow_list = [
        ["Flow of flows 1 test", "dev"],
        ["Flow of flows 2 - working", "dev"],
        ["Flow of flows 3", "dev"],
    ]
    flow = MultipleFlows(name="test", flows_list=flow_list)
    with caplog.at_level(logging.INFO):
        flow.run()
    assert "All of the tasks succeeded." in caplog.text


def test_multiple_flows_not_working(caplog):
    flow_list = [
        ["Flow of flows 1 test", "dev"],
        ["Flow of flows 2 test - not working", "dev"],
        ["Flow of flows 3", "dev"],
    ]
    flow = MultipleFlows(name="test", flows_list=flow_list)
    flow.run()
    assert "One of the flows has failed!" in caplog.text


def test_multiple_flows_working_one_flow(caplog):
    flow_list = [["Flow of flows 1 test", "dev"]]
    flow = MultipleFlows(name="test", flows_list=flow_list)
    with caplog.at_level(logging.INFO):
        flow.run()
    assert "All of the tasks succeeded." in caplog.text


def test_multiple_flows_last_not_working(caplog):
    flow_list = [
        ["Flow of flows 1 test", "dev"],
        ["Flow of flows 2 - working", "dev"],
        ["Flow of flows 2 test - not working", "dev"],
    ]
    flow = MultipleFlows(name="test", flows_list=flow_list)
    flow.run()
    assert "One of the flows has failed!" in caplog.text
