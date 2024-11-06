import pendulum
import pytest

from viadot.orchestration.prefect.utils import DynamicDateHandler


ddh1 = DynamicDateHandler(
    ["<<", ">>"], dynamic_date_format="%Y%m%d", dynamic_date_timezone="Europe/Warsaw"
)

ddh2 = DynamicDateHandler(
    ["[[", "]]"], dynamic_date_format="%Y%m%d", dynamic_date_timezone="Europe/Warsaw"
)


@pytest.fixture
def setup_dates():
    """Fixture to provide the current dates for comparison in tests."""
    today = pendulum.today("Europe/Warsaw")
    yesterday = pendulum.yesterday("Europe/Warsaw")
    last_month = today.subtract(months=1).month
    last_year = today.subtract(years=1)
    last_day_prev_month = today.subtract(months=1).end_of("month")
    now_time = pendulum.now("Europe/Warsaw")
    return {
        "today": today.strftime("%Y%m%d"),
        "yesterday": yesterday.strftime("%Y%m%d"),
        "current_month": today.strftime("%m"),
        "last_month": f"{last_month:02d}",
        "current_year": today.strftime("%Y"),
        "last_year": last_year.strftime("%Y"),
        "last_day_previous_month": last_day_prev_month.strftime("%Y%m%d"),
        "now_time": now_time.strftime("%H%M%S"),
    }


def test_create_dates(setup_dates):
    """Test if create_dates function returns correct dictionary values."""
    keys_to_compare = [
        "today",
        "yesterday",
        "current_month",
        "last_month",
        "current_year",
        "last_year",
        "last_day_previous_month",
    ]
    assert all(setup_dates[key] == ddh1.replacements[key] for key in keys_to_compare)


def test_process_dates_single(setup_dates):
    """Test if process_dates function replaces a single date placeholder."""
    text = "Today's date is <<today>>."
    replaced_text = ddh1.process_dates(text)
    expected_text = f"Today's date is {setup_dates['today']}."
    assert replaced_text == expected_text


def test_process_dates_multiple(setup_dates):
    """Test if process_dates function replaces multiple placeholders."""
    text = "Today is <<today>>, yesterday was <<yesterday>>"
    replaced_text = ddh1.process_dates(text)
    expected_text = (
        f"Today is {setup_dates['today']}, yesterday was {setup_dates['yesterday']}"
    )
    assert replaced_text == expected_text


def test_process_dates_with_custom_symbols(setup_dates):
    """Test if process_dates function works with custom start and end symbols."""
    text = "The year is [[current_year]]."
    replaced_text = ddh2.process_dates(text)
    expected_text = f"The year is {setup_dates['current_year']}."
    assert replaced_text == expected_text


def test_process_dates_with_malformed_keys():
    """Test if process_dates function leaves malformed placeholders unchanged."""
    text = "This is a malformed placeholder: <<today."
    replaced_text = ddh1.process_dates(text)
    assert replaced_text == text


def test_process_eval_date_success(setup_dates):
    """Test if process_dates function works with a pendulum code."""
    text = "Yesterday was <<pendulum.today().subtract(days=1)>>."
    replaced_text = ddh1.process_dates(text)
    expected_text = f"Yesterday was {setup_dates['yesterday']}."
    assert replaced_text == expected_text


def test_process_eval_date_fail():
    """Test if process_dates function works with a pendulum code."""
    text = "Yesterday was <<(pendulum.today().subtract(days=1))>>."  # It should not start with `(`
    with pytest.raises(TypeError):
        ddh1.process_dates(text)

    text2 = "Yesterday was <<some_operation=['1','2']>>."  # only `pendulum` works
    with pytest.raises(TypeError):
        ddh1.process_dates(text2)


def test_process_last_x_years():
    """Test if process_dates function returns a date range of last x years."""
    x = 5
    text = f"<<last_{x}_years>>"
    processed_range = ddh1.process_dates(text)
    x_years_ago = pendulum.today().year - (x - 1)
    expected_result = [str(x_years_ago + i) for i in range(x)]
    assert processed_range == expected_result


def test_process_last_x_months():
    """Test if process_dates function returns a date range of last x years."""
    x = 18
    text = f"<<last_{x}_months>>"
    processed_range = ddh1.process_dates(text)
    start_date = pendulum.today().subtract(months=(x - 1))
    expected_result = [
        start_date.add(months=i).start_of("month").format("YMM") for i in range(x)
    ]
    assert processed_range == expected_result


def test_process_y_years_from_x():
    """Test if `process_dates()` returns a range of years from a given start year."""
    x = 2019
    y = 4
    text = f"<<{y}_years_from_{x}>>"
    processed_range = ddh1.process_dates(text)
    expected_result = [str(x + i) for i in range(y)]
    assert processed_range == expected_result


def test_process_string():
    """Test the `_process_string` function to verify it correctly processes dates."""
    result = ddh1._process_string("Let's meet <<today>>.")
    assert result == f"Let's meet {pendulum.today().strftime('%Y%m%d')}."

    result = ddh1._process_string(
        "The event is scheduled for <<pendulum.tomorrow().strftime('%Y-%m-%d')>>."
    )
    assert (
        result
        == f"The event is scheduled for {pendulum.tomorrow().strftime('%Y-%m-%d')}."
    )
