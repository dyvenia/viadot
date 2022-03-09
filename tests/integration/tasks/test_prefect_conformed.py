import datetime
from datetime import datetime, timedelta

from viadot.tasks import ReRunFailedFlow

PREFECT_CONFORMED_TASK = ReRunFailedFlow()
DATE_FROM_PREFECT = "2022-03-08T11:25:22.490586+00:00"
DATE_FROM_PREFECT2 = "2022-03-08T11:35:22.490586+00:00"
DATE_FROM_PREFECT3 = "2022-03-08T12:25:22.490586+00:00"

DATE_FROM_PREFECT_CLEAN = datetime(2022, 3, 8, 11, 25, 22)
DATE_FROM_PREFECT_CLEAN2 = datetime(2022, 3, 8, 11, 35, 22)
DATE_FROM_PREFECT_CLEAN3 = datetime(2022, 3, 8, 12, 25, 22)

PREFECT_JSON = {
  "data": {
    "flow": [
      {
        "id": "4cd71aa1-1b56-40c9-bd06-deeaadf62931",
        "flow_runs": [
          {
            "id": "baa2353e-7a37-46f6-819a-1d11ea582525",
            "state": "Failed",
            "start_time": "2022-03-08T12:39:05.379383+00:00",
            "scheduled_start_time": "2022-03-08T12:38:34.161073+00:00",
            "created_by_user_id": "5878be75-ee66-42f4-8179-997450063ea4"
          },
          {
            "id": "d8ec69c2-ea5d-4f8c-8fc7-96d0a2e078b6",
            "state": "Failed",
            "start_time": "2022-03-08T12:34:15.236954+00:00",
            "scheduled_start_time": "2022-03-08T12:33:53+00:00",
            "created_by_user_id": "09720c91-a99c-4f72-b7b5-3c061c83408b"
          },
        ]
      },
      {
        "id": "f456999c-d04f-4f2f-affd-00830df06cb7",
        "flow_runs": []
      },
      {
        "id": "71e11b29-9bc1-46c0-9c5b-c220a8e2ac68",
        "flow_runs": [
          {
            "id": "b40623a5-4071-4361-9503-243d182b6e53",
            "state": "Success",
            "start_time": "2022-03-02T11:25:22.490586+00:00",
            "scheduled_start_time": "2022-03-02T11:25:00+00:00",
            "created_by_user_id": "09720c91-a99c-4f72-b7b5-3c061c83408b"
          }
        ]
      }
    ]
  }
}


def test_check_if_scheduled_run():
    is_scheduled = PREFECT_CONFORMED_TASK.check_if_scheduled_run(
        created_by_user_id = "09720c91-a99c-4f72-b7b5-3c061c83408b"
    )
    assert is_scheduled is True

    is_scheduled = PREFECT_CONFORMED_TASK.check_if_scheduled_run(
        created_by_user_id = "5878be75-ee66-42f4-8179-997450063ea4"
    )
    assert is_scheduled is False
    

def test_get_formatted_dated():
    new_date_list = PREFECT_CONFORMED_TASK.get_formatted_date(
        time_unclean = DATE_FROM_PREFECT
    )
    assert new_date_list == datetime(2022, 3, 8, 11, 25, 22)

    
def test_set_new_schedule():
    new_schedule = PREFECT_CONFORMED_TASK.set_new_schedule(
        last_run_scheduled_start_time = DATE_FROM_PREFECT_CLEAN,
        last_run_start_time = DATE_FROM_PREFECT_CLEAN2,
        minutes_delay = 10
    )
    assert new_schedule == datetime.now().replace(microsecond=0) + timedelta(minutes = 10)
    
    new_schedule = PREFECT_CONFORMED_TASK.set_new_schedule(
        last_run_scheduled_start_time = DATE_FROM_PREFECT_CLEAN,
        last_run_start_time = DATE_FROM_PREFECT_CLEAN3,
        minutes_delay = 10
    )
    assert new_schedule == datetime(2022, 3, 8, 11, 35, 22)


# In[ ]:




