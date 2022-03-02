#!/usr/bin/env python
# coding: utf-8

# In[1]:


from os import times_result
from typing import List, Literal
import prefect
from datetime import date, datetime, timedelta
import pandas as pd

from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs
from prefect.utilities import logging

logger = logging.get_logger()
    
class ReRunFailedFlow(Task):
    def __init__(
        self,
        flow_name: str = None,
        *args,
        **kwargs,
    ):

        self.flow_name = flow_name

        super().__init__(
            name="prefect_extract_details",
            *args,
            **kwargs,
        )

    def __call__(self):
        """Extract time from Prefect Flow run"""
        super().__call__(self)
        
        
    def check_if_scheduled_run(self, created_by_user_id: str = None) -> bool:
        """
        Check if run was scheduled or started by user.

        Args:
            created_by_user_id (str, optional): User_id in prefect. Defaults to None.

        Returns:
            bool: True if flow run was started automatically. False if Flow was started by user.
        """

        if created_by_user_id == "09720c91-a99c-4f72-b7b5-3c061c83408b" :
            return True
        else:
            return False

    def get_formatted_date(self, time_unclean: str = None):
        """
        Format date from "2022-02-21T01:00:00+00:00" to date or time.

        Args:
            time_unclean (str, optional): Time in datetime format obtained from Prefect. Defaults to None.

        Returns:
            List of two paramaters: [datetime.date, datetime.time]
        """

        time_extracted = time_unclean.split("T")[1]
        time_clean_str = time_extracted.split(".")[0]
        time_clean = datetime.strptime(time_clean_str[:8], "%H:%M:%S")

        date_extracted = time_unclean.split("T")[0]
        date_clean = datetime.strptime(date_extracted, "%Y-%m-%d")
        return [date_clean, time_clean]    

    

    @defaults_from_attrs(
        "flow_name",
    )
    def run(
        self,
        flow_name,
        **kwargs,
    ) -> None:

        query = (
            """
             {           
                flow (where: { name: { _eq: "%s" } } )
                {
                id,
                flow_runs(
                    order_by: {end_time: desc}
                    where: {start_time:{ _is_null:false } } ) 
                    {
                      id
                      state
                      start_time
                      scheduled_start_time
                      created_by_user_id
                    }  
                } 
            }
        """
            % flow_name
        )
   
        client = prefect.Client()
        flow_runs = client.graphql(query)
        last_flow_run = flow_runs.data.flow[0]["flow_runs"][0]   # list of all parameters for the most recent flow_run
        main_flow_id = flow_runs.data.flow[0].id     # ID of a parent flow
              
        scheduler_id = last_flow_run["created_by_user_id"]
        is_scheduled = self.check_if_scheduled_run(created_by_user_id=scheduler_id)                 
            
        if last_flow_run.state != 'Success' and is_scheduled == True:
            # print("Failed flow with id: ", last_flow_run.id)
            
            last_schedule_datetime_clean = self.get_formatted_date(last_flow_run.scheduled_start_time)   
            last_schedule_date_clean = last_schedule_datetime_clean[0].date()
            last_schedule_time_clean = last_schedule_datetime_clean[1].time()
            new_schedule = datetime.strptime("{} {}".format(last_schedule_date_clean, last_schedule_time_clean), "%Y-%m-%d %H:%M:%S") + timedelta(minutes = 5)
            
            client.create_flow_run(flow_id=main_flow_id, scheduled_start_time=new_schedule)
            msg = f"New schedule for flow '{flow_name}' is authomatically set to {new_schedule}"
            
        elif last_flow_run.state != 'Success' and is_scheduled == False:   
            msg = f"Failed flow run with id: {last_flow_run.id} and authomatical schedule = {is_scheduled}. You manual actions is needed."

        else:
            msg = f"Successful flow run with id {last_flow_run.id}. :) "

        print(msg)
        



