import pandas as pd

from typing import Any, Dict

from prefect import Flow, Task, Parameter

from viadot.tasks import SQLiteInsert, StatsToDF

uk_carbon_intensity_stats_to_df_task = StatsToDF()
sqlite_insert_task = SQLiteInsert()

class UKCarbonIntensityToSQLite(Flow):
    def __init__(
        self,
        days_back: int,
        df: pd.DataFrame,
        db_path: str,
        table_name: str,
        schema: str,
        dtypes: Dict[str, Any],
        if_exists: str = "skip",
        
    ):
        self.days_back = days_back
        self.df = df
        self.db_path = db_path
        self.table_name = table_name
        self.dtypes = dtypes
        self.schema = schema
        self.if_exists = if_exists
        
       
        super().__init__(*args, **kwargs)
        self.gen_flow()
        

    def gen_flow(self) -> Flow:
    
        uk_carbon_intensity_stats_to_df_task.bind(
            df = self.df,
            days_back = self.days_back,
            flow=self,
        )
        sqlite_insert_task.bind(
            schema=self.schema,
            table=self.table,
            dtypes=self.dtypes,
            sep=self.sep,
            if_exists=self.if_exists,
            flow=self,
        )

    
