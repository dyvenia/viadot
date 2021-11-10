import pandas as pd
from typing import Any, Dict, List

from prefect import Flow, Task, Parameter

from viadot.tasks import SQLiteInsert, StatsToDF

uk_carbon_intensity_stats_to_df_task = StatsToDF()
sqlite_insert_task = SQLiteInsert()

class UKCarbonIntensityToSQLite(Flow):
    def __init__(
        self,
        name: str,
        days_back: int,
        df: pd.DataFrame,
        db_path: str, 
        table_name: str,
        if_exists: str,
        dtypes: Dict[str, Any],
        schema: str = None,
        *args: List[any],
        **kwargs: Dict[str, Any]
    ):
        self.days_back = days_back
        self.df = df
        self.db_path = db_path
        self.schema = schema
        self.table_name = table_name
        self.dtypes = dtypes
        self.if_exists = if_exists
        super().__init__(*args,name=name,**kwargs)
        self.gen_flow()
    

    def gen_flow(self) -> Flow:
        uk_carbon_intensity_stats_to_df_task.bind(
            days_back = self.days_back,
            flow=self
        )
        sqlite_insert_task.bind(
            db_path=self.db_path,
            df=self.df,
            schema=self.schema,
            dtypes=self.dtypes,
            table_name=self.table_name,
            if_exists=self.if_exists,
            flow=self
        )
        
    def run(self):
        df = uk_carbon_intensity_stats_to_df_task.run()
        sqlite_insert_task.run(
            db_path=self.db_path, 
            schema=self.schema,
            dtypes=self.dtypes,
            table_name=self.table_name,
            if_exists=self.if_exists,
            df=df)
        
        
    
