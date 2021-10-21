import json
import os
from typing import Any, Dict, List, Union
from pathlib import Path
import pandas as pd
import pendulum
import prefect
from prefect import Flow, Task, apply_map, task
from prefect.backend import set_key_value
from prefect.tasks.secrets import PrefectSecret
from prefect.utilities import logging
from visions.functional import infer_type
from visions.typesets.complete_set import CompleteSet

from ..task_utils import (
	df_get_data_types_task,
	add_ingestion_metadata_task, 
	# update_dtypes_dict
)
from ..tasks import AzureDataLakeUpload
from ..tasks.sharepoint import SharepointToDF

excel_to_df_task = SharepointToDF()
file_to_adls_task = AzureDataLakeUpload()
json_to_adls_task = AzureDataLakeUpload()


@task
def dtypes_to_json_task(dtypes_dict, local_json_path: str):
    with open(local_json_path, "w") as fp:
        json.dump(dtypes_dict, fp)


@task
def df_to_csv_task(df, path="output.csv", sep='\t', if_exists="replace", **kwargs) -> None:
	if if_exists == "append":
		if os.path.isfile(path):
			csv_df = pd.read_csv(path)
			out_df = pd.concat([csv_df, df])
		else:
			out_df = df
	elif if_exists == "replace":
		out_df = df

	out_df.to_csv(path, index=False, sep=sep)


@task
def df_to_parquet_task(df, path="output.parquet", if_exists="replace", **kwargs):
	if if_exists == "append":
		if os.path.isfile(path):
			parquet_df = pd.read_parquet(path)
			out_df = pd.concat([parquet_df, df])
		else:
			out_df = df
	elif if_exists == "replace":
		out_df = df

	out_df.to_parquet(path, index=False, **kwargs)


@task
def df_mapp_mixed_dtypes_for_parquet_task(df, dtypes_dict):
	df_mapped = df.copy()
	for col, dtype in dtypes_dict.items():
		if dtype != 'Date': #datetime64[ns]
				if dtype == 'DateTime':
					df_mapped[col] = df_mapped[col].astype('string')
				else:
					df_mapped[col] = df_mapped[col].astype(f'{dtype.lower()}')
		if dtype == 'Object':
			df_mapped[col] = df_mapped[col].astype('string')
	return df_mapped



class SharepointToADLS(Flow):
	def __init__(
		self,
		name: str = None,
		nrows_to_df: int = None,
		adls_sp_credentials_secret: str = None,
		file_from_sharepoint: str = None,
		output_file_extension: str = ".csv",
		local_dir_path: str = None,
		adls_dir_path: str = None,
		if_empty: str = "warn",
		if_exists: str = "replace",
		*args: List[any],
		**kwargs: Dict[str, Any],
	):

		# ShareointToDF
		self.if_empty = if_empty
		self.nrows = nrows_to_df
		self.file_from_sharepoint = file_from_sharepoint
		self.local_dir_path = local_dir_path
		
		# AzureDataLakeUpload
		self.adls_sp_credentials_secret = adls_sp_credentials_secret
		self.if_exists = if_exists
		self.output_file_extension = output_file_extension
		self.now = str(pendulum.now("utc"))
		if self.local_dir_path != None:
			self.local_file_path = (
				self.local_dir_path + self.slugify(name) + self.output_file_extension
			)
		else: 
			self.local_file_path = (
				self.slugify(name) + self.output_file_extension
			)
		self.local_json_path = self.slugify(name) + ".json"
		self.adls_schema_file_dir_file = os.path.join(
            adls_dir_path, "schema", self.now + ".json"
        )
		self.adls_dir_path = adls_dir_path
		self.adls_file_path = os.path.join(
			adls_dir_path, self.now + self.output_file_extension
		)

		super().__init__(*args, name=name, **kwargs)

		self.gen_flow()

	def gen_flow(self) -> Flow:
		df = excel_to_df_task.bind(
			path_to_file = self.file_from_sharepoint, 
			nrows = self.nrows,
			flow=self
		)
		df_with_metadata = add_ingestion_metadata_task.bind(df, flow=self)
		dtypes_dict = df_get_data_types_task.bind(df_with_metadata, flow=self)
		df_mapped = df_mapp_mixed_dtypes_for_parquet_task.bind(
					df_with_metadata,dtypes_dict,
					 flow=self
					 )
		# dtypes_dict_mapped = df_get_data_types_task.bind(df_with_metadata, flow=self)
		if self.output_file_extension == ".parquet":
			
			df_to_file = df_to_parquet_task.bind(
				df=df_mapped,
				path=self.local_file_path, 
				if_exists=self.if_exists,
				flow=self,
			)
		else:
			df_to_file = df_to_csv_task.bind(
				df=df_with_metadata,
				path=self.local_file_path,
				if_exists=self.if_exists,
				flow=self
			)
		# dtypes_updated = update_dtypes_dict(dtypes_dict=dtypes_dict, flow=self)

		file_to_adls_task.bind(
			from_path=self.local_file_path,
			to_path=self.adls_file_path,
			sp_credentials_secret=self.adls_sp_credentials_secret,
			flow=self,
		)

		# jak nie zadziala to trzeba bedzie zrobic dtypes_dict = dtypes_dict_mapped
		dtypes_to_json_task.bind(
		dtypes_dict=dtypes_dict, local_json_path=self.local_json_path, flow=self
		)
		json_to_adls_task.bind(
			from_path=self.local_json_path,
			to_path=self.adls_schema_file_dir_file,
			sp_credentials_secret=self.adls_sp_credentials_secret,
			flow=self,
		)

		df_mapped.set_upstream(df_with_metadata, flow=self)
		# update_dtypes_dict.set_upstream(df_mapped, flow=self)
		dtypes_to_json_task.set_upstream(df_mapped, flow=self)
		df_to_file.set_upstream(dtypes_to_json_task, flow=self)
		
		file_to_adls_task.set_upstream(df_to_file, flow=self)
		json_to_adls_task.set_upstream(dtypes_to_json_task, flow=self)
		set_key_value(key=self.adls_dir_path, value=self.adls_file_path)

	@staticmethod
	def slugify(name):
		return name.replace(" ", "_").lower()