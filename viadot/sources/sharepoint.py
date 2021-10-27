from pandas._config import config
import sharepy 
import pandas as pd
import os

from .base import Source
from ..config import local_config

import visions
from visions.functional import infer_type
from visions.typesets.complete_set import CompleteSet

class Sharepoint(Source):
	def __init__(self, 
		site: str = None, 
		username: str = None, 
		password: str = None,
		url_to_file: str = None,
		*args, 
		**kwargs):
		"""
		A Sharepoint class to connect and download specific Excel file from Sharepoint.
        You should't be connected with VPN during connect to sharepoint and download file.

		Args:
			site (str, optional): Path to sharepoint website (e.g : {tenant_name}.sharepoint.com). Defaults to None.
			username (str, optional): Sharepoint username (e.g username@{tenant_name}.com). Defaults to None.
			password (str, optional): Sharepoint password. Defaults to None.
			url_to_file (str, optional): Full url to file 
					(e.g : https://{tenant_name}.sharepoint.com/sites/{folder}/Shared%20Documents/Dashboard/{file_name}). Defaults to None.
		"""		
		credentials = local_config.get("SHAREPOINT")
		self.site= site or credentials["site"]
		self.username= username or credentials["username"]
		self.password=password or credentials["password"]
		self.url_to_file= url_to_file or credentials["file_url"]

		super().__init__(*args, **kwargs)
        
		
	def check_connection(self):	
		return sharepy.connect(
			site=self.site, 
			username=self.username, 
			password=self.password)

	
	def download_file(self,filename:str = 'Sharepoint_file.xlsm') -> None:
		conn = self.check_connection()
		conn.getfile(url=self.url_to_file, filename=filename)

