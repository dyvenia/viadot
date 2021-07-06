from prefect import Task

from ..sources import AzureBlobStorage


class BlobFromCSV(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(name="csv_to_blob_storage", *args, **kwargs)

    def __call__(self):
        """Generate a blob from a local CSV file"""

    def run(self, from_path: str, to_path: str, overwrite: bool = False):

        blob_storage = AzureBlobStorage()

        self.logger.info(f"Copying from {from_path} to {to_path}...")
        blob_storage.to_storage(
            from_path=from_path, to_path=to_path, overwrite=overwrite
        )
        self.logger.info(f"Successfully uploaded data to {to_path}.")
