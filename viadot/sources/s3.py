from typing import List, Dict, Literal
import boto3

import pandas as pd
import awswrangler as wr


class S3:
    """
    A class for pulling data from and uploading to S3.

    Args:
        profile_name (str, optional): The name of the profile.
        aws_secret_access_key (str, optional): AWS secret access key
        aws_session_token (str, optional): AWS temporary session token
    """

    # add env var
    def __init__(
        self,
        profile_name: str = None,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
    ):
        if profile_name:
            self.session = boto3.session.Session(profile_name=profile_name)
        elif aws_access_key_id and aws_secret_access_key:
            self.session = boto3.session.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
            )
        else:
            self.session = boto3.session.Session()

    def ls(self, path: str, suffix: str = None) -> List[str]:
        """
        Returns a list of files in a S3.
        Args:
            path (str): Path to a folder.
            suffix (Union[str, List[str], None]) - Suffix or List of suffixes for filtering S3 keys.
        """

        return wr.s3.list_objects(boto3_session=self.session, path=path, suffix=suffix)

    def exists(self, path: str) -> bool:
        """
        Check if a location exists in S3.
        Args:
            path (str): The path to check. Can be a file or a directory.
        Returns:
            bool: Whether the paths exists.
        """
        return wr.s3.does_object_exist(boto3_session=self.session, path=path)

    def upload(
        self,
        from_path: str,
        to_path: str,
    ) -> None:
        """
        Upload file(s) to the S3.
        Args:
            from_path (str): Path to the local file to be uploaded.
            to_path (str): Path to the destination S3 file/folder.
        """
        wr.s3.upload(boto3_session=self.session, local_file=from_path, path=to_path)

    def download(
        self,
        from_path: str,
        to_path: str,
    ) -> None:
        """
        Download file(s) from S3.

        Args:
            from_path (str): Path to a S3 folder.
            to_path (str): Path to a local folder.
        """

        wr.s3.download(boto3_session=self.session, path=from_path, local_file=to_path)

    def cp(self, paths: List[str], from_path: str, to_path: str):
        """
        Copies the contents of `from_path` to `to_path`.

        Args:
            paths (List[str]): List of S3 objects paths (e.g. [s3://bucket/dir0/key0, s3://bucket/dir0/key1]).
            from_path (str, optional): S3 Path for the source directory.
            to_path (str, optional): S3 Path for the target directory.
        """

        wr.s3.copy_objects(
            boto3_session=self.session,
            paths=paths,
            source_path=from_path,
            target_path=to_path,
        )

    def rm(self, path: str):
        """
        Deletes files in a path.
        Args:
            path (str): Path to a folder.
        """

        wr.s3.delete_objects(boto3_session=self.session, path=path)

    def from_df(
        self,
        df: pd.DataFrame = None,
        to_path: str = None,
        sep: str = ",",
        index: bool = False,
        compression: str = None,
    ):
        """
        Upload a pandas `DataFrame` to a csv or parquet file. You can choose different file backends, and have the option of compression.
        Args:
            df (pd.DataFrame, optional): Pandas DataFrame
            to_path (str, optional): Path to a S3 folder. Defaults to None.
            index (bool): Write row names (index).
            sep (str): Field delimiter for the output file.
            compression (str, optional): Compression style (None, snappy, gzip, zstd).
        """

        if to_path.endswith(".csv"):
            wr.s3.to_csv(
                boto3_session=self.session,
                df=df,
                path=to_path,
                dataset=False,
                index=index,
                sep=sep,
            )
        elif to_path.endswith(".parquet"):
            wr.s3.to_parquet(
                boto3_session=self.session,
                df=df,
                path=to_path,
                dataset=False,
                index=index,
                compression=compression,
            )
        else:
            raise ValueError("Only CSV and parquet formats are supported.")

    def to_df(self, from_path: str = None):
        """
        Reads a csv or parquet file to dataframe.
        Args:
            from_path (str, optional): Path to a S3 folder. Defaults to None.
        """
        if from_path.endswith(".csv"):
            df = wr.s3.read_csv(
                boto3_session=self.session, path=from_path, dataset=False
            )
        elif from_path.endswith(".parquet"):
            df = wr.s3.read_parquet(
                boto3_session=self.session, path=from_path, dataset=False
            )
        else:
            raise ValueError("Only CSV and parquet formats are supported.")
        return df


# s3_session = S3()
# print("Before upload:")
# print(s3_session.ls(path="s3://annger-bucket"))
# s3_session.upload(
#     from_path="test_final.parquet",
#     to_path="s3://annger-bucket/test_folder/test_file.parquet",
# )
# print("After upload:")
# print(s3_session.ls(path="s3://annger-bucket"))
# s3_session.download(
#     from_path="s3://annger-bucket/test_folder/test_file.parquet",
#     to_path="test_final_download.parquet",
# )
# s3_session.cp(
#     paths=["s3://annger-bucket/test_folder/test_file.parquet"],
#     from_path="s3://annger-bucket/test_folder/",
#     to_path="s3://annger-bucket/test_folder2",
# )
# print("After copy:")
# print(s3_session.ls(path="s3://annger-bucket"))

# print("Dataframe from to_df:")
# df = s3_session.to_df(from_path="s3://annger-bucket/test_folder2/test_file.parquet")
# print(df)

# s3_session.from_df(df=df, to_path="s3://annger-bucket/test_folder3/test_file.parquet")
# s3_session.from_df(df=df, to_path="s3://annger-bucket/test_folder4/test_file.csv")

# print("After from_df:")
# print(s3_session.ls(path="s3://annger-bucket"))
# s3_session.rm(path="s3://annger-bucket/test_folder/test_file.parquet")
# s3_session.rm(path="s3://annger-bucket/test_folder2/test_file.parquet")
# s3_session.rm(path="s3://annger-bucket/test_folder3/test_file.parquet")
# s3_session.rm(path="s3://annger-bucket/test_folder4/test_file.csv")
# print("After cleanup:")
# print(s3_session.ls(path="s3://annger-bucket"))
