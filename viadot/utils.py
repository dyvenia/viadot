import pandas as pd
from prefect import Task


def slugify(name: str) -> str:
    return name.replace(" ", "_").lower()


class FlattenDataFrame(Task):
    def __init__(self, dataframe: pd.DataFrame = None, *args, **kwargs):
        self.dataframe = dataframe
        super().__init__(name="flatten_df", *args, **kwargs)

    def __call__(self, *args, **kwargs):
        """Flatten columns in previously loaded pandas Data Frame"""
        return super().__call__(*args, **kwargs)

    def run(self, dataframe: pd.DataFrame = None):
        s = (dataframe.applymap(type) == list).all()
        list_columns = s[s].index.tolist()

        # applymap for columns that contains dicts (eg totals)
        s = (dataframe.applymap(type) == dict).all()
        dict_columns = s[s].index.tolist()

        for col in dict_columns + list_columns:
            columns_df = pd.json_normalize(dataframe[col]).add_prefix(f"{col}.")
            dataframe = pd.concat([dataframe, columns_df], axis=1).drop(columns=[col])
        return dataframe
