import pandas as pd


class CSVLoader:

    def __init__(self, chunk_size=50000):
        self.chunk_size = chunk_size

    def load(self, file_path, columns):
        return pd.read_csv(
            file_path,
            sep=';',
            encoding='latin1',
            names=columns,
            header=None,
            dtype=str,
            chunksize=self.chunk_size,
            low_memory=False
        )