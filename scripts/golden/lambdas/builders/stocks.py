from builder_interface import BuilderInterface
from io import StringIO
from pandas import read_csv


class Stocks(BuilderInterface):
    
    def __init__(self, source_bucket, source_key):
        self.source_bucket = source_bucket
        self.source_key = source_key

    def __call__(self):
        object_content = self.extract_data(self.source_bucket, self.source_key)
        df = read_csv(StringIO(object_content))
        print(df.head())

