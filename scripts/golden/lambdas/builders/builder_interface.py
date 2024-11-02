from ABC import ABC, abstractmethod
import boto3

class BuilderInterface(ABC):

    @abstractmethod
    def load_data(self, source_bucket, source_key):
        pass

    @abstractmethod
    def transform_data(self):
        pass

    def extract_data(self, source_bucket, source_key):
        """Leo un archivo de s3 y lo devuelvo"""
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=source_bucket, Key=source_key)
        return obj['Body'].read().decode('utf-8')
