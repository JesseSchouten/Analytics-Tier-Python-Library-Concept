from pyspark.sql import SparkSession

from StorageAccount import StorageAccountDCInternal

class BlobContainerDCInternal(StorageAccountDCInternal):
    def __init__(self, spark):
        super().__init__()
        self.spark = spark

    def select_from_csv(self, container_name, file_path, options={}):
        file = "wasbs://{}@{}.blob.core.windows.net/{}".format(container_name, self.name, file_path)
        try:
            return f"read file {file} using options {options}"
        except Exception as e:
            return None
