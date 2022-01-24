from pyspark.sql import SparkSession


class BlobContainer():
    def __init__(self, context):
        super().__init__()
        self.context = context

    def select_from_csv(
            self,
            container_name,
            file_path,
            options={},
            spark=None):
        dc_internal_sa = self.context['StorageAccountDCInternal']

        file = "wasbs://{}@{}.blob.core.windows.net/{}".format(
            container_name, dc_internal_sa.name, file_path)
        try:
            print(f"read file {file} using options {options}")
            return True
        except Exception as e:
            return False
