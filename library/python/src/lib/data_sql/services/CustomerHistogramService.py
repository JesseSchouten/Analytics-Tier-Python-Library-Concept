from pyspark.sql import DataFrame


class CustomerHistogramService:
    def __init__(self, context):
        self.context = context
        self.table_name = "customer_histogram"
        self.columns = ['channel', 'cohort', 'age', 'recurring', 'cancelled', 'total', 'older_than', 'churn']
        self.data_type = DataFrame

    def run_insert(self, data, batch_size=10000):
        if not isinstance(data, self.data_type):
            return False
        elif len(data.columns) != len(self.columns):
            return False

        repository = self.context['CustomerHistogramRepository']

        return repository.insert(data, batch_size)
