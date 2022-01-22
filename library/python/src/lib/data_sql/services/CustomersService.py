from pyspark.sql import DataFrame

class CustomersService:
    def __init__(self, context):
        self.context = context
        self.table_name = "customers"
        self.columns = ['customer_id',
                        'channel',
                        'country',
                        'first_name',
                        'last_name',
                        'registration_country',
                        'create_date',
                        'activation_date',
                        'last_login',
                        'facebook_id',
                        'email',
                        'first_payment_date']
        self.data_type = DataFrame

    def run_insert(self, data, batch_size=10000):
        if not isinstance(data, self.data_type):
            return False
        elif len(data.columns) != len(self.columns):
            return False

        repository = self.context['CustomersRepository']

        return repository.insert(data, batch_size)