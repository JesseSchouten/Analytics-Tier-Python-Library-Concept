from pyspark.sql import DataFrame


class PaymentsReceivedService:
    def __init__(self, context):
        self.context = context
        self.table_name = "payments_received"
        self.columns =  ['company_account', 'merchant_account', 'psp_reference', 'merchant_reference',
                        'payment_method','creation_date','time_zone','currency','amount','type', 'risk_scoring',
                        'shopper_interaction','shopper_country', 'issuer_name','issuer_id','issuer_country','shopper_email',
                        'shopper_reference','3d_directory_response','3d_authentication_response','cvc2_response','avs_response',
                        'acquirer_response','raw_acquirer_response','authorisation_code','acquirer_reference','payment_method_variant'] 
 
        self.data_type = DataFrame

    def run_insert(self, data, batch_size=10000):
        if not isinstance(data, self.data_type):
            return False
        elif len(data.columns) != len(self.columns):
            return False

        repository = self.context['PaymentsReceivedRepository']

        return repository.insert(data, batch_size)