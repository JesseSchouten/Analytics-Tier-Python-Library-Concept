from StorageAccount import StorageAccount


def main(environment='test', dbutils=None, mock=None):
    context = {}
    if environment == 'test':
        context['StorageAccountDCInternal'] = StorageAccount(
            dbutils, name='dcinternal', key_name='dcinternal-key-name')
    return context
