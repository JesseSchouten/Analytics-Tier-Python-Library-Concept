class CatalogService:
    def __init__(self, context):
        self.context = context
        self.table_name = "catalog"
        self.columns = ['id', 'asset_type', 'asset_subtype', 'channel', 'title', 'genre', 'tags']
        self.data_type = list

    def run_insert(self, data, batch_size=10000):
        if not isinstance(data, self.data_type):
            return False
        elif len(data[0]) != len(self.columns):
            return False
        repository = self.context['CatalogRepository']

        return repository.insert(data, batch_size)
