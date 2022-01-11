from ADXCluster import ADXClusterUelPrdAdx

class ADXTableUelPrdAdx(ADXClusterUelPrdAdx):
    def __init__(self, spark):
        self.spark = spark

    def select(self, query, db_name, options={}):
        try:
            return f"read query {query} from database {db_name} using options {options}"
        except Exception as e:
            return None