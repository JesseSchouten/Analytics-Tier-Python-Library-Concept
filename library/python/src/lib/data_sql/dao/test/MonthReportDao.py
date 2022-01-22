class MonthReportDao:
    def __init__(self, context):
        self.context = context

    def insert(self, *args, **kwargs):
        return True