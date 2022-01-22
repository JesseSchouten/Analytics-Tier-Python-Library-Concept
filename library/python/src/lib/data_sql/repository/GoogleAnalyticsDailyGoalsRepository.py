import time


class GoogleAnalyticsDailyGoalsRepository:
    def __init__(self, context):
        self.context = context

    def insert(self, data):
        dao = self.context['GoogleAnalyticsDailyGoalsDao']

        # Try 5 times.
        result = False
        for i in range(0, 5):
            try:
                result = dao.insert(data=data)
            except Exception as e:
                print("Inserts failed on attempt {} - {}".format(i, e))
                time.sleep(20)
                continue
            break
        return result
