import time


class CustomersRepository:
    def __init__(self, context):
        self.context = context

    def insert(self, data, batch_size=10000):
        dao = self.context['CustomersDao']

        # Try 5 times.
        result = False
        for i in range(0, 5):
            try:
                result = dao.insert(data=data, batch_size=batch_size)
            except Exception as e:
                print("Inserts failed on attempt {} - {}".format(i, e))
                time.sleep(20)
                continue
            break
        return result