import datetime
import random


class Interval:
    def __init__(self, start, end):
        self.start_time = start
        self.end_time = end

    @staticmethod
    def pick_random_time_interval(start, end, duration):
        assert end > start
        total_duration = end - start - duration
        random_seconds = random.randint(0, total_duration.total_seconds())
        random_start = start + datetime.timedelta(seconds=random_seconds)
        random_end = random_start + duration
        # Check that the selected interval is contained within start and end
        assert start <= random_start <= random_end <= end
        return Interval(random_start, random_end)

    def start(self):
        return self.start_time.isoformat() + 'Z'

    def end(self):
        return self.end_time.isoformat() + 'Z'

    def __str__(self):
        return f'Interval({self.start()}, {self.end()})'


if __name__ == '__main__':
    start = datetime.datetime(2014, 1, 1)
    end = datetime.datetime(2021, 1, 1)
    duration = datetime.timedelta(seconds=30)
    print(Interval.pick_random_time_interval())
