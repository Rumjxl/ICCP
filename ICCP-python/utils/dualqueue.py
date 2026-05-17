from collections import deque
import math
class dq:
    def __init__(self, size, default_max=None):
        self.size = size
        self.default_max = default_max if default_max is not None else 100  # 默认100Mbps
        self.dq = deque()
        self.dq_min = deque()
        self.dq_max = deque()
        self.average = 0
        self.length = 0

    def get_length(self):
        return self.length

    def add(self, entry):
        new_min = self.get_min()
        new_max = self.get_max()

        if entry < new_min:
            new_min = entry
        if entry > new_max:
            new_max = entry

        if self.length >= self.size:
            removed = self.dq.pop()
            self.average = (self.average * self.length - removed + entry) / self.length

            if removed == self.get_min():
                new_min = self.min()
                if entry < new_min:
                    new_min = entry
            self.dq_min.pop()
            self.dq_min.appendleft(new_min)
            if removed == self.get_max():
                new_max = self.max()
                if entry > new_max:
                    new_max = entry
            self.dq_max.pop()
            self.dq_max.appendleft(new_max)
            self.dq.appendleft(entry)
        else:
            self.average = (self.average * self.length + entry) / (self.length + 1)
            self.dq_min.appendleft(new_min)
            self.dq_max.appendleft(new_max)
            self.dq.appendleft(entry)
        self.length = min(self.length + 1, self.size)

    def get_min(self):
        return self.dq_min[0] if self.dq_min else float('inf')

    def get_max(self):
        return self.dq_max[0] if self.dq_max else float('-inf')

    def get_avg(self):
        return self.average

    def get_sum(self):
        return self.get_avg() * len(self.dq)

    def max(self):
        return max(self.dq) if self.dq else 0

    def min(self):
        return min(self.dq) if self.dq else float('inf')

    def sum(self):
        return sum(self.dq)

    def avg(self):
        return self.sum() / len(self.dq) if self.dq else 0

    def std(self):
        mean = self.avg()
        var = sum((x - mean) ** 2 for x in self.dq) / len(self.dq)
        return math.sqrt(var), mean