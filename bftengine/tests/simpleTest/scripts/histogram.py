import math
import io


class Histogram:
    kBucketLimit = [
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 18, 20, 25, 30, 35, 40, 45,
        50, 60, 70, 80, 90, 100, 120, 140, 160, 180, 200, 250, 300, 350, 400, 450,
        500, 600, 700, 800, 900, 1000, 1200, 1400, 1600, 1800, 2000, 2500, 3000,
        3500, 4000, 4500, 5000, 6000, 7000, 8000, 9000, 10000, 12000, 14000,
        16000, 18000, 20000, 25000, 30000, 35000, 40000, 45000, 50000, 60000,
        70000, 80000, 90000, 100000, 120000, 140000, 160000, 180000, 200000,
        250000, 300000, 350000, 400000, 450000, 500000, 600000, 700000, 800000,
        900000, 1000000, 1200000, 1400000, 1600000, 1800000, 2000000, 2500000,
        3000000, 3500000, 4000000, 4500000, 5000000, 6000000, 7000000, 8000000,
        9000000, 10000000, 12000000, 14000000, 16000000, 18000000, 20000000,
        25000000, 30000000, 35000000, 40000000, 45000000, 50000000, 60000000,
        70000000, 80000000, 90000000, 100000000, 120000000, 140000000, 160000000,
        180000000, 200000000, 250000000, 300000000, 350000000, 400000000,
        450000000, 500000000, 600000000, 700000000, 800000000, 900000000,
        1000000000, 1200000000, 1400000000, 1600000000, 1800000000, 2000000000,
        2500000000.0, 3000000000.0, 3500000000.0, 4000000000.0, 4500000000.0,
        5000000000.0, 6000000000.0, 7000000000.0, 8000000000.0, 9000000000.0,
        1e200]
    kNumBuckets = 154
    buckets_ = [0] * kNumBuckets

    def __init__(self):
        self.min_ = 0
        self.max_ = 0
        self.num_ = 0
        self.sum_ = 0
        self.sum_squares_ = 0
        self.kNumBuckets = 154

    def clear(self):
        self.min_ = self.kBucketLimit[self.kNumBuckets-1]
        self.max_ = 0
        self.num_ = 0
        self.sum_ =  0
        self.sum_squares_ = 0
        self.buckets_ = [0] * self.kNumBuckets

    def add(self, value):
        # Linear search is fast enough for our usage in db_bench
        b = 0
        while b < self.kNumBuckets - 1 and self.kBucketLimit[b] <= value:
            b += 1

        self.buckets_[b] += 1
        if self.min_ > value:
            self.min_ = value
        if self.max_ < value:
            self.max_ = value
        self.num_ += 1
        self.sum_ += value
        self.sum_squares_ += (value * value)

    def merge(self, other):
        if other.min_ < self.min_:
            self.min_ = other.min_
        if other.max_ > self.max_:
            self.max_ = other.max_
        self.num_ += other.num_
        self.sum_ += other.sum_
        self.sum_squares_ += other.sum_squares_
        for i in range(0 , self.kNumBuckets):
            self.buckets_[i] += other.buckets_[i]

    def percentile(self, p):
        threshold = self.num_ * (p / 100.0)
        tsum = 0
        for b in range(0, self.kNumBuckets):
            tsum += self.buckets_[b]
            if tsum >= threshold:
                # Scale linearly within this bucket
                left_point = 0 if (b == 0) else self.kBucketLimit[b-1]
                right_point = self.kBucketLimit[b]
                left_sum = tsum - self.buckets_[b]
                right_sum = tsum
                pos = (threshold - left_sum) / (right_sum - left_sum)
                r = left_point + (right_point - left_point) * pos
                if r < self.min_:
                    r = self.min_
                if r > self.max_:
                    r = self.max_
                return r
            return self.max_

    def median(self):
        return self.percentile(50.0)

    def average(self):
        if self.num_ == 0.0:
            return 0
        return self.sum_ / self.num_

    def standard_deviation(self):
        if self.num_ == 0.0:\
            return 0
        variance = (self.sum_squares_ * self.num_ - self.sum_ * self.sum_) / (self.num_ * self.num_)
        return math.sqrt(variance)

    def to_string(self):
        buf = io.StringIO()
        buf.write("Count: %.0f  Average: %.4f  StdDev: %.2f\n" %
                  (self.num_, self.average(), self.standard_deviation()))
        buf.write("Min: %.4f  Median: %.4f  Max: %.4f\n" %
                  (0.0 if self.num_ == 0 else self.min_, self.median(),
                   self.max_))
        buf.write("------------------------------------------------------\n")
        mult = 100.0 / self.num_
        tsum = 0
        for b in range(0, self.kNumBuckets):
            if self.buckets_[b] <= 0.0:
                continue

            tsum += self.buckets_[b]
            buf.write("[ %7.0f, %7.0f ) %7.0f %7.3f%% %7.3f%%" %
                     (0.0 if b == 0 else self.kBucketLimit[b-1], # left
                     self.kBucketLimit[b],                       # right
                     self.buckets_[b],                           # count
                     mult * self.buckets_[b],                    # percentage
                     mult * tsum))               # cumulative percentage

            # Add hash marks based on percentage 20 marks for 100%.
            marks = int(20 * (self.buckets_[b] / self.num_) + 0.5)
            buf.write('#' * marks)
            buf.write("\n")

        return buf.getvalue()


