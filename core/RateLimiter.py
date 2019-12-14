from datetime import datetime

class RateLimiter:

    def __init__(self, rps):
        self.rps = rps
        self.count = 0
        self.time = self._get_time(datetime.now())

    def is_rate_limited(self):
        now = self._get_time(datetime.now())
        if now != self.time:
            self.time = now
            self.count = 0

        result = self.count >= self.rps
        self.count += 1
        return result

    def _get_time(self, now):
        return str(now.minute) + ':' + str(now.second)


if __name__ == '__main__':
    rl = RateLimiter(5)

    passed = 0
    failed = 0
    for _ in range(1000000):
        val = rl.is_rate_limited()
        if val is True:
            failed += 1
        else:
            passed += 1

    print('passed: {}, failed: {}'.format(passed, failed))