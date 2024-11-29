class EstimateRateOfCompletion:
    def __init__(self):
        # how many completions do we need to see before we can estimate the rate of completion?
        self.min_completion_timestamps = 10
        # how many completions do we want to use to estimate our rate at most
        self.max_completion_timestamps = 50
        # a log of the timestamp of each completion in order it occurred
        self.completion_timestamps = []
        self.prev_completion_count = None
        self.completion_timestamps = []

    def poll(self, state):
        completion_count = (
            state.get_failed_task_count() + state.get_successful_task_count()
        )
        if self.prev_completion_count is None:
            self.prev_completion_count = completion_count
        else:
            now = state.get_time()
            for _ in range(completion_count - self.prev_completion_count):
                self.completion_timestamps.append(now)
            # if we have too many samples, drop the oldest
            while len(self.completion_timestamps) > self.max_completion_timestamps:
                del self.completion_timestamps[0]
            self.prev_completion_count = completion_count

        self.completion_rate = None
        if len(self.completion_timestamps) > self.min_completion_timestamps:
            completion_window = state.get_time() - self.completion_timestamps[0]
            if completion_window > 0:
                # only compute the completion rate if the time over which we measured is non-zero
                # otherwise we get a division by zero
                self.completion_rate = (
                    len(self.completion_timestamps) / completion_window
                )
