class UserError(Exception):
    def __init__(self, message):
        self.message = message


class NoWorkersRunning(Exception):
    pass
