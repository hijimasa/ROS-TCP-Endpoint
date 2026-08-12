import threading

class ThreadPauser:
    def __init__(self):
        self.condition = threading.Condition()
        self.result = None
        self.resumed = False

    def sleep_until_resumed(self, timeout=None):
        # wait_for tolerates resume_with_result having run before we got here;
        # a bare wait() would miss that notify and sleep forever.
        with self.condition:
            self.condition.wait_for(lambda: self.resumed, timeout)
            return self.resumed

    def resume_with_result(self, result):
        with self.condition:
            self.result = result
            self.resumed = True
            self.condition.notify()
