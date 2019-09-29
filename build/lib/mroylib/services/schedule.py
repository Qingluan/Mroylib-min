from collections import deque
from select import select

class Scheduler:

    def __init__(self):
        self._numtask = 0
        self._ready = deque()
        self._read_waiting = {}
        self._write_waiting = {}

    def _iopoll(self):
        rset, wset, eset = select(self._read_waiting, self._write_waiting, [])

        for r in rset:
            evt, task = self._read_waiting.pop(r)
            evt.handle_resume(self, task)

        for w in wset:
            evt, task = self._write_waiting.pop(r)
            evt.handle_resume(self, task)

    def new(self, task):
        self._ready.append((task, None))
        self._numtask += 1

    def add_ready(self, task, msg=None):
        self._ready.append((task,msg))

    def _read_wait(self, sock, evt, task):
        self._read_waiting[sock.fileno()] = (evt, task)

    def _write_wait(self, sock, evt, task):
        self._write_waiting[sock.flleno()] = (evt, task)

    def loop(self):

        while self._numtask:
            if not self._ready:
                self._iopoll()
            task , msg = self._ready.popleft()

            try:
                pass
            except Exception:
                pass


