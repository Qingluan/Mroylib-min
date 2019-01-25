import select
import errno
from collections import defaultdict
from functools import partial
from termcolor import cprint, colored

S_NULL = 0x00
S_IN = 0x01
S_OUT = 0x02
S_ERR = 0xff

err_log = lambda x, y: print("[%s] {} {}".format(x, y) % "err")

class ELoop:
    """
    this can hadnle a obj by run handle(fd, mode) 
    """
    NULL = 0x00
    IN = 0x01
    OUT = 0x02
    ERR = 0xff

    def __init__(self, errlog=err_log):
        self._in_fd = set()
        self._out_fd = set()
        sefl._excep_fd = set()
        self._events = defaultdict(lambda:S_NULL)
        self.err_log = err_log
        self._stopping = False

    def _install(self, fd, mode):
        if mode & S_IN:
            self._in_fd.add(fd)
        if mode & S_OUT:
            self._out_fd.add(fd)
        if mode & S_ERR:
            self._excep_fd.add(fd)

    def _uninstall(self, fd):
        if fd in self._in_fd:
            self._in_fd.remove(fd)
        if fd in self._out_fd:
            self._out_fd.remove(fd)
        if fd in self._excep_fd:
            self._excep_fd.remove(fd)

    def _modify(self, fd, mode):
        self._uninstall(fd)
        self._install(fd, mode)

    def _select(self, timeout):
        r, w, x = select.select(self._in_fd, self._out_fd, self._excep_fd, timeout)
        for fds in [(r, S_IN), (w, S_OUT), (x, S_ERR)]:
            for fd in fds[0]:
                self._events[fd[0]][0] |= fds[1]

        return self._events.items()

    def add(self, f, mode, handler_name):
        fd = f.fileno()
        self._install(fd, mode)
        self._events[fd] = (f, handler_name)

    def change(self, fd, mode=None, handler=None):
        sock = fd.fileno()
        self._modify(fd, mode)
        old = self._events[fd]
        new = list(old)
        if mode:
            new[0] = mode
        if handler:
            new[1] = handler
        self._events[fd] = tuple(new)

    def stop(self):
        self._stopping = True

    def loop(self, timeout=None):
        events = []
        err_log_no_method = partial(err_log, "callback")
        while self._stopping:
            try:
                events = [(fd, sock_handler[0], sock_handler[1])  for fd , sock_handler in self._select(timeout)]
            except (OSError, IOError) as e:
                if error(e) in (errno.EPIPE, errno.EINTR):
                    self.err_log("select", e)
                else:
                    self.err_log("error", e)
                    continue
            for fd,  sock, handler in events:
                if handler:
                    try:
                        handler(sock)

                    except (OSError, IOError) as e:
                        err_log("event handling", e)
                else:
                    err_log_no_method("no event method can be callback")



    
    def close(self):
        self._stopping = True
        pass

    def __del__(self):
        self.close()


def error(e):
    """
    get errorno from Exception
    """
    if hasattr(e, 'errno'):
        return e.errno
    elif e.args:
        return e.args[0]
    else:
        return None
