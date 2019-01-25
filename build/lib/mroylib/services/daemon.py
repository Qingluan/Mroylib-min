import os
import sys
import atexit
import signal

from functools import partial
from termcolor import cprint, colored

def daemonize(pidfile, *, stdin='/dev/null',
              stdout='/dev/null',
              stderr='/dev/null'):
    if os.path.exists(pidfile):
            raise RuntimeError('Already running')
    # First fork (detaches from parent)
    try:
        if os.fork() > 0:
            raise SystemExit(0)
    except OSError as e:
        raise RuntimeError('fork #1 failed.')
    os.chdir('/')
    os.umask(0)
    os.setsid()
    # Second fork (relinquish,session leadership)
    try:
        if os.fork() > 0:
            raise SystemExit(0)
    except OSError as e:
        raise RuntimeError('fork #2 failed.')
    # Flush I/O buffers
    sys.stdout.flush()
    sys.stderr.flush()
    # Replace file descriptors for stdin, stdout, and stderr
    with open(stdin, 'rb', 0) as f:
        os.dup2(f.fileno(), sys.stdin.fileno())
    with open(stdout, 'ab', 0) as f:
        os.dup2(f.fileno(), sys.stdout.fileno())
    with open(stderr, 'ab', 0) as f:
        os.dup2(f.fileno(), sys.stderr.fileno())
        # Write the PID file
    with open(pidfile, 'w') as f:
        print(os.getpid(), file=f)
    # Arrange to have the PID file removed on exit/signal
    atexit.register(lambda: os.remove(pidfile))
    # Signal handler for termination(required)

    def sigterm_handler(signo, frame):
        raise SystemExit(1)
    signal.signal(signal.SIGTERM, sigterm_handler)


def test(): 
    import time
    sys.stdout.write('Daemon started with pid {}\n'.format(os.getpid()))
    while True:
        sys.stdout.write('Daemon Alive! {}\n'.format(time.ctime()))
        time.sleep(10)


def run(func):
    ru_f = '/usr/local/var/run/'
    lg_f = '/usr/local/var/log/'
    if  not os.path.exists(ru_f):
        ru_f = '/var/run/'
    if not os.path.exists(lg_f):
        lg_f = '/var/log/'

    PID_F = os.path.join(ru_f, func.__name__ + ".pid")
    LOG_F = os.path.join(lg_f, func.__name__ + ".log")
    ERROR_F = os.path.join(lg_f, func.__name__ + ".error.log")
    try:
        print(func.__name__, colored("[start]", "green", attrs=['blink']))
        daemonize(PID_F, stdout=LOG_F, stderr=ERROR_F)
    except RuntimeError as e:
        cprint(e, "red", file=sys.stderr)
        raise SystemExit(1)
    func()


def restart(func):
    print("--- restart ---")
    stop(func)
    print("--- stop %s wait to starting ---" % func.__name__)
    run(func)
    print("--- start %s ---" % func.__name__)


def stop(func):
    ru_f = '/usr/local/var/run/'
    lg_f = '/usr/local/var/log/'
    if  not os.path.exists(ru_f):
        ru_f = '/var/run/'
    if not os.path.exists(lg_f):
        lg_f = '/var/log/'

    PID_F = os.path.join(ru_f, func.__name__ + ".pid")
    LOG_F = os.path.join(lg_f, func.__name__ + ".normal.log")
    for i in [PID_F]:
        if os.path.exists(i):
            with open(i) as f:
                os.kill(int(f.read()), signal.SIGTERM)
        else:
            print("Not running : ", i, file=sys.stderr)
    from time import asctime
    with open(LOG_F, "a+") as fp:
        print(asctime() ," -- stop --", file=fp)
        cprint("-- stop --", "blue")
    raise SystemExit(1)
