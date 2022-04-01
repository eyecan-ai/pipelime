import os
import time
import errno


class Lockfile(object):
    """A file locking mechanism that has context-manager support so
    you can use it in a with statement. This should be relatively cross
    compatible as it doesn't rely on msvcrt or fcntl for the locking.
    """

    def __init__(self, lockfile, timeout=-1, delay=0.05):
        """Prepare the file locker. Specify the file to lock and optionally
        the maximum timeout and the delay between each attempt to lock.
        """
        self._is_locked = False
        self._lockfile = lockfile
        self._timeout = timeout
        self._delay = delay

    def acquire(self):
        """Acquire the lock, if possible. If the lock is in use, it check again
        every `wait` seconds. It does this until it either gets the lock or
        exceeds `timeout` number of seconds, in which case it throws
        an exception.
        """
        start_time = time.time()
        while not self._is_locked:
            try:
                # Open file exclusively
                self.fd = os.open(self.lockfile, os.O_CREAT | os.O_EXCL | os.O_RDWR)
                self._is_locked = True
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise
                if self._timeout > 0 and (time.time() - start_time) >= self._timeout:
                    raise IOError("Timeout occured.")
                time.sleep(self._delay)

    def release(self):
        """Get rid of the lock by deleting the lockfile.
        When working in a `with` statement, this gets automatically
        called at the end.
        """
        # Close files, delete files
        if self._is_locked:
            os.close(self.fd)
            os.unlink(self.lockfile)
            self._is_locked = False

    def __enter__(self):
        """Activated when used in the with statement.
        Should automatically acquire a lock to be used in the with block.
        """
        if not self._is_locked:
            self.acquire()
        return self

    def __exit__(self, type, value, traceback):
        """Activated at the end of the with statement.
        It automatically releases the lock if it isn't locked.
        """
        if self._is_locked:
            self.release()

    def __del__(self):
        """Make sure that the FileLock instance doesn't leave a lockfile
        lying around.
        """
        self.release()
