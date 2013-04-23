import sys
import subprocess
import signal
from common import get_executable

class Executable(object):
    '''Parent object that represents an executable for use in task-sets.'''

    def __init__(self, exec_file, extra_args=None, stdout_file = None,
                 stderr_file = None, cwd = None):
        self.exec_file = get_executable(exec_file)
        self.cwd = cwd
        self.stdout_file = stdout_file
        self.stderr_file = stderr_file
        self.sp = None

        if extra_args is None:
            self.extra_args = None
        else:
            self.extra_args = [str(a) for a in list(extra_args)] # make a duplicate

        if not self.exec_file:
            raise Exception("Not executable ? : %s" % self.exec_file)

    def __del__(self):
        # Try and clean up
        if self.stdout_file is not None:
            self.stdout_file.close()
        if self.stderr_file is not None:
            self.stderr_file.close()

        if self.sp is not None:
            try:
                self.sp.terminate()
            except OSError as e:
                if e.errno == 3:
                    pass # no such process (already killed), okay
                else:
                    raise e

    def __get_full_command(self):
        full_command = [self.exec_file]
        if self.extra_args is not None:
            full_command += self.extra_args
        return full_command

    def __str__(self):
        return " ".join(self.__get_full_command())

    def execute(self):
        '''Execute the binary.'''
        full_command = self.__get_full_command()
        self.sp = subprocess.Popen(full_command, stdout=self.stdout_file,
                stderr=self.stderr_file, cwd=self.cwd)

    def kill(self):
        self.sp.kill()

    def interrupt(self):
        self.sp.send_signal(signal.SIGINT)

    def poll(self):
        return self.sp.poll()

    def terminate(self):
        '''Send the terminate signal to the binary.'''
        self.sp.terminate()

    def wait(self, error=True):
        '''Wait until the executable is finished, checking return code.

        If the exit status is non-zero, raise an exception.

        '''

        self.sp.wait()
        if self.sp.returncode != 0 and error:
            print >>sys.stderr, "Non-zero return %d: %s %s" % (self.sp.returncode,
                                                               self.exec_file,
                                                               " ".join(self.extra_args))
            return 0
        else:
            return 1
