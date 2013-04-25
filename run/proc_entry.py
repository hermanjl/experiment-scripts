import os
import traceback

class ProcEntry(object):
    def __init__(self, proc, data):
        self.proc = proc
        self.data = str(data)

        if not os.path.exists(self.proc):
            raise ValueError("Invalid proc entry %s" % self.proc)

    def write_proc(self):
        try:
            with open(self.proc, 'w') as entry:
                entry.write(self.data)
        except:
            traceback.print_exc()

            val = str(self.data)
            val = val if '\n' not in val else '\n'+val

            raise IOError("Failed to write into %s value: %s" %
                          (self.proc, val))
