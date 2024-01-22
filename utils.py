import os
from multiprocessing import current_process

class Logger:
    def __init__(self, path='data/logs.txt'):
        self.path = path
        self.reset()
        self.f = open(self.path, 'a')
        
    def write(self, msg, flush=None):
        self.f.write(msg.rstrip() + '\n')
        if (current_process().name != 'MainProcess') or flush:
            # we are in a child process, doing multiprocessing
            # so flush the log, to be psuedo-concurrency safe
            self.f.flush()
    def debug(self, msg):
        self.write(f"DEBUG: {msg}")
    def info(self, msg):
        self.write(f"INFO: {msg}")
    def warn(self, msg):
        self.write(f"WARNING: {msg}")
    def warning(self, msg):
        self.warn(msg)
    def error(self, msg):
        self.write(f"ERROR: {msg}")

    def close(self):
        self.f.close()
        
    # erase the file
    # but leave a blank file in place
    # to do that, open in `w` mode, and write nothing.
    def reset(self):
        with open(self.path, 'w'):
            pass

# on Linux and Mac this makes this python process lower priority
# so when it's running and using up all your CPU, your interface won't lag.
# So you can keep browsing the web, typing documents etc
try:
    os.nice(20)
except OSError: # ignore error, this is probably on Windows
    pass


# some cells will take a long time to run
# and uses lots of CPU
# If you want to free up CPU to use your laptop for something else,
# without losing progress,
# create this file next to this ipynb file
# and call check_for_pause() inside the code somewhere (e.g. at the start of each for loop iteration)
pause_path = 'pause.txt'
def check_for_pause():
    global logger
    if os.path.exists(pause_path):
        logger.info("paused")
        logger.close()
        while os.path.exists(pause_path):
            sleep(5)
        logger = Logger()
        logger.info("Resumed")

check_for_pause()