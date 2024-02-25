# This script contains custom functions which we want to re-use across all our .ipynb playbooks
# It's a python library.
# But you don't need to pip install anything. Just make sure that you open Jupyter lab from the directory that contains this script.
import os
from multiprocessing import current_process
from itertools import islice
import traceback
from random import shuffle

class Logger:
    def __init__(self, path='data/logs.txt', flush=False):
        create_dir(file=path)
        self.path = path
        self.f = open(self.path, 'a')
        self._flush = flush
        
    def write(self, msg, flush=None):
        self.f.write(msg.rstrip() + '\n')
        if (current_process().name != 'MainProcess') or flush or self._flush:
            # we are in a child process, doing multiprocessing
            # so flush the log, to be psuedo-concurrency safe
            self.flush()
    def flush(self):
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
    def exception(self, ex):
        # this just prints the most recent one
        traceback.print_exc(file=self.f)
        
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
def renice():
    try:
        os.nice(19)
    except (AttributeError, OSError) as e: # on Windows
        print(f"Unable to change process niceness {e}. Continuing anyway.")
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


# https://docs.python.org/3/library/itertools.html#itertools.batched
# added to standard library in 3.12
def batched(iterable, n):
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError('n must be at least one')
    it = iter(iterable)
    while batch := tuple(islice(it, n)):
        yield batch

# create a directory
# (do nothing if it exists)
# file argument is to create a directory that will contain that file
# choose exactly one argument
def create_dir(dir=None, file=None):
    if dir and file:
        raise ValueError(f"Must specify either dir or file")
    if file:
        dir = os.path.dirname(file)
    if not os.path.exists(dir):
        try:
            os.makedirs(dir)
        except FileExistsError:
            # race condition when multiprocessing
            pass


# list files in a directory, recursively
# returning an iterable of one full path at a time
# if randomised=True, shuffle the list of files
def walk(dir, randomised=False):
    if randomised:
        # need to physicalise into a list
        # and mutate
        paths = list(walk(dir, randomised=False))
        assert isinstance(paths, list)
        shuffle(paths)
        # don't mix yield and return in one function
        for p in paths:
            yield p
    else:
        for (dir,subdirs,files) in os.walk(dir):
            for file in files:
                yield os.path.join(dir, file)


# when multiprocessing, how many CPUs to use?
# leave_space=True to True to leave one unused CPU when multiprocessing
# so that you can still do other stuff on your laptop without your internet browser or whatever being laggy
# *2 because we assumy hyperthreading
def num_cpu(leave_spare=True):
    
    if leave_spare:
        num_processes = os.cpu_count() - 2
    else:
        num_processes = os.cpu_count()
    return num_processes
