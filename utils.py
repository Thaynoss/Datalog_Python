import threading, collections, parserDatalog

LAMBDA = lambda: None

class DatalogError(Exception):
    def __init__(self, value, lineno, function):
        self.value = value
        self.lineno = lineno
        self.function = function
    def __str__(self):
        return "%s\nin line %s of %s" % (self.value, self.lineno, self.function)        


class Counter(object):
    lock = threading.RLock()
    def __init__(self):
        self.i = 0

    def __iter__(self):
        return self

    def next(self):
        with Counter.lock:
            self.i += 1
            return self.i
    

class Logic(object):

    tl = threading.local()  # contains the Logic in the current thread
    def __new__(cls, logic=None):
        if isinstance(logic, cls):
            parserDatalog.clear()
            Logic.tl.logic = copy.copy(logic) 
            Logic.tl.logic.Subgoals = {} # for memoization of subgoals (tabled resolution)
            Logic.tl.logic.Tasks = None # LIFO stack of tasks
            Logic.tl.logic.Recursive_Tasks = None # FIFO queue of tasks for recursive clauses
            Logic.tl.logic.Recursive = False # True -> process Recursive_tasks. Otherwise, process Tasks
            Logic.tl.logic.Goal = None
            Logic.tl.logic.gc_uncollected = False # did we run gc.collect() yet ?

        elif not (logic) or not hasattr(Logic.tl, 'logic'):
            Logic.tl.logic = object.__new__(cls)
        return Logic.tl.logic
    
    def __init__(self, logic=None):
        if not (logic) or not (hasattr(self, 'Db')):
            parserDatalog.clear()

            
    def clear(self):
        """ move the logic to the current thread and clears it """
        Logic(self)  # just to be sure
