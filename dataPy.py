from collections import defaultdict
import inspect
import string
import weakref

from . import Logic
from . import pyEngine
from . import parserDatalog
from . import util

     

    

""" ****************** direct access to datalog knowledge base ***************** """
DatalogError= util.DatalogError

def assert_fact(predicate_name, *args):
    """ assert predicate_name(args) """
    + parserDatalog.Literal.make(predicate_name, [parserDatalog.Expression._pyD_for(arg) for arg in args])

def retract_fact(predicate_name, *args):
    """ retracts predicate_name(args) """
    - parserDatalog.Literal.make(predicate_name, [parserDatalog.Expression._pyD_for(arg) for arg in args])

def program():
    """ A decorator for datalog program  """
    return parserDatalog.add_program

def predicate():
    """decorator function to create a predicate resolver in python"""
    return _predicate 

def _predicate(func):
    arity = len(inspect.getargspec(func)[0])
    pyEngine.Python_resolvers[func.__name__ + '/' + str(arity)] = func
    return func

def load(code):
    """loads the clauses contained in the code string """
    stack = inspect.stack()
    newglobals = {}
    for key, value in stack[1][0].f_globals.items():
        if hasattr(value, '_pyD_atomized'):
            newglobals[key] = value
    return parserDatalog.load(code, newglobals=newglobals)

def ask(code):
    """returns the result of the query contained in the code string"""
    return parserDatalog.ask(code)

def clear():
    """ resets the default datalog database """
    parserDatalog.clear()
    Logic()

class Classe(object):
    def __init__(self, cls):
        self.cls = cls
    def __call__(self, *arguments, **keyargs):
        for a in arguments: 
            if isinstance(a, parserDatalog.Expression):
                assert not keyargs, "Sorry, key word arguments are not supported yet" #TODO
                return parserDatalog.Operation(self.cls, '(', arguments) 
        return self.cls(*arguments, **keyargs)
        

def _pyD_decorator(arg):
    if hasattr(arg, '_pyD_atomized'): 
        return arg
    atomized = arg
    if inspect.isclass(arg):
        atomized = Classe(arg)
        for c in arg.__mro__[-2::-1]: # reverse order, ignoring object class
            for a in c.__dict__:
                try:
                    new_f = _pyD_decorator(getattr(arg, a))
                    atomized.__dict__[a] = new_f
                except AttributeError:
                    pass # sometimes raised by pypy, e.g. for time
    elif inspect.ismodule(arg):
        for a in arg.__dict__:
            new_f = _pyD_decorator(getattr(arg, a))
            setattr(arg, a, new_f)
    elif hasattr(arg, '__call__'): # it's a function
        if inspect.isgeneratorfunction(arg):
            #TODO support atomized generator functions
            atomized = arg
        else:
            def atomized(*arguments, **keyargs):
                # if any argument is an Expression, return an Operation
                # else immediately evaluate the function
                # TODO give it arg's name ?
                for a in arguments: 
                    if isinstance(a, parserDatalog.Expression):
                        assert not keyargs, "Sorry, key word arguments are not supported yet" #TODO
                        return parserDatalog.Operation(arg, '(', arguments) 
                return arg(*arguments, **keyargs)
        
            try: # copy __doc__
                atomized.__dict__.update(arg.__dict__)
            except:
                pass
    try:
        setattr(atomized, '_pyD_atomized', True)
    except:
        pass
    return atomized

ATOMS = ['_sum','sum_','_min','min_','_max','max_', '_len','len_','concat','concat_','rank','rank_',
         'running_sum','running_sum_','range_','tuple_', 'format_', 'mean_', 'linear_regression_']

def create_terms(*args):
    """ create terms for in-line clauses and queries """
    stack = inspect.stack()
    try:
        locals_ = stack[1][0].f_locals
        args = [arg.strip() for arglist in args for arg in 
                (arglist.split(',') if isinstance(arglist, util.string_types) else [arglist])]
        for arg in set(args + ATOMS):
            assert isinstance(arg, util.string_types)
            words = arg.split('.')
            if 2<len(words): #TODO deal with more
                    raise util.DatalogError("Too many '.' in atom %s" % arg, None, None)
            b = __builtins__ if isinstance(__builtins__, dict) else __builtins__.__dict__ # for pypy
            if words[0] in b: # if it 's a builtin
                root = b[words[0]]
                locals_[words[0]] = _pyD_decorator(root) 
            elif words[0] in locals_:
                root = locals_[words[0]]
                if len(words)==2: # e.g. str.split
                    atom = getattr(root, words[1])
                    setattr(root, words[1], _pyD_decorator(atom))
                else: # e.g. math
                    locals_[arg] = _pyD_decorator(root)
            else:
                if len(words)==2: # e.g. kkecxivarenx.len
                    raise util.DatalogError("Unknown variable : %s" % words[0], None, None)
                locals_[arg] = parserDatalog.Term(arg)
    finally:
        del stack

create_atoms = create_terms # for backward compatibility

def variables(n):
    """ create variables for in-line clauses and queries """
    return [parserDatalog.Term('??') for i in range(n)]

Variable = parserDatalog.Term
Answer = parserDatalog.Answer


""" ****************** python Mixin ***************** """

class metaMixin(type):
    """Metaclass used to define the behavior of a subclass of Mixin"""
    __refs__ = defaultdict(weakref.WeakSet)
    
    def __init__(cls, name, bases, dct):
        """when creating a subclass of Mixin, save the subclass in Class_dict. """
        super(metaMixin, cls).__init__(name, bases, dct)
        pyEngine.add_class(cls, name)
        cls.has_SQLAlchemy = any(base.__module__ in ('sqlalchemy.ext.declarative', 
                            'sqlalchemy.ext.declarative.api') for base in bases)
        
        def _getattr(self, attribute):
            """ responds to instance.method by asking datalog engine """
            if not attribute == '__iter__' and not attribute.startswith('_sa_'):
                predicate_name = "%s.%s[1]==" % (self.__class__.__name__, attribute)
                terms = (parserDatalog.Term('_pyD_class', forced_type='constant'), self, parserDatalog.Term("X")) #prefixed
                literal = parserDatalog.Literal.make(predicate_name, terms) #TODO predicate_name[:-2]
                result = literal.lua.ask()
                return result[0][-1] if result else None                    
            raise AttributeError
        cls.__getattr__ = _getattr   

        def __lt__(self, other): # needed for sorting in aggregate functions using Python 3
            return id(self) < id(other)
        cls.__lt__ = __lt__    
    
    def __getattr__(cls, method):
        """
        when access to an attribute of a subclass of Mixin fails, 
        return an object that responds to () and to [] 
        """
        if cls in ('Mixin', 'metaMixin') or method in (
                '__mapper_cls__', '_decl_class_registry', '__sa_instrumentation_manager__', 
                '_sa_instance_state', '_sa_decl_prepare', '__table_cls__', '_pyD_query'):
            raise AttributeError
        return parserDatalog.Term("%s.%s" % (cls.__name__, method))

    def pyDatalog_search(cls, literal):
        """Called by pyEngine to resolve a prefixed literal for a subclass of Mixin."""
        terms = literal.terms
        attr_name = literal.pred.suffix
        operator = literal.pred.name.split(']')[1] # what's after ']' or None

        def check_attribute(X):
            if attr_name not in X.__dict__ and attr_name not in cls.__dict__:
                raise AttributeError("%s does not have %s attribute" % (cls.__name__, attr_name))

        if len(terms)==3: #prefixed
            X, Y = terms[1], terms[2]
            if X.is_const():
                # try accessing the attribute of the first term in literal
                check_attribute(X.id)
                Y1 = getattr(X.id, attr_name)
                if not Y.is_const() or not operator or pyEngine.compare(Y1,operator,Y.id):
                    yield (terms[0], X.id, Y.id if Y.is_const() else Y1 if operator=='==' else None)
            elif cls.has_SQLAlchemy:
                if cls.session:
                    q = cls.session.query(cls)
                    check_attribute(cls)
                    if Y.is_const():
                        q = q.filter(pyEngine.compare(getattr(cls, attr_name), operator, Y.id))
                    for r in q:
                        Y1 = getattr(r, attr_name)
                        if not Y.is_const() or not operator or pyEngine.compare(Y1,operator,Y.id):
                                yield (terms[0], r, Y.id if Y.is_const() else Y1 if operator=='==' else None)
            else:
                # python object with Mixin
                for X in metaMixin.__refs__[cls]:
                    check_attribute(X)
                    Y1 = getattr(X, attr_name)
                    if not Y.is_const() or not operator or pyEngine.compare(Y1,operator,Y.id):
                        yield (terms[0], X, Y.id if Y.is_const() else Y1 if operator=='==' else None)
            return
        else:
            raise AttributeError ("%s could not be resolved" % literal.pred.name)

# following syntax to declare Mixin is used for compatibility with python 2 and 3
Mixin = metaMixin('Mixin', (object,), {})

#When creating a Mixin object without SQLAlchemy, add it to the list of instances,
#so that it can be included in the result of queries

def __init__(self):
    if not self.__class__.has_SQLAlchemy:
        for cls in self.__class__.__mro__:
            if cls.__name__ in pyEngine.Class_dict and cls not in (Mixin, object):
                metaMixin.__refs__[cls].add(self)
Mixin.__init__ = __init__