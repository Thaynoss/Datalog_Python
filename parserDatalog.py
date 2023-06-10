from collections import OrderedDict
import inspect
import re
import string
import sys
import threading


from . import utils

# global variable to differentiate between in-line queries and pyDatalog program / ask
Thread_storage = threading.local()
Thread_storage.variables = set([]) #call list of variables parsed since the last clause

def clear():
    Thread_storage.variables = set([])
    def sort(self):
        if self.data is not True: 
            self.data.sort()
        return self
    
    def __str__(self):
        return utils.cast_to_str(self.__unicode__())

class Expression(object):
    """ base class for objects that can be part of an inequality, operation or slice """
    @classmethod
    def _pyD_for(cls, operand):
        """ factory that converts an operand to an Expression """
        import aggregate
        if isinstance(operand, (Expression, aggregate)):
            return operand
        if isinstance(operand, slice):
            return Term([operand.start, operand.stop, operand.step])
        return Term(operand, forced_type="constant")
    
    def is_variable(self):
        return False
    
    # handlers of inequality and operations
    def __eq__(self, other):
        if isinstance(self, Operation) and self._pyD_operator in '+-' and self._pyD_lhs._pyD_value == 0:
            raise utils.DatalogError("Did you mean to assert or retract a fact ? Please add parenthesis.", None, None)
        return Literal.make_for_comparison(self, "==", other)
    def __ne__(self, other):
        return Literal.make_for_comparison(self, '!=', other)
    def __le__(self, other):
        return Literal.make_for_comparison(self, '<=', other)
    def __lt__(self, other):
        return Literal.make_for_comparison(self, '<', other)
    def __ge__(self, other):
        return Literal.make_for_comparison(self, '>=', other)
    def __gt__(self, other):
        return Literal.make_for_comparison(self, '>', other)
    def in_(self, values):
        """ called when evaluating (X in (1,2)) """
        return Literal.make_for_comparison(self, '_pyD_in', values)
    _in = in_ # for backward compatibility
    def not_in_(self, values):
        """ called when evaluating (X not in (1,2)) """
        return Literal.make_for_comparison(self, '_pyD_not_in', values)
    _not_in = not_in_  # for backward compatibility
    
    def __pos__(self):
        """ called when evaluating -X """
        return 0 + self
    def __neg__(self):
        """ called when evaluating -X """
        return 0 - self

    def __add__(self, other):
        return Operation(self, '+', other)
    def __sub__(self, other):
        return Operation(self, '-', other)
    def __mul__(self, other):
        return Operation(self, '*', other)
    def __div__(self, other):
        return Operation(self, '/', other)
    def __truediv__(self, other):
        return Operation(self, '/', other)
    def __floordiv__(self, other):
        return Operation(self, '//', other)
    def __pow__(self, other):
        return Operation(self, '**', other)
    
    # called by constant + Term (or lambda + symbol)
    def __radd__(self, other):
        return Operation(other, '+', self)
    def __rsub__(self, other):
        return Operation(other, '-', self)
    def __rmul__(self, other):
        return Operation(other, '*', self)
    def __rdiv__(self, other):
        return Operation(other, '/', self)
    def __rtruediv__(self, other):
        return Operation(other, '/', self)
    def __rfloordiv__(self, other):
        return Operation(other, '//', self)
    def __rpow__(self, other):
        return Operation(other, '**', self)

    def __getitem__(self, keys):
        """ called when evaluating expression[keys] """
        if isinstance(keys, slice):
            return Operation(self, '[', [keys.start, keys.stop, keys.step])
        return Operation(self, '[', keys)
    
    def __getattr__(self, name):
        """ called when evaluating <expression>.attribute """
        return Operation(self, '.',  Term(name, forced_type='constant'))

    def __call__ (self, *args, **kwargs):
        assert not kwargs, "Sorry, key word arguments are not supported yet"
        return Operation(self, '(', args)

    
class Term(threading.local, Expression ):
    
    def __init__(self, name='??', forced_type=None):
        
        self._pyD_negated = False # for aggregate with sort in descending order
        self._pyD_precalculations = Body() # no precalculations
        self._pyD_atomized = True
        
        if (isinstance(name, utils.string_types)):
            name = 'X%i' % id(self) if name =='??' else name
            name = True if name=='True' else False if name =='False' else name
        if isinstance(name, (list, tuple, utils.xrange)):
            self._pyD_value = list(map(Expression._pyD_for, name))
            self._pyD_name = utils.unicode_type([element._pyD_name for element in self._pyD_value])
            self._pyD_type = 'tuple'

            self._pyD_precalculations = pre_calculations(self._pyD_value)
        elif forced_type=="constant" or isinstance(name, (int, float, bool)) \
        or name is None \
        or ((isinstance(name, utils.string_types) and name[0] not in string.ascii_uppercase + '_' and not '.' in name)):
            self._pyD_value = name
            self._pyD_type = 'constant'
        else:
            self._pyD_value = name
            self._pyD_name = name
            self._pyD_type = 'variable'
            index = name.find('.')


    @classmethod
    def make_for_prefix(cls, name): #prefixed #call
        """ returns either '_pyD_class' or the prefix"""
        prefix = name.split('.')[0]
        return Term(prefix)

    def is_variable(self):
        return self._pyD_type == 'variable' and not self._pyD_name.startswith('_pyD_')
    
    def _pyD_variables(self):
        """ returns an ordered dictionary of the variables in the varSymbol """
        if self.is_variable():
            return OrderedDict({self._pyD_name : self})
        elif self._pyD_type == 'tuple':
            variables = OrderedDict()
            for element in self._pyD_value:
                variables.update(element._pyD_variables())
            return variables
        else:
            return OrderedDict()
    
    def __add__(self, other):
        return Operation(self, '+', other)
    def __radd__(self, other):
        return Operation(other, '+', self)

    def __neg__(self):
        """ called when evaluating -X. Used in aggregate arguments """
        neg = Term(self._pyD_value)
        neg._pyD_negated = not(self._pyD_negated)

        expr = 0 - self
        expr._pyD_variable = neg
        return expr
    
    def __getattr__(self, name):
        """ called when evaluating class.attribute """
        if self._pyD_name in Thread_storage.variables: #prefixed
            return Operation(self, '.', Term(name, forced_type='constant'))
        return Term(self._pyD_name + '.' + name)

    def __getitem__(self, keys):
        """ called when evaluating name[keys] """
        if self._pyD_name in Thread_storage.variables: #prefixed
            return Expression.__getitem__(self, keys)
        return Function(self._pyD_name, keys)

    def __setitem__(self, keys, value):
        """  called when evaluating f[X] = expression """
        function = Function(self._pyD_name, keys)
        value = Expression._pyD_for(value)
        if Expression._pyD_for(keys)._pyD_lua.is_const() and value._pyD_lua.is_const():
            +(function == value)
        else:
            (function == function._pyD_symbol) <= (function._pyD_symbol == value)
            
    def __delitem__(self, keys):
        """  called when evaluating del f[X] """
        function = Function(self._pyD_name, keys)
        Y = Term('??')
        if Expression._pyD_for(keys)._pyD_lua.is_const():
            -(function == ((function == Y) >= Y) )
        else:
            literal = (function == Y)
            literal.lua.pred.reset_clauses()

    def __call__ (self, *args, **kwargs):
        """ called when evaluating p(args) """
        from . import aggregate
        if self._pyD_name == 'ask': # call ask() and return an answer
            if 1<len(args):
                raise RuntimeError('Too many arguments for ask !')
            return Answer.make(args[0].ask())
        
        # manage the aggregate functions
        elif self._pyD_name in ('_sum', 'sum_'):
            if isinstance(args[0], Term):
                return aggregate.Sum(args[0], for_each=kwargs.get('for_each', kwargs.get('key', [])))
            else:
                return sum(args)
        elif self._pyD_name in ('concat', 'concat_'):
            return aggregate.Concat(args[0], order_by=kwargs.get('order_by',kwargs.get('key', [])), sep=kwargs['sep'])
        elif self._pyD_name in ('_min', 'min_'):
            if isinstance(args[0], Term):
                return aggregate.Min(args[0], order_by=kwargs.get('order_by',kwargs.get('key', [])),)
            else:
                return min(args)
        elif self._pyD_name in ('_max', 'max_'):
            if isinstance(args[0], Term):
                return aggregate.Max(args[0], order_by=kwargs.get('order_by',kwargs.get('key', [])),)
            else:
                return max(args)
        elif self._pyD_name in ('rank', 'rank_'):
            return aggregate.Rank(None, group_by=kwargs.get('group_by', []), order_by=kwargs.get('order_by', []))
        elif self._pyD_name in ('running_sum', 'running_sum_'):
            return aggregate.Running_sum(args[0], group_by=kwargs.get('group_by', []), order_by=kwargs.get('order_by', []))
        elif self._pyD_name == 'tuple_':
            return aggregate.Tuple(args[0], order_by=kwargs.get('order_by', []))
        elif self._pyD_name == 'mean_':
            return aggregate.Mean(args[0], for_each=kwargs.get('for_each', []))
        elif self._pyD_name == 'linear_regression_':
            return aggregate.Linear_regression(args[0], for_each=kwargs.get('for_each', []))
        elif self._pyD_name in ('_len', 'len_'):
            if isinstance(args[0], Term):
                return aggregate.Len(args[0])
            else: 
                return len(args[0]) 
        elif self._pyD_name == 'range_':
            return Operation(None, '..', args[0])
        elif self._pyD_name == 'format_':
            return Operation(args[0], '%', args[1:])
        elif '.' in self._pyD_name: #call
            pre_term = (Term.make_for_prefix(self._pyD_name), ) #prefixed
            return Call(self._pyD_name, pre_term + tuple(args), kwargs)
        else: # create a literal
            literal = Literal.make(self._pyD_name, tuple(args), kwargs)
            return literal

    def __str__(self):
        if self._pyD_name in Thread_storage.variables: #prefixed
            return LazyList.__str__(self)
        return utils.cast_to_str(self._pyD_name)
    
    def __unicode__(self):
        if self._pyD_name in Thread_storage.variables: #prefixed
            return LazyList.__unicode__(self)
        return utils.unicode_type(self._pyD_name)
    

def pre_calculations(args):
    """ collects the pre_calculations of all args"""
    pre_calculations = Body()
    for arg in args:
        if isinstance(arg, Expression):
            pre_calculations = pre_calculations & arg._pyD_precalculations
    return pre_calculations

        
class Function(Expression):
    """ represents predicate[a, b]"""
    counter = utils.Counter() # counter of functions evaluated so far
        
    def __init__(self, name, keys):
        self._pyD_keys = keys if isinstance(keys, tuple) else (keys,)
        self._pyD_name = "%s[%i]" % (name, len(self._pyD_keys))
        self._argument_precalculations = pre_calculations(self._pyD_keys)
                
        self._pyD_symbol = Term('_pyD_X%i' % Function.counter.next())
        self._pyD_lua = self._pyD_symbol._pyD_lua
        self._pyD_precalculations = self._argument_precalculations & (self == self._pyD_symbol)
    
    def __eq__(self, other):
        return Literal.make_for_comparison(self, '==', other)
    
    # following methods are used when the function is used in an expression
    def _pyD_variables(self):
        """ returns an ordered dictionary of the variables in the keys of the function"""
        return self._argument_precalculations._variables()

    def __unicode__(self):
        return "%s[%s]" % (self._pyD_name.split('[')[0], ','.join(utils.unicode_type(key) for key in self._pyD_keys))
    
    def __str__(self):
        return utils.cast_to_str(self.__unicode__())
    
class Operation(Expression):
    """created when evaluating an operation (+, -, *, /, //) """
    def __init__(self, lhs, operator, rhs):
        self._pyD_operator = operator
        self._pyD_lhs = Expression._pyD_for(lhs) # left  hand side
        self._pyD_rhs = Expression._pyD_for(rhs)
        self._pyD_precalculations = pre_calculations((self._pyD_lhs, self._pyD_rhs)) #TODO test for slice, len
        
    @property
    def _pyD_name(self):
        return utils.unicode_type(self)
    
    def _pyD_variables(self):
        """ returns an ordered dictionary of the variables in this Operation"""
        temp = self._pyD_lhs._pyD_variables()
        temp.update(self._pyD_rhs._pyD_variables())
        return temp
    
    def __unicode__(self):
        return '(' + utils.unicode_type(self._pyD_lhs._pyD_name) + self._pyD_operator + utils.unicode_type(self._pyD_rhs._pyD_name) + ')'

    def __str__(self):
        return utils.cast_to_str(self.__unicode__())
    
class Literal(object):
    """
    created by source code like 'p(a, b)'
    operator '<=' means 'is true if', and creates a Clause
    """
    def __init__(self, predicate_name, args, kwargs, prearity=None, aggregate=None):
        import aggregate
        t = sorted(kwargs.items()) if kwargs is not None else ()
        self.predicate_name = '_'.join([predicate_name]+[p[0] for p in t])
        self.args = list(args) + [p[1] for p in t]
        self.prearity = len(self.args) if prearity is None else prearity
        self.pre_calculations = Body()
        
        self.todo = self
        
        cls_name = self.predicate_name.split('.')[0].replace('~','') if 1< len(self.predicate_name.split('.')) else ''

        self.terms = [] # the list of args converted to Expression
        for arg in self.args:
            if isinstance(arg, Literal):
                raise utils.DatalogError("Syntax error: Literals cannot have a literal as argument : %s%s" % (self.predicate_name, self.terms), None, None)
            elif isinstance(arg, aggregate):
                raise utils.DatalogError("Syntax error: Incorrect use of aggregation.", None, None)
            if isinstance(arg, Term) and arg.is_variable():
                arg.todo = self
                arg._data = [] # reset the variable. For use in in-line queries
            self.terms.append(Expression._pyD_for(arg))
                            
        for term in self.terms:
            for var in term._pyD_variables().keys():
                Thread_storage.variables.add(var) #call update the list of variables since the last clause
            
        tbl = [a._pyD_lua for a in self.terms]
        # now create the literal for the head of a clause

    @classmethod
    def make(cls, predicate_name, terms, kwargs=None, prearity=None, aggregate=None):
        """ factory class that creates a Query or HeadLiteral """
        precalculations = pre_calculations(terms)
        if '!' in predicate_name: #pred e.g. aggregation literal
            return precalculations & HeadLiteral(predicate_name, terms, kwargs, prearity, aggregate)
        else:
            return precalculations & Query(predicate_name, terms, kwargs, prearity, aggregate)
    
    @classmethod
    def make_for_comparison(cls, self, operator, other):
        """ factory of Literal (or Body) for a comparison. """
        from .aggregate import aggregate
        other = Expression._pyD_for(other)
        if isinstance(other, Function) and operator == '==':
            self, other = other, self
        if isinstance(self, Function):
            if isinstance(other, aggregate): # p[X]==aggregate()
                return other.make_literal_for(self, operator)
            #TODO perf : do not add pre-term for non prefixed #prefixed
            name, prearity = self._pyD_name + operator, 1+len(self._pyD_keys)
            terms = [Term.make_for_prefix(self._pyD_name)] + list(self._pyD_keys) + [other]  #prefixed
            literal = Query(name, terms, {}, prearity)
            return self._argument_precalculations & other._pyD_precalculations & literal
        else:
            if not isinstance(other, Expression):
                raise utils.DatalogError("Syntax error: Term or Expression expected", None, None)
            literal = Query(operator, [self] + [other])
            return self._pyD_precalculations & other._pyD_precalculations & literal

    @property
    def literals(self):
        return [self]
    
    def _variables(self):
        """ returns an ordered dictionary of the variables in the Literal"""
        if self.predicate_name[0] == '~': #pred ignore variables of negated literals
            return OrderedDict()
        variables = OrderedDict()
        for term in self.terms:
            variables.update(term._pyD_variables())
        return variables
    
    def __le__(self, body):
        " head <= body creates a clause"
        Thread_storage.variables = set([]) #call reset the list of variables
        body = body.as_literal if isinstance(body, Call) else body #call
        if not isinstance(body, (Literal, Body)):
                raise utils.DatalogError("Invalid body for clause", None, None)
        else:
            newBody = Body()
            for literal in body.literals:
                if isinstance(literal, HeadLiteral):
                    raise utils.DatalogError("Aggregation cannot appear in the body of a clause", None, None)
                newBody = newBody & literal
            return add_clause(self, newBody)

class HeadLiteral(Literal):
    """ represents literals that can be used only in head of clauses, i.e. literals with aggregate function"""
    pass

class Query(Literal):
    """
    represents a literal that can be queried (thus excludes aggregate literals)
    unary operator '+' means insert it as fact
    binary operator '&' means 'and', and returns a Body
    """
    def __init__(self, predicate_name, terms, kwargs=None, prearity=None, aggregate=None):

        Literal.__init__(self, predicate_name, terms, kwargs, prearity, aggregate)
        
    def ask(self):
        self._data = Body(self.pre_calculations, self).ask()
        self.todo = None
        return self._data

    def __pos__(self):
        " unary + means insert into database as fact "
        if self._variables():
            raise utils.DatalogError("Cannot assert a fact containing Variables", None, None)


    def __neg__(self):
        " unary - means retract fact from database "
        if self._variables():
            raise utils.DatalogError("Cannot retract a fact containing Variables", None, None)
        
    def __invert__(self):
        """unary ~ means negation """
        return Literal.make('~' + self.predicate_name, self.terms) #pred

    def __and__(self, other):
        " literal & literal" 
        return Body(self, other)

    def literal(self):
        return self

class Call(Operation): #call
    """ represents an ambiguous A.b(X) : usually an expression, but sometimes a literal"""
    def __init__(self, name, args, kwargs):
        self.as_literal = Query(name, args, kwargs)
        Operation.__init__(self, name, '(', args)

    @property
    def literals(self):
        return [self.as_literal]
    
    def __and__(self, other):
        " Call & literal"
        return Body(self.as_literal, other)
        
    def __invert__(self):
        """unary ~ means negation """
        return ~ self.as_literal

    def __le__(self, other):
        " head <= other creates a clause or comparison"
        if isinstance(other, (Literal, Body)):
            return self.as_literal <= other
        return other > self

    def ask(self):
        return Body(self.as_literal).ask()
        
class Body():
    """ created by p(a,b) & q(c,d)  """
    def __init__(self, *args):
        
        self.literals = []
        for arg in args:
            self.literals += [arg] if isinstance(arg, Literal) else arg.literals
            
        env = OrderedDict()
        for literal in self.literals:
            for term in literal._variables().values():
                env[term._pyD_name] = term
        self.__variables = env
        
        self.todo = self
        for variable in env.values():
            variable.todo = self

    def _variables(self):
        return self.__variables

    def __and__(self, body2):
        """ operator '&' means 'and', and returns a Body """
        b = Body(self, body2)
        return b if len(b.literals) != 1 else b.literals[0]

    def literal(self):
        """ return a literal that can be queried to resolve the body """
        prearity = None
        if len(self.literals)==1: # determine the literal prearity in case of a single literal
            # it could be less than the literal prearity in case of repetition of a variable
            base_literal = self.literals[0]
            if not base_literal.predicate_name.startswith('~'):
                variables = OrderedDict()
                for i in range(base_literal.prearity):
                    variables.update(base_literal.terms[i]._pyD_variables())
                prearity = len(variables)
        literal = Literal.make('_pyD_query' + utils.unicode_type(Body.counter.next()), list(self._variables().values()), {}, prearity=prearity)
        literal <= self
        return literal
        
    def __invert__(self):
        """unary ~ means negation """
        return ~(self.literal())

    def ask(self):
        """ resolve the query and determine the values of its variables"""
        literal = self.literal()
        self._data = literal.lua.ask()
        literal.todo, self.todo = None, None
        - (literal <= self) # delete the temporary clause
        # update the variables
        transposed = list(zip(*(self._data))) if isinstance(self._data, list) else None # transpose result
        for i, arg in enumerate(self._variables().values()):
            if self._data is True:
                arg._data = True
            elif self._data:
                arg._data = list(transposed[i])
            arg.todo = None
        return self._data

def add_clause(head,body):
    if isinstance(body, Body):
        tbl = [a.lua for a in body.literals]
    else: # body is a literal
        tbl = [body.lua,]
    clause = pyEngine.Clause(head.lua, tbl)
    result = pyEngine.assert_(clause)
    if not result: 
        raise utils.DatalogError("Can't create clause", None, None)
    return result


        
"""                             Parser methods                                                   """

def add_symbols(names, variables):
    """ add the names to the variables dictionary"""
    for name in names:
        if name not in variables.keys():
            variables[name] = Term(name)            
    
class _transform_ast(ast.NodeTransformer):
    """ does some transformation of the Abstract Syntax Tree of the datalog program """
    def visit_Call(self, node):
        """rename builtins to allow customization"""
        self.generic_visit(node)
        if hasattr(node.func, 'id'):
            node.func.id = 'sum_' if node.func.id == 'sum' else node.func.id
            node.func.id = 'len_' if node.func.id == 'len' else node.func.id
            node.func.id = 'min_' if node.func.id == 'min' else node.func.id
            node.func.id = 'max_' if node.func.id == 'max' else node.func.id
        return node
    
    def visit_Compare(self, node):
        """ rename 'in' to allow customization of (X in (1,2))"""
        self.generic_visit(node)
        if 1 < len(node.comparators): 
            raise utils.DatalogError("Syntax error: please verify parenthesis around (in)equalities", node.lineno, None) 
        if not isinstance(node.ops[0], (ast.In, ast.NotIn)): return node
        var = node.left # X, an _ast.Name object
        comparators = node.comparators[0] # (1,2), an _ast.Tuple object
        

def load(code, newglobals=None, defined=None, function='load'):
    """ code : a string or list of string 
        newglobals : global variables for executing the code
        defined : reserved symbols
    """
    newglobals, defined = newglobals or {}, defined or set([])
    # remove indentation based on first non-blank line
    lines = code.splitlines() if isinstance(code, utils.string_types) else code
    r = re.compile('^\s*')
    for line in lines:
        spaces = r.match(line).group()
        if spaces and line != spaces:
            break
    code = '\n'.join([re.sub('^' + spaces, '', line) for line in lines])
    try:
        tree = _transform_ast().visit(tree)
    except utils.DatalogError as e:
        e.function = function
        e.message = e.value
        e.value = "%s\n%s" % (e.value, lines[e.lineno-1])
        utils.reraise(*sys.exc_info())
    code = compile(tree, function, 'exec')

    defined = defined.union(dir(utils.builtins))
    defined.add('None')
    for name in set(code.co_names).difference(defined): # for names that are not defined
        add_symbols((name,), newglobals)
    try:
        utils.exec_(code, newglobals)
    except utils.DatalogError as e:
        e.function = function
        traceback = sys.exc_info()[2]
        e.lineno = 1
        while True:
            if traceback.tb_frame.f_code.co_name == '<module>':
                e.lineno = traceback.tb_lineno
                break
            elif traceback.tb_next:
                traceback = traceback.tb_next 
        e.message = e.value
        e.value = "%s\n%s" % (e.value, lines[e.lineno-1])
        utils.reraise(*sys.exc_info())
        
class _NoCallFunction(object):
    """ This class prevents a call to a datalog program created using the 'program' decorator """
    def __call__(self):
        raise TypeError("Datalog programs are not callable")

def add_program(func):
    """ A helper for decorator implementation   """
    source_code = inspect.getsource(func)
    lines = source_code.splitlines()
    # drop the first 2 lines (@pydatalog and def _() )
    if '@' in lines[0]: del lines[0]
    if 'def' in lines[0]: del lines[0]
    source_code = lines

    try:
        code = func.__code__
    except:
        raise TypeError("function or method argument expected")
    newglobals = func.__globals__.copy() if func.func_globals.copy() else {}
    func_name = func.__name__
    defined = set(code.co_varnames).union(set(newglobals.keys())) # local variables and global variables

    load(source_code, newglobals, defined, function=func_name)
    return _NoCallFunction()

def ask(code):
    """ runs the query in the code string """
    tree = _transform_ast().visit(tree)
    code = compile(tree, 'ask', 'eval')
    newglobals = {}
    add_symbols(code.co_names, newglobals)
    parsed_code = eval(code, newglobals)
    a = parsed_code.ask()
    return Answer.make(a)

class Answer(object):
    """ object returned by ask() """
    def __init__(self, name, arity, answers):
        self.name = name
        self.arity = arity
        self.answers = answers

    @classmethod
    def make(cls, answers):
        if answers is True:
            answer = Answer('_pyD_query', 0, True)
        elif answers:
            answer = Answer('_pyD_query', len(answers), answers)
        else:
            answer = None

    def __eq__ (self, other):
        return other == True if self.answers is True \
            else other == set(self.answers) if self.answers \
            else other is None
            
    def __str__(self):
        return 'True' if self.answers is True \
            else utils.cast_to_str(utils.unicode_type(set(self.answers))) if self.answers is not True \
            else 'True'