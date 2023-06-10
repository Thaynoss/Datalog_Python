from itertools import groupby
from . import utils
from .parserDatalog import Expression, Literal, Term, Operation, add_clause


import statistics
mean = statistics.mean

class Aggregate(object):
    compteur = utils.Counter()
    
    def __init__(self, Y=None, group_by=tuple(), for_each=tuple(), order_by=tuple(), sep=None):
        # convert for_each=Z to for_each=(Z,)
        self.Y = Y
        self.group_by = (group_by,) if isinstance(group_by, Expression) else tuple(group_by)
        self.for_each = (for_each,) if isinstance(for_each, Expression) else tuple(for_each)
        self.order_by = (order_by,) if isinstance(order_by, Expression) else tuple(order_by)
        
        # recover the negated variable in order_by
        self.order_by = tuple([e.__dict__.get('_pyD_variable', e) for e in self.order_by])
        
        if not all([isinstance(e, Term) and e.is_variable() for e in self.group_by + self.for_each + self.order_by]):
            raise utils.DatalogError("Arguments of aggregate must be variable(s).", None, None)
        
        if sep and not isinstance(sep, utils.string_types):
            raise utils.DatalogError("Separator in aggregation must be a string", None, None)
        self.sep = sep
        
        # verify presence of keyword arguments
        for kw in self.required_kw:
            arg = getattr(self, kw)
            if arg is None or (isinstance(arg, tuple) and arg == tuple()):
                raise utils.DatalogError("Error: argument missing in aggregate", None, None)
        
        # used to create literal.
        self.args = ((Y,) if Y is not None else tuple()) + self.group_by + self.for_each + self.order_by + ((sep,) if sep is not None else tuple())
        self.Y_arity = 1 if Y is not None else 0
        self.sep_arity = 1 if sep is not None else 0
        
    @property
    def arity(self):
        return len(self.args)

    def make_literal_for(self, function, operator):
        if operator != '==':
            raise utils.DatalogError("Aggregate operator can only be used with equality.", None, None)

        name = function._pyD_name + operator
        result = function._pyD_symbol
        terms = [Term.make_for_prefix(function._pyD_name)] + list(function._pyD_keys) + [result]
        self.index_first_arg = len(terms)-1 # position of the value to add
        
        # 1 create literal that can be queried
        head = Literal.make(name, terms, {}, prearity=len(terms)-1)

        # 2 create clause to resolve it
        
        terms[-1:-1] = self.args # insert the aggregate arguments before the result

        # determine list of variables, without duplication
        variables, new_terms = {}, [] 
        for variable in terms:
            if isinstance(variable, Term) and variable._pyD_name not in variables:
                variables[variable._pyD_name] = len(new_terms)
                new_terms.append(variable)
                
        new_name = name + '!' + str(Aggregate.compteur.next())
        body = Literal.make(new_name, new_terms, {}, aggregate=self) #pred
        add_clause(head, body)
                
        self.index_value = variables[self.Y._pyD_name] if self.Y is not None else None
        self.slice_for_each = [variables[variable._pyD_name] for variable in self.for_each]
        self.reversed_order_by = [variables[variable._pyD_name] for variable in self.order_by][::-1]
        self.reverse_order = [variable._pyD_negated for variable in new_terms[:-1]]
        if isinstance(self, Rank): # can't use required_kw because rank does not require group_by
            self.slice_group_by = [variables[variable._pyD_name] for variable in self.group_by]
        else:
            self.slice_group_by = [variables[Expression._pyD_for(variable)._pyD_name] 
                                   for variable in function._pyD_keys if isinstance(variable, Term)]
        self.slice_to_variabilize = [variables[variable._pyD_name] for variable in function._pyD_keys 
                                     if isinstance(variable, Term) 
                                     and variables[variable._pyD_name] not in self.slice_group_by]
        
        # return a literal without the result
        new_literal = Literal.make(new_name, new_terms[:-1], {})
        return new_literal
        
    def fact_candidate(self, subgoal, row):
        if row is not None:
            class0 = subgoal.literal.pred._class()
            row[self.index_first_arg:-1] = [""] * (len(row)-self.index_first_arg-1)
            subgoal.fact_candidate(class0, row)

    def complete(self, base_subgoal, subgoal):
        """ calculate the aggregate after base facts have been found """
        result = [ tuple(l.terms) for l in base_subgoal.facts.values()]
        if result:
            self.sort_result(result)
            for _, v in groupby(result, self.key):
                self.reset()
                for r in v:
                    row = self.add(r)
                    self.fact_candidate(subgoal, row)
                row = self.fact(r)
                self.fact_candidate(subgoal, row)
        
    def sort_result(self, result):
        """ sort result according to the aggregate argument """
        # first sort per order_by, allowing for _pyD_negated
        for i in self.reversed_order_by:
            result.sort(key=lambda literal, i=i, self=self: literal[i].id,
                reverse = self.reverse_order[i])
        # then sort per group_by
        result.sort(key=lambda literal, self=self: [literal[i].id for i in self.slice_group_by])
    
    def key(self, result):
        """ return the grouping key of a result """
        return list(result[i].id for i in self.slice_group_by)
    
    def reset(self):
        """ by default, _value is 0 """
        self._value = 0
        
    @property
    def value(self):
        """ by default, value is _value"""
        return self._value
    
    def fact(self, row):
        """ returns the terms of an aggregated fact"""
        return list(row) + [self.value]
       
class Sum(Aggregate):
    """ represents sum_(Y, for_each=(Z,T))"""
    required_kw = ('Y', 'for_each')

    def add(self, row):
        self._value += row[self.index_value].id
        
class Len(Aggregate, Operation):
    """ represents len_(X) : a simple or aggregate operation"""
    required_kw = ('Y')

    def __init__(self, Y):
        Aggregate.__init__(self, Y)
        Operation.__init__(self, None, '#', Y)
        
    def add(self, row):
        self._value += 1

class Tuple(Aggregate):
    """ represents tuple_(X, order_by=(Y,)"""
    required_kw = ('Y', 'order_by')
        
    def reset(self):
        self._value = []
        
    def add(self, row):
        self._value.append(row[self.index_value].id)
        
    @property
    def value(self):
        return tuple(self._value)

class Mean(Tuple):
    """ represents mean_(X, for_each=(Y,)"""
    required_kw = ('Y', 'for_each')
    
    @property
    def value(self):
        return mean(self._value)

class Linear_regression(Aggregate):
    """ represents mean_(Y, for_each=(X,)"""
    required_kw = ('Y', 'for_each')
    
    def reset(self):
        self.X = []
        self.Y = []
        
    def add(self, row):
        self.X.append(row[self.slice_for_each[0]].id)
        self.Y.append(row[self.index_value].id)
        
    @property
    def value(self):
        length = len(self.X)
        sum_x = sum(self.X)
        sum_y = sum(self.Y)
    
        sum_x_squared = sum(map(lambda a: a * a, self.X))
        covariance = sum(self.X[i] * self.Y[i] for i in range(length))
    
        a = (covariance - (sum_x * sum_y) / length) / (sum_x_squared - ((sum_x ** 2) / length))
        b = (sum_y - a * sum_x) / length
        return (a, b)    
    
class Concat(Tuple):
    """ represents concat_(Y, order_by=(Z1,Z2), sep=sep)"""
    required_kw = ('Y', 'order_by', 'sep')

    @property
    def value(self):
        return self.sep.join(self._value)

class Min(Aggregate):
    """ represents min_(Y, order_by=(Z,T))"""
    required_kw = ('Y', 'order_by')

    def reset(self):
        self._value = None
        
    def add(self, row):
        # take the value of the first row
        self._value = row[self.index_value].id if self._value is None else self._value

class Max(Min):
    """ represents max_(Y, order_by=(Z,T))"""

    def add(self, row):
        # take the value of the last row
        self._value = row[self.index_value].id

class Rank(Aggregate):
    """ represents rank_(group_by=Z, order_by=T)"""
    required_kw = ('order_by',)
    
    def add(self, row):
        self._value += 1
        return list(row) + [self._value-1]
        
    def fact(self, k):
        return None

class Running_sum(Rank):
    """ represents running_sum(Y, group_by=Z, order_by=T"""
    required_kw = ('Y', 'group_by', 'order_by')
    
    def add(self, row):
        self._value += row[self.index_value].id
        return list(row) + [self._value]