import re, parserDatalog
from datalog import Datalog
from parserDatalog import parserDatalog

class Database:
    def __init__(self, edb_facts):
        self.edb_facts = edb_facts
        self.idb_facts = []
        self.idb_rules = []
        self.idb_rules_with_aggregation = []
        self.rules = []
    
    def add_rule(self, head_predicate, body):
        rule = [head_predicate] + body
        self.rules.append(rule)
        if self.is_aggregate_function(body):
            self.idb_rules_with_aggregation.append(rule)
        else:
            self.idb_rules.append(rule)

    def add_rules(self, rules):
        for rule in rules:
            self.add_rule(rule[0], rule[1:])
        
    def get_rule(self, head_predicate):
        for rule in self.rules:
            if rule[0] == head_predicate:
                return rule
    
    def add_facts(self, facts):
        for fact in facts:
            self.add_fact(fact[0], fact[1:])
    
    def add_fact(self, predicate, arguments):
        fact = [predicate] + arguments
        self.edb_facts.append(fact)

    def get_facts(self, predicate):
        return [fact for fact in self.edb_facts if fact[0] == predicate]


    def is_aggregate_function(self, body):
        if not body:
            return False
        return body[0] in ["COUNT", "SUM", "AVG", "MIN", "MAX"]

    def condition_rule(self, rule):
        return len(rule) == 1

    def split_rules(self, rules):
        if isinstance(rules, str):
            return rules.split(",")
        return rules

    def split_facts(self, facts):
        if isinstance(facts, str):
            return facts.split(",")
        return facts

    def extract_parts_in_rule(self, rule):
        var = []
        value = []
        for part in rule:
            if part[0] == "?":
                var.append(part)
            else:
                value.append(part)
        return var, value
    
    def extract_parts_in_fact(self, fact):
        var = []
        value = []
        for part in fact:
            if part[0] == "?":
                var.append(part)
            else:
                value.append(part)
        return var, value

    def extract_inside(self, inside):
        return inside.split(",")
    
    def is_fact(self, rule):
        return len(rule) == 1
    
    def is_rule(self, rule):
        return len(rule) > 1 

    def is_variable(self, rule):
        return rule[0].isupper()

    def is_constant(self, rule):
        return rule[0].islower()

    def query(self, predicate, arguments):
        return [fact for fact in self.edb_facts if fact[0] == predicate and fact[1:] == arguments]

    def get_aggregate_function(self, rule):
        return rule[1][0]

    def extract_parts_in_condition(self, condition):
        return condition.split(" ")

    def match_facts(self, facts):
        for fact in facts:
            self.add_fact(fact[0], fact[1:])


    def get_predicates_names(self):
        return [fact[0] for fact in self.edb_facts]

        
    def get_aggregate_arguments(self, aggregate_name, body):
        """
        Renvoie les arguments de la fonction d'agrégation.
        """
        # Si le nom de l'agrégation est "count"
        if aggregate_name == "COUNT":
            # Renvoyer le nombre d'arguments du prédicat
            return len(body)
        # Si le nom de l'agrégation est "sum"
        elif aggregate_name == "SUM":
            # Renvoyer le nom de la variable d'agrégation
            return body[0].arguments[0]
        # Si le nom de l'agrégation est "min"
        elif aggregate_name == "MIN":
            # Renvoyer le nom de la variable d'agrégation
            return body[0].arguments[0]
        # Si le nom de l'agrégation est "max"
        elif aggregate_name == "MAX":
            # Renvoyer le nom de la variable d'agrégation
            return body[0].arguments[0]
        # Si le nom de l'agrégation est "avg"
        elif aggregate_name == "AVG":
            # Renvoyer le nom de la variable d'agrégation
            return body[0].arguments[0]
        elif aggregate_name == "LEN":
            return body[0].arguments[0]
        # Sinon
        else:
            # Lever une exception
            raise Exception("Unknown aggregate function: {}".format(aggregate_name))