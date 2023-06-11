import parserDatalog
import database
import copy
import datalog


class Agregation:
    def __init__(self) -> None:
        self.parser = parserDatalog.parserDatalog()
        self.database = database.Database([])
        self.aggregation = []
        self.aggregation_rules = []
        self.aggregation_facts = []
        self.aggregation_rules_with_aggregation = []
        self.aggregation_facts_with_aggregation = []
        self.aggregation_rules_without_aggregation = []
        self.aggregation_facts_without_aggregation = []

    def get_agregate_function(self, aggregate_name):
        """
        Renvoie la fonction d'agrégation correspondant au nom de l'agrégation.
        """
        # Si le nom de l'agrégation est "count"
        try:
            if aggregate_name == "COUNT":
                # Renvoyer la fonction "count"
                return self.count
            # Si le nom de l'agrégation est "sum"
            elif aggregate_name == "SUM":
                # Renvoyer la fonction "sum"
                return self.sum
            # Si le nom de l'agrégation est "min"
            elif aggregate_name == "MIN":
                # Renvoyer la fonction "min"
                return self.min
            # Si le nom de l'agrégation est "max"
            elif aggregate_name == "MAX":
                # Renvoyer la fonction "max"
                return self.max
            # Si le nom de l'agrégation est "avg"
            elif aggregate_name == "AVG":
                # Renvoyer la fonction "avg"
                return self.avg
            elif aggregate_name == "LEN":
                return self.len
        except:
            pass

    def eval_aggregate_predicate(self, head, body, facts):
        """
        Évalue un prédicat d'agrégation Datalog.
        """
        aggregate_name = head
        aggregate_function = self.get_agregate_function(aggregate_name)
        if aggregate_function is not None:
            aggregate_value = aggregate_function(facts)
            return [(aggregate_name, aggregate_value)]
        else:
            return []
        
    
    def eval_predicate(self, head, body, facts):
        """
        Évalue un prédicat Datalog.
        """
        # Si le prédicat est un prédicat d'agrégation
        if self.is_aggregate_predicate(head):
            # Renvoyer l'évaluation du prédicat d'agrégation
            return self.eval_aggregate_predicate(head, body, facts)
        # Sinon
        else:
            # Renvoyer le prédicat
            return [head]
        
    def eval_predicate_(self, predicate):
        """
        Évalue un prédicat Datalog.
        """
        # Si le prédicat est un prédicat d'agrégation
        if self.is_aggregate_predicate(predicate):
            # Renvoyer l'évaluation du prédicat d'agrégation
            return self.eval_aggregate_predicate(predicate)
        # Sinon
        else:
            # Renvoyer le prédicat
            return [predicate]


    def is_aggregate_predicate(self, predicate):
        """
        Détermine si un prédicat est un prédicat d'agrégation.
        """
        # Renvoyer vrai si le prédicat est un prédicat d'agrégation
        return predicate[0] in ["COUNT", "SUM", "MIN", "MAX", "AVG", "LEN"]
    
    def eval_head_aggregate(self, head):
        """
        Évalue la tête d'une règle Datalog d'agrégation.
        """
        # Récupérer le prédicat de la tête
        predicate = head[0]
        # Récupérer le corps de la règle
        body = head[1]
        # Évaluer le corps de la règle
        facts = self.eval_body(body)
        # Évaluer le prédicat de la tête
        return self.eval_aggregate_predicate(predicate, body, facts)
    
    def eval_body(self, body):
        """
        Évalue le corps d'une règle Datalog.
        """
        # Si le corps est vide
        if len(body) == 0:
            # Renvoyer une liste vide
            return []
        # Sinon
        else:
            # Récupérer le premier prédicat du corps
            predicate = body[0]
            # Évaluer le prédicat
            facts = self.eval_predicate_(predicate)
            # Évaluer le reste du corps
            return facts + self.eval_body(body[1:])
        
    
    def eval_rule_with_database(self,rule, aggregate):
        """
        Évalue une règle Datalog avec une base de données.
        """
        # Récupérer la tête de la règle
        head = rule[0]
        # Récupérer le corps de la règle
        body = rule[1]
        # Évaluer le corps de la règle
        facts = self.eval_body(body)
        # Évaluer la tête de la règle
        return self.eval_aggregate_predicate(head, body, facts)
    
    def eval_rule_with_extension(self, rule, extension):
        """
        Évalue une règle Datalog avec une extension.
        """
        # Récupérer la tête de la règle
        head = rule
        # Récupérer le corps de la règle
        body = rule
        # Évaluer le corps de la règle
        facts = self.eval_body(body)
        # Évaluer la tête de la règle
        return self.eval_aggregate_predicate(head, body, facts)