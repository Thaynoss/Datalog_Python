from pyparsing import *
from typing import List, Tuple
import os
import parserDatalog 
import database
import agregation

class Datalog:
    def __init__(self):
        self.database = database.Database([])
        self.parser = parserDatalog.parserDatalog()
        self.edb_facts = []
        self.rules = []
        self.idb_rules = []
        self.idb_facts = []
        self.parser = parserDatalog.parserDatalog()
        self.aggregation = agregation.Agregation()

    def load_database(self, database_file):
        """
        Charge une base de données Datalog à partir d'un fichier.
        """
        # Ouvrir le fichier en lecture
        with open(database_file, "r") as file:
            # Charger le contenu du fichier
            database_content = file.read()
        # Charger le contenu de la base de données
        self.load_database_content(database_content)


    def load_database_content(self, database_content):
        """
        Charge une base de données Datalog à partir d'une chaîne de caractères.
        Les commentaires sont ignorés.
        Les faits et les règles sont séparés par des lignes vides.
        Parse les faits et les règles avec le parser Datalog.
        """
        # Créer une liste pour stocker les faits et les règles
        facts = []
        rules = []
        # Parcourir toutes les lignes de la base de données
        for line in database_content.splitlines():
            # Si la ligne est un commentaire
            if line.startswith("#"):
                # Ignorer la ligne
                continue
            # Si la ligne est vide
            elif not line.strip():
                # Ignorer la ligne
                continue
            # Si la ligne est un fait donc ne contient pas la flèche
            elif ":-" not in line and line.endswith("."):
                # Ajouter le fait à la liste des faits
                facts.append(line)
            # Sinon, la ligne est une règle
            elif ":-" in line:
                # Ajouter la règle à la liste des règles
                rules.append(line)
            else:
                raise Exception("Invalid line: {}".format(line))
        return facts, rules
    
    def stratify_rules(self, rules):
        """
        Stratifie les règles Datalog.
        """
        # Créer une liste pour stocker les règles stratifiées
        stratified_rules = []
        # Tant qu'il reste des règles à stratifier
        while rules:
            # Récupérer les règles indépendantes
            independent_rules = self.get_independent_rules(rules)
            # Ajouter les règles indépendantes à la liste des règles stratifiées
            stratified_rules.append(independent_rules)
            # Supprimer les règles indépendantes de la liste des règles à stratifier
            for rule in independent_rules:
                rules.remove(rule)
        # Renvoyer les règles stratifiées
        return stratified_rules
    
    def get_independent_rules(self, rules):
        """
        Renvoie les règles Datalog qui ne dépendent d'aucune autre règle.
        """
        # Créer une liste pour stocker les règles indépendantes
        independent_rules = []
        # Parcourir toutes les règles
        for rule in rules:
            # Récupérer les dépendances de la règle
            dependencies = self.get_dependencies(rule)
            # Si la règle n'a aucune dépendance
            if not dependencies:
                # Ajouter la règle à la liste des règles indépendantes
                independent_rules.append(rule)
        # Renvoyer les règles indépendantes
        return independent_rules

    def get_dependencies(self, rule):
        """
        Renvoie les dépendances d'une règle Datalog.
        """
        # Récupérer le corps de la règle
        body = self.parse_body(rule)
        # Récupérer les dépendances du corps de la règle
        dependencies = self.get_dependencies_from_body(body)
        # Renvoyer les dépendances
        return dependencies
    
    def get_dependencies_from_body(self, body):
        """
        Renvoie les dépendances d'un corps de règle Datalog.
        """
        # Créer une liste pour stocker les dépendances
        dependencies = []
        # Parcourir tous les prédicats du corps de la règle
        for predicate in body:
            # Récupérer le nom du prédicat
            predicate_name = self.parser.parse_predicate_name(predicate)
            # Si le prédicat est un prédicat de base de données
            if predicate_name in self.database.get_predicates_names():
                # Ajouter le prédicat à la liste des dépendances
                dependencies.append(predicate)
        # Renvoyer les dépendances
        return dependencies
    
    
    def eval_rules(self, rules, aggregate=False):
        """
        Évalue toutes les règles Datalog dans l'ordre stratifié.
        """
        # Stratifier les règles
        stratified_rules = self.stratify_rules(rules)
        # Parcourir toutes les règles stratifiées
        for rules in stratified_rules:
            # Évaluer les règles
            self.eval_rules_with_database(rules, aggregate)

    def eval_rules_with_database(self, rules, aggregate):
        """
        Évalue toutes les règles Datalog dans l'ordre stratifié.
        """
        # Parcourir toutes les règles
        for rule in rules:
            # Évaluer la règle
            self.eval_rule_with_database(rule, aggregate)
    
    def eval_rule_with_database(self, rule, aggregate):
        """
        Évalue une règle Datalog.
        """
        # Récupérer la tête de la règle
        head = self.parse_head(rule)
        # Récupérer le corps de la règle
        body = self.parse_body(rule)
        # Récupérer le nom du prédicat de la tête de la règle
        head_predicate_name = self.parser.parse_predicate_name(head)
        # Si le prédicat de la tête de la règle est un prédicat de base de données
        if head_predicate_name in self.database.get_predicates_names():
            # Évaluer la règle
            print("Évaluer la règle avec la base de données")
            self.aggregation.eval_rule_with_database(rule, aggregate)
            self.eval_rule_with_database(rule, aggregate)
        # Sinon, le prédicat de la tête de la règle est un prédicat d'extension
        else:
            # Évaluer la règle
            print("Évaluer la règle avec la base d'extension")
            self.aggregation.eval_rule_with_extension(rule, aggregate)

    def eval_rule(self, rule, aggregate):
        """
        Évalue une règle Datalog.
        """
        # Récupérer la tête de la règle
        head = self.parse_head(rule)
        # Récupérer le corps de la règle
        body = self.parse_body(rule)
        # Récupérer le nom du prédicat de la tête de la règle
        head_predicate_name = self.parser.parse_predicate_name(head)
        # Si le prédicat de la tête de la règle est un prédicat de base de données
        if head_predicate_name in self.database.get_predicates_names():
            # Évaluer la règle
            self.eval_rule_with_database(rule, aggregate)
        # Sinon, le prédicat de la tête de la règle est un prédicat d'extension
        else:
            # Évaluer la règle
            self.aggregation.eval_rule_with_extension(rule, aggregate)

    def parse_rule(self, rule):
        """
        Parse une règle Datalog.
        """
        return self.parser.parse_rule(rule)
    
    def parse_head(self, rule):
        """
        Parse la tête d'une règle Datalog.
        """
        return self.parser.parse_head(rule)
    
    def parse_body(self, rule):
        """
        Parse le corps d'une règle Datalog.
        """
        return self.parser.parse_body(rule)

    def eval_stratified_rules(self, stratified_rules, aggregate):
        """
        Évalue les règles stratifiées.
        """
        # Parcourir toutes les règles stratifiées
        for rules in stratified_rules:
            # Évaluer les règles
            self.eval_rules_with_database(rules, aggregate)

if __name__ == "__main__":
    datalog = Datalog()

    with open("data/database.dl", "r") as file:
        database_content = file.read()
        
    # séparer les faits et les règles
    facts, rules = datalog.load_database_content(database_content)
    
    # Ecrit les faits et les règles dans un fichier en séparant les faits et les règles
    with open("resultats/database_facts.dl", "w") as file:
        file.write("\n".join(facts))
    with open("resultats/database_rules.dl", "w") as file:
        file.write("\n".join(rules))

    # Stratifier les règles
    stratified_rules = datalog.stratify_rules(rules)
    with open("resultats/database_stratified_rules.dl", "w") as file:
        file.write("\n".join("\n".join(rule) for rule in stratified_rules))

    # Évaluer les aggregations
    aggregate = ["AVG", "SUM", "COUNT"]
    for aggregation in aggregate:
        # Évaluer les règles stratifiées
        datalog.eval_stratified_rules(stratified_rules, aggregation)
        
        # Écrire le résultat dans un fichier
        with open("resultats/database_result_" + aggregation + ".dl", "w") as file:
            file.write("\n".join(datalog.database.get_facts(predicate=facts) for facts in datalog.database.get_predicates_names()))


        

