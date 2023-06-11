from pyparsing import *
from typing import List, Tuple

# Définir les symboles Datalog
ident = quotedString | Word(alphas + "_", alphanums + "_") | Word(alphas.lower(), alphanums + "_") | Word(alphas.upper(), alphanums + "_") | Word(alphas + "_", alphanums + "_")
var = Word(alphas.upper(), alphanums + "_") | Word(alphas.lower(), alphanums + "_")
op_compar = Literal("=") | Literal("!=") | Literal("<") | Literal(">") | Literal("<=") | Literal(">=") | Literal("==")
comma = Literal(",") | Literal(":-")
dot = Literal(".") | Literal(":-")
semicolon = Literal(";") | Literal(":-")
lparen = Literal("(").suppress() | Literal(":(").suppress()
rparen = Literal(":)").suppress() | Literal(")").suppress()
colon = Literal(":").suppress() | Literal(":-")
arrow = Literal(":-") | Literal(":") | Literal("->") | Literal("<-")

count_kw = CaselessLiteral("COUNT") | CaselessLiteral("COUNT_DISTINCT")
sum_kw = CaselessLiteral("SUM") | CaselessLiteral("SUM_DISTINCT")
avg_kw = CaselessLiteral("AVG") | CaselessLiteral("AVG_DISTINCT")
max_kw = CaselessLiteral("MAX") | CaselessLiteral("MAX_DISTINCT")
min_kw = CaselessLiteral("MIN") | CaselessLiteral("MIN_DISTINCT")
len_kw = CaselessLiteral("LEN") | CaselessLiteral("LEN_DISTINCT")
mean_kw = CaselessLiteral("MEAN") | CaselessLiteral("MEAN_DISTINCT")
neg_kw = CaselessLiteral("NOT") | CaselessLiteral("NOT_DISTINCT")
op_math = oneOf("+ - * /")

class Datalog:
    def __init__(self):
        self.db = Database([])
        self.edb_facts = []
        self.rules = []
        self.idb_rules = []
        self.idb_facts = []

    def load_file(self, filename: str):
        with open(filename, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                elif ":-" not in line and line.endswith("."):
                    try:
                        parsed = self.parse_fact(line)
                        self.db.add_fact(parsed[0], parsed[1:])
                    except ParseException as e:
                        print(e)
                        print("Error parsing fact: " + line)
                elif ":-" in line and line.endswith("."):
                    try:
                        parsed = self.parse_rule(line)
                        print(parsed)
                        self.db.add_rule(parsed[0], parsed[1:-1])
                    except ParseException as e:
                        print(e)
                        print("Error parsing rule: " + line)
                else:
                    print("Error parsing line: " + line)

        self.rules = self.db.rules
        self.idb_rules = self.db.idb_rules
        self.edb_facts = self.db.edb_facts


    def parse_fact(self, line):
        expr = ident | var | Word(nums)
        term = Group(expr + lparen + delimitedList(expr | op_math) + rparen)
        atom = Group(ident + lparen + delimitedList(expr) + rparen + Optional(dot | semicolon))
        literal = atom | Group(op_math + term + term)
        body = delimitedList(literal | Group(neg_kw + atom), comma)
        head = Group(ident + lparen + delimitedList(var | expr) + rparen)
        fact = head + Optional(dot | semicolon)
        return fact.parseString(line)

    def parse_rule(self, line):
        # Définir les expressions Datalog IDB
        expr = ident | var | Word(nums)
        term = Group(expr + lparen + delimitedList(expr | op_math) + rparen)
        atom = Group(ident + lparen + delimitedList(expr) + rparen + Optional(dot | semicolon))
        literal = atom | Group(op_math + term + term)
        body = delimitedList(literal | Group(neg_kw + atom), comma)
        head = Group(ident + lparen + delimitedList(var | expr) + rparen)
        rule = Group(head + arrow + body + Optional(dot))

        aggregate = (count_kw | sum_kw | avg_kw | max_kw | min_kw | len_kw | mean_kw) + lparen + ident + rparen
        aggregate_rule = Group(head + arrow + body + aggregate + Optional(dot))
        rule_expr = Forward()
        rule_expr << (rule | aggregate_rule)
        return rule_expr.parseString(line)
    
    def stratify_rules(self,rules):
    # Trie les règles en fonction de leur niveau de dépendance
        levels = []
        while rules:
            # Trouve les règles qui ne dépendent pas d'autres règles
            independent_rules = [rule for rule in rules if not any(dep in rule[1] for dep in levels)]
            # Ajoute les règles indépendantes au niveau actuel
            levels.append(independent_rules)
            # Supprime les règles indépendantes de la liste des règles restantes
            rules = [rule for rule in rules if rule not in independent_rules]
        # Retourne les niveaux de règles triés
        return levels

    
    def eval_rule(self, rule, aggregate=None):
        # Évaluation d'une règle IDB avec agrégation COUNT
        print("Evaluating rule:", rule)

        if "COUNT" in rule:
            count_var = rule[-2]
            head, body, aggregate = rule[0], rule[1:-2], rule[-1]
            results = self.db.query(head[0], head[1:])
            total = len(results)
            self.edb_facts.append([count_var, total])
            self.db.add_fact(count_var, [total])
            print("COUNT", count_var, total)
            return total
        elif "SUM" in rule:
            # Évaluation d'une règle IDB avec agrégation SUM
            sum_var = rule[-2]
            head, body, aggregate = rule[0], rule[1:-2], rule[-1]
            results = self.db.query(head[0], head[1:])
            total = sum(result[aggregate[0]] for result in results)
            self.edb_facts.append([sum_var, total])
            self.db.add_fact(sum_var, [total])
            print("SUM", sum_var, total)
            return total
        elif "AVG" in rule:
            # Évaluation d'une règle IDB avec agrégation AVG
            avg_var = rule[-2]
            head, body, aggregate = rule[0], rule[1:-2], rule[-1]
            results = self.db.query(head[0], head[1:])
            total = sum(result[aggregate[0]] for result in results)
            avg = total / len(results)
            self.edb_facts.append([avg_var, avg])
            self.db.add_fact(avg_var, [avg])
            print("AVG", avg_var, avg)
            return avg
        elif "MAX" in rule:
            # Évaluation d'une règle IDB avec agrégation MAX
            max_var = rule[-2]
            head, body, aggregate = rule[0], rule[1:-2], rule[-1]
            results = self.db.query(head[0], head[1:])
            max_val = max(result[aggregate[0]] for result in results)
            self.edb_facts.append([max_var, max_val])
            self.db.add_fact(max_var, [max_val])
            print("MAX", max_var, max_val)
            return max_val
        elif "MIN" in rule:
            # Évaluation d'une règle IDB avec agrégation MIN
            min_var = rule[-2]
            head, body, aggregate = rule[0], rule[1:-2], rule[-1]
            results = self.db.query(head[0], head[1:])
            min_val = min(result[aggregate[0]] for result in results)
            self.edb_facts.append([min_var, min_val])
            self.db.add_fact(min_var, [min_val])
            print("MIN", min_var, min_val)
        elif "LEN" in rule:
            # Évaluation d'une règle IDB avec agrégation LEN
            len_var = rule[-2]
            head, body, aggregate = rule[0], rule[1:-2], rule[-1]
            results = self.db.query(head[0], head[1:])
            len_val = len(results)
            self.edb_facts.append([len_var, len_val])
            self.db.add_fact(len_var, [len_val])
            print("LEN", len_var, len_val)
            return len_val
        elif "MEAN" in rule:
            # Évaluation d'une règle IDB avec agrégation MEAN
            mean_var = rule[-2]
            head, body, aggregate = rule[0], rule[1:-2], rule[-1]
            results = self.db.query(head[0], head[1:])
            total = sum(result[aggregate[0]] for result in results)
            mean = total / len(results)
            self.edb_facts.append([mean_var, mean])
            self.db.add_fact(mean_var, [mean])
            print("MEAN", mean_var, mean)
            return mean
        else:
            # Évaluation d'une règle IDB sans agrégation
            head, body = rule[0], rule[1:]
            results = self.db.query(head[0], head[1:])
            for result in results:
                self.edb_facts.append([head[0]] + [result[str(i)] for i in range(1, len(head) + 1)])
                self.db.add_fact(head[0], [result[str(i)] for i in range(1, len(head) + 1)])
                print(head[0], [result[str(i)] for i in range(1, len(head) + 1)])
            return results


class Database:
    def __init__(self, edb_facts):
        self.edb_facts = edb_facts
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

    def is_aggregate_function(self, body):
        if not body:
            return False
        return body[0] in ["COUNT", "SUM", "AVG", "MIN", "MAX"]


    def filtre_dictionary(self, index, dictionary):
        return {key: value for key, value in dictionary.items() if key == index}

    def get_index(self, predicate, arguments):
        return self.edb_facts.index([predicate] + arguments)

    def get_index_condition(self, operator, value, dictionary):
        index = []
        for key, val in dictionary.items():
            if operator == "=" and val == value:
                index.append(key)
            elif operator == ">" and val > value:
                index.append(key)
            elif operator == "<" and val < value:
                index.append(key)
            elif operator == ">=" and val >= value:
                index.append(key)
            elif operator == "<=" and val <= value:
                index.append(key)
            elif operator == "!=" and val != value:
                index.append(key)
        return index

    def condition_rule(self, rule):
        return len(rule) == 1

    def split_rules(self, rules):
        if isinstance(rules, str):
            return rules.split(",")
        return rules

    def split_rules(self, rules):
        if isinstance(rules, str):
            return rules.split(",")
        return rules

    def extract_parts_in_rule(self, rule, index):
        if "(" not in rule or ")" not in rule:
            raise ValueError("Rule does not contain parentheses")
        parts = rule.split("(")[1].split(")")[0].split(",")
        var = parts[index].strip()
        value = parts[index+1].strip()
        return var, value

    def extract_inside(self, inside):
        return inside.split(",")

    def is_variable(self, rule):
        return rule[0].isupper()

    def is_constant(self, rule):
        return rule[0].islower()

    def query(self, predicate, arguments):
        return [fact for fact in self.edb_facts if fact[0] == predicate and fact[1:] == arguments]

    def get_aggregate(self, aggregate, dictionary):
        if aggregate == "SUM":
            return sum(dictionary.values())
        elif aggregate == "AVG":
            return sum(dictionary.values()) / len(dictionary)
        elif aggregate == "MAX":
            return max(dictionary.values())
        elif aggregate == "MIN":
            return min(dictionary.values())
        elif aggregate == "LEN":
            return len(dictionary)
        elif aggregate == "MEAN":
            return sum(dictionary.values()) / len(dictionary)

    def get_aggregate_function(self, rule):
        for agg in ["SUM", "AVG", "MAX", "MIN", "LEN", "MEAN"]:
            if agg in rule:
                return agg

    def extract_parts_in_condition(self, condition):
        return condition.split(" ")
    
    def add_fact(self, predicate, arguments):
        self.edb_facts.append([predicate] + arguments)
        datalog.edb_facts.append([predicate] + arguments)

    def evaluate_rule(self, rule):
        if self.condition_rule(rule):
            # Évaluation d'une règle IDB sans agrégation
            head, body = self.split_rules(rule)
            results = self.query(head[0], head[1:])
            for result in results:
                self.add_fact(head[0], [result[str(i)] for i in range(1, len(head) + 1)])
        elif self.is_aggregate_function(rule):
            # Évaluation d'une règle IDB avec agrégation
            head, body = self.split_rules(rule)
            results = self.query(head[0], head[1:])
            dictionary = {}
            for result in results:
                dictionary[result[body[0]]] = result[body[0]]
            self.add_fact(head[0], [self.get_aggregate(self.get_aggregate_function(rule), dictionary)])
        else:
            # Évaluation d'une règle EDB
            head, body = self.split_rules(rule)
            results = self.query(head[0], head[1:])
            for result in results:
                self.add_fact(head[0], [result[str(i)] for i in range(1, len(head) + 1)])
    
    def evaluate_rules(self):
        for rule in self.rules:
            head, body = rule[0], rule[1]
            if self.condition_rule(body):
                # Évaluation d'une règle IDB sans agrégation
                results = self.query(head[0], head[1:])


if __name__ == "__main__":
    datalog = Datalog()
    datalog.load_file("data/Exemple1.dl")

    with open("output.txt", "w") as f:
        f.write("EDB:\n")
        # Écrire les faits EDB sans les [] et les mots ParseResults
        for fact in datalog.db.edb_facts:
            f.write(str(fact) + "\n")


        f.write("\nIDB:\n")
        # Écrire les règles IDB
        for rule in datalog.db.rules:
            f.write(str(rule) + "\n")

            
        f.write("\nIDB with aggregation:\n")
        # Écrire les règles IDB avec agrégation
        for rule in datalog.db.idb_rules:
            rule_list = datalog.db.split_rules(rule)
            f.write(str(rule_list) + "\n")


        # compte le nombre d'artists
        count_result = datalog.eval_rule(['COUNT', 'X', ':-', 'artist(X, _, _)'])
        f.write("COUNT artists: " + str(count_result) + "\n")
