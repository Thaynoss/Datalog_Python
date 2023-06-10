from pyparsing import *

# Définir les symboles Datalog
ident = quotedString | Word(alphas + "_", alphanums + "_") | Word(alphas.lower(), alphanums + "_") | Word(alphas.upper(), alphanums + "_")
var = Word(alphas.upper(), alphanums + "_") | Word(alphas.lower(), alphanums + "_")
op = Literal("=") | Literal("!=") | Literal("<") | Literal(">") | Literal("<=") | Literal(">=")
comma = Literal(",") | Literal(":-")
dot = Literal(".") | Literal(":-")
semicolon = Literal(";") | Literal(":-")
lparen = Literal("(").suppress() | Literal(":(").suppress()
rparen = Literal(":)").suppress() | Literal(")").suppress()
colon = Literal(":").suppress() | Literal(":-")
arrow = Literal(":-") | Literal(":")
count_kw = CaselessLiteral("COUNT") | CaselessLiteral("COUNT_DISTINCT")
sum_kw = CaselessLiteral("SUM") | CaselessLiteral("SUM_DISTINCT")
avg_kw = CaselessLiteral("AVG") | CaselessLiteral("AVG_DISTINCT")
max_kw = CaselessLiteral("MAX") | CaselessLiteral("MAX_DISTINCT")
min_kw = CaselessLiteral("MIN") | CaselessLiteral("MIN_DISTINCT") 
len_kw = CaselessLiteral("LEN") | CaselessLiteral("LEN_DISTINCT")
mean_kw = CaselessLiteral("MEAN") | CaselessLiteral("MEAN_DISTINCT")
neg_kw = CaselessLiteral("NOT") | CaselessLiteral("NOT_DISTINCT")
op = oneOf("+ - * /")

class Datalog:
    def __init__(self):
        self.edb_facts = []
        self.rules = []
        self.idb_rules = []

    def load_file(self, filename):
        with open(filename, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                elif ":-" not in line and line.endswith("."):
                    # Définir les expressions Datalog EDB
                    expr = ident | var | Word(nums)
                    term = Group(expr + lparen + delimitedList(expr | op) + rparen)
                    atom = Group(ident + lparen + delimitedList(expr) + rparen + Optional(dot | semicolon))
                    literal = atom | Group(op + term + term)
                    body = delimitedList(literal | Group(neg_kw + atom), comma)
                    head = Group(ident + lparen + delimitedList(var | expr) + rparen)
                    fact = head + Optional(dot | semicolon)
                    try:
                        parsed = fact.parseString(line)
                        self.edb_facts.append(parsed)
                    except ParseException as e:
                        print(e)
                        print("Error parsing fact: " + line)
                    
                elif ":-" in line and line.endswith("."):

                    # Définir les expressions Datalog IDB
                    expr = ident | var | Word(nums)
                    term = Group(expr + lparen + delimitedList(expr | op) + rparen)
                    atom = Group(ident + lparen + delimitedList(expr) + rparen + Optional(dot | semicolon))
                    literal = atom | Group(op + term + term)
                    body = delimitedList(literal | Group(neg_kw + atom), comma)
                    head = Group(ident + lparen + delimitedList(var | expr) + rparen)
                    rule = Group(head + arrow + body + Optional(dot))

                    aggregate = (count_kw | sum_kw | avg_kw | max_kw | min_kw | len_kw | mean_kw) + lparen + ident + rparen
                    aggregate_rule = Group(head + arrow + body + aggregate + Optional(dot))
                    rule_expr = Forward()
                    rule_expr << (rule | aggregate_rule)
                    parsed = rule_expr.parseString(line)
                    if aggregate_rule in parsed:
                        print("Aggregate rule:", parsed)
                    self.rules.append(parsed)
                    if aggregate_rule in parsed:
                        self.idb_rules.append(parsed)
        self.eval_rules()


    def query(self, predicate, args):
        # Recherche les faits correspondants à un prédicat et des arguments donnés
        results = []
        for fact in self.edb_facts:
            if fact[0] == predicate and fact[1:] == args:
                results.append(dict(zip(["predicate"] + list(range(1, len(args) + 1)), fact)))
        return results

    def eval_rules(self):
        # Évalue les règles IDB en utilisant la stratification
        idb_rules = self.idb_rules.copy()
        level = 0
        while idb_rules:
            # Trouver les règles IDB qui ne dépendent pas d'autres règles IDB
            independent_rules = []
            for rule in idb_rules:
                if not any(rule[0][0] == r[0][0] for r in idb_rules if r != rule):
                    independent_rules.append(rule)
            # Évaluer les règles IDB indépendantes
            for rule in independent_rules:
                self.eval_rule(rule)
                idb_rules.remove(rule)
            level += 1
        if idb_rules:
            raise Exception("Circular dependency in IDB rules")
    
    def eval_rule(self, rule):
        # Évalue une règle IDB
        head, body = rule[0], rule[1:]
        if count_kw in body:
            # Évaluation d'une règle IDB avec une agrégation COUNT
            count_var = body[-2]
            body = body[:-2]
            results = self.query(head[0], head[1:])
            count = len(results)
            for result in results:
                self.edb_facts.append([count_var, count] + [result[str(i)] for i in range(1, len(head) + 1)])
        else:
            # Évaluation d'une règle IDB normale
            results = []
            for literal in body:
                if isinstance(literal, str):
                    # Recherche de faits correspondants à un prédicat et des arguments donnés
                    results.append(self.query(literal, []))
                elif isinstance(literal, ParseResults):
                    # Évaluation d'une expression Datalog
                    op, arg1, arg2 = literal
                    if op == "==":
                        results.append(self.query(arg1[0], arg1[1:]))
                    elif op == "!=":
                        all_results = self.query(arg1[0], arg1[1:])
                        results.append([result for result in self.edb_facts if result not in all_results])
                    elif op == "<=":
                        all_results = self.query(arg1[0], arg1[1:])
                        results.append([result for result in self.edb_facts if result in all_results or result < arg2])
                    elif op == ">=":
                        all_results = self.query(arg1[0], arg1[1:])
                        results.append([result for result in self.edb_facts if result in all_results or result > arg2])
                    elif op == "<":
                        all_results = self.query(arg1[0], arg1[1:])
                        results.append([result for result in self.edb_facts if result not in all_results and result < arg2])
                    elif op == ">":
                        all_results = self.query(arg1[0], arg1[1:])
                        results.append([result for result in self.edb_facts if result not in all_results and result > arg2])
            # Intersection des résultats
            if results:
                intersection = set(results[0])
                for r in results[1:]:
                    intersection &= set(r)
                # Ajout des nouveaux faits à la base de données
                for fact in intersection:
                    self.edb_facts.append([head[0]] + list(fact.values())[1:])


if __name__ == "__main__":
    datalog = Datalog()
    datalog.load_file("data/Exemple1.dl")
    
    # Count the number of films
    print("Number of films:", len(datalog.query("films", [])))

    with open("output.txt", "w") as f:
        # Écrire les faits EDB
        for fact in datalog.edb_facts:
            head, dot = fact.asList()
            f.write(str(head) + "\n")
        # Écrire les règles IDB
        for rule in datalog.rules:
            rule_list = rule.asList()
            if len(rule_list) == 1:
                f.write(str(rule_list[0]) + "\n")
            else:
                head, arrow, body, dot = rule_list
                f.write(str(head) + " " + str(arrow) + " " + str(body) + "\n")
        # Écrire les règles IDB avec agrégation
        for rule in datalog.idb_rules:
            rule_list = rule.asList()
            if len(rule_list) == 1:
                f.write(str(rule_list[0]) + "\n")
            else:
                head, arrow, body, aggregate, dot = rule_list
                f.write(str(head) + " " + str(arrow) + " " + str(body) + " " + str(aggregate) + "\n")

