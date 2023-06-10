from pyparsing import *

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
                    term = Group(expr + lparen + delimitedList(expr | op_math) + rparen)
                    atom = Group(ident + lparen + delimitedList(expr) + rparen + Optional(dot | semicolon))
                    literal = atom | Group(op_math + term + term)
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
                    parsed = rule_expr.parseString(line)
                    self.rules.append(parsed)
                    self.idb_rules.append(parsed)
                else:
                    print("Error parsing line: " + line)
                    
        self.eval_rules()


    def query(self, predicate, arguments):
        # Convert the predicate string to a list
        predicate_list = [predicate]

        # Create a new rule with the given predicate and arguments
        rule = predicate_list + arguments

        # Evaluate the rule and return the result
        result = self.eval_rule(rule)
        return result

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


    def eval_rules(self):
        # Évalue les règles IDB en utilisant la stratification
        levels = self.stratify_rules(self.idb_rules)
        for level in levels:
            for rule in level:
                self.eval_rule(rule)

    def eval_rule(self, rule):
        # Évaluation d'une règle IDB avec agrégation COUNT
        if "COUNT" in rule:
            count_var = rule[-2]
            head, body, aggregate = rule[0], rule[1:-2], rule[-1]
            results = self.query(head[0], head[1:])
            count = len(results)
            self.edb_facts.append([count_var, count])
            print("COUNT", count_var, count)
        elif "SUM" in rule:
            # Évaluation d'une règle IDB avec agrégation SUM
            sum_var = rule[-2]
            head, body, aggregate = rule[0], rule[1:-2], rule[-1]
            results = self.query(head[0], head[1:])
            total = sum(result[aggregate[0]] for result in results)
            self.edb_facts.append([sum_var, total])
            print("SUM", sum_var, total)
        elif "AVG" in rule:
            # Évaluation d'une règle IDB avec agrégation AVG
            avg_var = rule[-2]
            head, body, aggregate = rule[0], rule[1:-2], rule[-1]
            results = self.query(head[0], head[1:])
            total = sum(result[aggregate[0]] for result in results)
            avg = total / len(results)
            self.edb_facts.append([avg_var, avg])
            print("AVG", avg_var, avg)
        elif "MAX" in rule:
            # Évaluation d'une règle IDB avec agrégation MAX
            max_var = rule[-2]
            head, body, aggregate = rule[0], rule[1:-2], rule[-1]
            results = self.query(head[0], head[1:])
            max_val = max(result[aggregate[0]] for result in results)
            self.edb_facts.append([max_var, max_val])
            print("MAX", max_var, max_val)
        elif "MIN" in rule:
            # Évaluation d'une règle IDB avec agrégation MIN
            min_var = rule[-2]
            head, body, aggregate = rule[0], rule[1:-2], rule[-1]
            results = self.query(head[0], head[1:])
            min_val = min(result[aggregate[0]] for result in results)
            self.edb_facts.append([min_var, min_val])
            print("MIN", min_var, min_val)
        elif "LEN" in rule:
            # Évaluation d'une règle IDB avec agrégation LEN
            len_var = rule[-2]
            head, body, aggregate = rule[0], rule[1:-2], rule[-1]
            results = self.query(head[0], head[1:])
            len_val = len(results)
            self.edb_facts.append([len_var, len_val])
            print("LEN", len_var, len_val)
        elif "MEAN" in rule:
            # Évaluation d'une règle IDB avec agrégation MEAN
            mean_var = rule[-2]
            head, body, aggregate = rule[0], rule[1:-2], rule[-1]
            results = self.query(head[0], head[1:])
            total = sum(result[aggregate[0]] for result in results)
            mean = total / len(results)
            self.edb_facts.append([mean_var, mean])
            print("MEAN", mean_var, mean)
        else:
            # Évaluation d'une règle IDB sans agrégation
            head, body = rule[0], rule[1:]
            results = self.query(head[0], head[1:])
            for result in results:
                self.edb_facts.append([head[0]] + [result[str(i)] for i in range(1, len(head) + 1)])
                print(head[0], [result[str(i)] for i in range(1, len(head) + 1)])

            

if __name__ == "__main__":
    datalog = Datalog()
    datalog.load_file("data/Exemple1.dl")

    with open("output.txt", "w") as f:
        f.write("EDB:\n")
        # Écrire les faits EDB
        for fact in datalog.edb_facts:
            f.write(" ".join(map(str, fact)) + "\n")

        f.write("\nIDB:\n")
        # Écrire les règles IDB
        for rule in datalog.rules:
            rule_list = rule.asList()
            if len(rule_list) == 1:
                f.write(str(rule_list[0]) + "\n")
            else:
                head, arrow, body, dot = rule_list
                f.write(" ".join(map(str, [head, arrow, body])) + "\n")
            
        f.write("\nIDB with aggregation:\n")
        # Écrire les règles IDB avec agrégation
        for rule in datalog.idb_rules:
            rule_list = rule.asList()
            if len(rule_list) == 1:
                f.write(str(rule_list[0]) + "\n")
            else:
                head, arrow, body, aggregate, dot = rule_list
                f.write(" ".join(map(str, [head, arrow, body, aggregate])) + "\n")

        # compte le nombre d'artists
        f.write("\nCOUNT artists:\n")
        

        datalog.query("artists", ["X", "'John'", "Y"])