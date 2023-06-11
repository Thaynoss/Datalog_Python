from pyparsing import *

class parserDatalog:
    """
    Classe pour analyser des faits, des requêtes et des règles Datalog.
    """
    # Définir les symboles terminaux

    IDENT = quotedString | Word(alphas + "_", alphanums + "_") | Word(alphas.lower(), alphanums + "_") | Word(alphas.upper(), alphanums + "_") | Word(alphas + "_", alphanums + "_")
    VAR = Word(alphas.upper(), alphanums + "_") | Word(alphas.lower(), alphanums + "_")
    COMPARISON_OP = Literal("=") | Literal("!=") | Literal("<") | Literal(">") | Literal("<=") | Literal(">=") | Literal("==")
    COMMA = Literal(",")
    DOT = Literal(".")
    SEMICOLON = Literal(";")
    LPAREN = Literal("(").suppress() | Literal(":(").suppress()
    RPAREN = Literal(":)").suppress() | Literal(")").suppress()
    COLON = Literal(":").suppress()
    ARROW = Literal(":-") | Literal("->") | Literal("<-")

    COUNT_KW = CaselessLiteral("COUNT") | CaselessLiteral("COUNT_DISTINCT")
    SUM_KW = CaselessLiteral("SUM") | CaselessLiteral("SUM_DISTINCT")
    AVG_KW = CaselessLiteral("AVG") | CaselessLiteral("AVG_DISTINCT")
    MAX_KW = CaselessLiteral("MAX") | CaselessLiteral("MAX_DISTINCT")
    MIN_KW = CaselessLiteral("MIN") | CaselessLiteral("MIN_DISTINCT")
    LEN_KW = CaselessLiteral("LEN") | CaselessLiteral("LEN_DISTINCT")
    MEAN_KW = CaselessLiteral("MEAN") | CaselessLiteral("MEAN_DISTINCT")
    NEG_KW = CaselessLiteral("NOT") | CaselessLiteral("NOT_DISTINCT")
    OP_MATH = oneOf("+ - * /")
    
    # Définir les règles de grammaire pour les faits Datalog
    fact = Forward()
    fact << Group(IDENT + LPAREN + delimitedList(IDENT | VAR) + RPAREN + DOT)
    

    # Définir les règles de grammaire pour les requêtes Datalog
    query = Forward()
    query << Group(IDENT + LPAREN + delimitedList(IDENT | VAR) + RPAREN + DOT)

    # Définir les règles de grammaire pour les règles Datalog
    rule = Forward()
    rule << Group(IDENT + LPAREN + delimitedList(IDENT | VAR) + RPAREN + ARROW + delimitedList(IDENT | VAR, delim=COMMA) + DOT)

    def __init__(self):
        """
        Initialise une instance de la classe DatalogParser.
        """
        self.query = self.query
        self.fact = self.fact
        self.rule = self.rule


    def parse_query(self, query_string):
        """
        Parse une requête Datalog et retourne un objet de type Query.
        """
        return self.query.parseString(query_string)[0]
    
    def parse_fact(self, fact_string):
        """
        Parse un fait Datalog et retourne un objet de type Fact.
        """
        self.EXPR = self.IDENT | self.VAR | Word(nums) | quotedString
        self.TERM = Group(self.EXPR + self.LPAREN + delimitedList(self.EXPR | self.OP_MATH) + self.RPAREN)
        self.ATOM = Group(self.IDENT + self.LPAREN + delimitedList(self.EXPR) + self.RPAREN + Optional(self.DOT | self.SEMICOLON))
        self.LITERAL = self.ATOM | Group(self.OP_MATH + self.TERM + self.TERM)
        body = delimitedList(self.LITERAL | Group(self.NEG_KW + self.ATOM), self.COMMA)
        head = Group(self.IDENT + self.LPAREN + delimitedList(self.VAR  | self.EXPR) + self.RPAREN)
        fact = head + Optional(self.DOT | self.SEMICOLON)
        return fact.parseString(fact_string)[0]
    
    
    def parse_rule(self, line):
        # Définir les expressions Datalog IDB
        try:
            self.EXPR = self.IDENT | self.VAR | Word(nums) | quotedString
            self.TERM = Group(self.EXPR + self.LPAREN + delimitedList(self.EXPR | self.OP_MATH) + self.RPAREN)
            self.ATOM = Group(self.IDENT + self.LPAREN + delimitedList(self.EXPR) + self.RPAREN + Optional(self.DOT | self.SEMICOLON))
            self.LITERAL = self.ATOM | Group(self.OP_MATH + self.TERM + self.TERM)
            body = delimitedList(self.LITERAL | Group(self.NEG_KW + self.ATOM), self.COMMA)
            head = Group(self.IDENT + self.LPAREN + delimitedList(self.VAR  | self.EXPR) + self.RPAREN)
            rule = Group(head + self.ARROW + body + Optional(self.DOT))

            self.aggregate = Group(self.COUNT_KW | self.SUM_KW | self.AVG_KW | self.MAX_KW | self.MIN_KW | self.LEN_KW | self.MEAN_KW)
            self.ggregate_rule = Group(self.aggregate + self.LPAREN + self.IDENT + self.COMMA + self.IDENT + self.RPAREN + self.ARROW + self.IDENT + self.LPAREN + self.IDENT + self.RPAREN + Optional(self.DOT))
            self.rule_expr = Forward()
            self.rule_expr << (self.ggregate_rule | rule)
        except:
            pass
        return self.rule_expr.parseString(line)[0]
    
    def parse_head(self, line):
        # Définir les expressions Datalog IDB
        try:
            self.EXPR = self.IDENT | self.VAR | Word(nums) | quotedString
            self.TERM = Group(self.EXPR + self.LPAREN + delimitedList(self.EXPR | self.OP_MATH) + self.RPAREN)
            self.ATOM = Group(self.IDENT + self.LPAREN + delimitedList(self.EXPR) + self.RPAREN + Optional(self.DOT | self.SEMICOLON))
            self.LITERAL = self.ATOM | Group(self.OP_MATH + self.TERM + self.TERM)
            body = delimitedList(self.LITERAL | Group(self.NEG_KW + self.ATOM), self.COMMA)
            head = Group(self.IDENT + self.LPAREN + delimitedList(self.VAR  | self.EXPR) + self.RPAREN)
            rule = Group(head + self.ARROW + body + Optional(self.DOT))

            self.aggregate = Group(self.COUNT_KW | self.SUM_KW | self.AVG_KW | self.MAX_KW | self.MIN_KW | self.LEN_KW | self.MEAN_KW)
            self.ggregate_rule = Group(self.aggregate + self.LPAREN + self.IDENT + self.COMMA + self.IDENT + self.RPAREN + self.ARROW + self.IDENT + self.LPAREN + self.IDENT + self.RPAREN + Optional(self.DOT))
            self.rule_expr = Forward()
            self.rule_expr << (self.ggregate_rule | rule)
        except:
            pass
        return self.rule_expr.parseString(line)[0][0]
    
    def parse_body(self, line):
        # Définir les expressions Datalog IDB
        try:
            self.EXPR = self.IDENT | self.VAR | Word(nums) | quotedString
            self.TERM = Group(self.EXPR + self.LPAREN + delimitedList(self.EXPR | self.OP_MATH) + self.RPAREN)
            self.ATOM = Group(self.IDENT + self.LPAREN + delimitedList(self.EXPR) + self.RPAREN + Optional(self.DOT | self.SEMICOLON))
            self.LITERAL = self.ATOM | Group(self.OP_MATH + self.TERM + self.TERM)
            body = delimitedList(self.LITERAL | Group(self.NEG_KW + self.ATOM), self.COMMA)
            head = Group(self.IDENT + self.LPAREN + delimitedList(self.VAR  | self.EXPR) + self.RPAREN)
            rule = Group(head + self.ARROW + body + Optional(self.DOT))

            self.aggregate = Group(self.COUNT_KW | self.SUM_KW | self.AVG_KW | self.MAX_KW | self.MIN_KW | self.LEN_KW | self.MEAN_KW)
            self.ggregate_rule = Group(self.aggregate + self.LPAREN + self.IDENT + self.COMMA + self.IDENT + self.RPAREN + self.ARROW + self.IDENT + self.LPAREN + self.IDENT + self.RPAREN + Optional(self.DOT))
            self.rule_expr = Forward()
            self.rule_expr << (self.ggregate_rule | rule)
        except:
            pass
        return self.rule_expr.parseString(line)[0][1]
    
    def parse_predicate_name(self, predicate):
        """
        Renvoie le nom d'un prédicat Datalog.
        """
        # Récupérer le nom du prédicat
        predicate_name = predicate[0]
        # Renvoyer le nom du prédicat
        return predicate_name
    
    def parse_predicate_args(self, predicate):
        """
        Renvoie les arguments d'un prédicat Datalog.
        """
        # Récupérer les arguments du prédicat
        predicate_args = predicate[1:-1]
        # Renvoyer les arguments du prédicat
        return predicate_args
    
    def parse_predicate(self, predicate):
        """
        Renvoie un prédicat Datalog.
        """
        # Récupérer le nom du prédicat
        predicate_name = self.parse_predicate_name(predicate)
        # Récupérer les arguments du prédicat
        predicate_args = self.parse_predicate_args(predicate)
        # Renvoyer le prédicat
        return (predicate_name, predicate_args)
    
    def parse_rule_head(self, rule):
        """
        Renvoie la tête d'une règle Datalog.
        """
        # Récupérer la tête de la règle
        rule_head = rule[0]
        # Renvoyer la tête de la règle
        return rule_head
    
    def parse_rule_body(self, rule):
        """
        Renvoie le corps d'une règle Datalog.
        """
        # Récupérer le corps de la règle
        rule_body = rule[1]
        # Renvoyer le corps de la règle
        return rule_body
    


if __name__ == "__main__":
    # Créer une instance de la classe DatalogParser
    parser = parserDatalog()

    # Analyser un fait Datalog
    fact_string = "artist(\"The Beatles\", \"Liverpool\")."

    fact = parser.parse_fact(fact_string)
    #print(fact)

    # Analyser une règle Datalog
    rule_string = "ancestor(X, Y) :- parent(X, Z), ancestor(Z, Y)."
    rule = parser.parse_rule(rule_string)
    #print(rule)