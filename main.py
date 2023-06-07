import re

EDB = [] # List of relations
IDB = [] # List of rules
types = []  # List of types

filename = "data/Exemple1.dl"

# Functions

def parser(file):
    if file.endswith(".dl"):

        with open(file, "r") as f:
            lines = f.readlines()
            for line in lines:
                line = line.strip()
                if line.startswith(":-"):
                    types.append(line[2:])
                elif line.startswith("%"):
                  pass
                elif ":-" in line:
                  IDB.append(line)
                elif line =="":
                  pass
                else:
                  EDB.append(line)

        print("Relations EDB:")
        for relation in EDB:
            print(relation)

        print("\nRègles IDB:")
        for rule in IDB:
            print(rule)
    else:
        print("Le fichier d'entrée doit avoir l'extension .dl")

print("Parser : ", parser(filename))

#Fonction pour extraire les parties d'une fonction : regle et arguments
def extract_parts_in_rule(expression):
  outside = re.findall(r'^([^()]+)', expression)[0].strip()
  inside = re.findall(r'\(([^()]+)\)', expression)
  return outside, inside




tab_edb = [[] for _ in range(len(EDB))] #Tableau pour stocker les relations et les arguments de l'EDB

i=0
for line in EDB:
  print(line)
  relation, arguments = extract_parts_in_rule(line)
  print("Relation:", relation)
  print("Arguments:", arguments)
  tab_edb[i].append(relation)

  arguments = ','.join(arguments)
  arguments = arguments.split(',')
  for arg in arguments:
    tab_edb[i].append(arg)
  i=i+1

print("Tableau EDB :", tab_edb)

#Transforme les nombres (string) en int
for line in tab_edb:
    for j in range(len(line)):
        if line[j].isdigit():
            line[j] = int(line[j])
        else: 
          line[j] = line[j].strip("'")

heads = []
bodies = []

#Sépare les idb en heads et bodies
def split_rule(line):
    line = line.strip()
    if ":-" in line:
        head, body = line.split(":-", 1)
        head = head.replace("%", "")
        body = body.rstrip('.')
        return head.strip(), body.strip()
    else:
        print("La règle est mal formée:", line)
        return None, None

for rule in IDB:
    rule = rule.strip()
    head, body = split_rule(rule)
    heads.append(head)
    bodies.append(body)

for i in range(len(heads)):
    print("Head:", heads[i])
    print("Body:", bodies[i])


rules_in_head = [[] for _ in range(len(heads))]
i=0
for head in heads:
  j=0
  head_elements = re.split(r",(?![^(]*\))", head)
  for element in head_elements:
    rules_in_head[i].append(element)
    j=j+1
  i=i+1


#Fonction pour extraire les parties d'une fonction : regle et arguments
def extract_parts_in_rule(expression):
  outside = re.findall(r'^([^()]+)', expression)[0].strip()
  inside = re.findall(r'\(([^()]+)\)', expression)
  return outside, inside


def extract_inside(inside):
  tmp = []
  inside = ','.join(inside)
  inside = inside.split(',')
  for i in inside:
    tmp.append(i)
  return tmp



def is_aggregate_function(rule):
  outside, inside = extract_parts_in_rule(rule)
  if outside == "COUNT":
    return True
  elif outside == "SUM":
    return True
  elif outside == "AVG":
    return True
  else:
    return False
  

def split_rules(rules):
  tab_rules = []
  elements = re.split(r",(?![^(]*\))", rules)
  for element in elements:
    tab_rules.append(element)
  
  return tab_rules



def find_index_parameters(inside):
    indices = []
    for i, val in enumerate(inside):
        if val != '_':
            indices.append(i)
    return indices


def create_dictionary(params):
    dictionary = {}
    for param in params:
        if param != '_':
            dictionary[param] = []
    return dictionary


def find_in_edb(outside, parameters, edb, dictionary):

    tab_index_parameters = find_index_parameters(parameters)
    dictionary = create_dictionary(parameters)

    for line in edb:
        if line[0] == outside:
            for val in tab_index_parameters:
                parameter = parameters[val]
                value = line[val+1]
                if parameter in dictionary:
                    dictionary[parameter].append(value)
    return dictionary



def extract_parts_in_condition(condition):
    match = re.search(r"\((\w+)([<>=])'?(\w+)'?\)", condition)

    if match:
        variable = match.group(1)
        operator = match.group(2)
        value = match.group(3)

        return variable, operator, value
    else:
        print("Aucune correspondance trouvée.")


def get_index_condition(var, operator, value, dictionary):
    index = []
    if var in dictionary:
        for indice, val in enumerate(dictionary[var]):
            if operator == "=" and val == value:
                index.append(indice)
            elif operator == "<" and val < int(value):
                index.append(indice)
            elif operator == ">" and val > int(value):
                index.append(indice)

    return index

def filtre_dictionary(index, dictionary):
  result = {cle: [valeur[indice] for indice in index] for cle, valeur in dictionary.items() if cle in dictionary}
  return result


def calculate_aggregation(dictionary, key, aggregate_function):
  values = dictionary.get(key, [])
  if aggregate_function == "COUNT":
      return len(values)
  elif aggregate_function == "SUM":
      numeric_values = [val for val in values if isinstance(val, (int, float))]
      return sum(numeric_values) if numeric_values else None
  elif aggregate_function == "AVG":
      numeric_values = [val for val in values if isinstance(val, (int, float))]
      return sum(numeric_values) / len(numeric_values) if numeric_values else None
  

def condition_rule(rule):
  if '=' in rule:
    return True
  elif '<' in rule:
    return True
  elif '>' in rule:
    return True
  else:
    return False


#Pour chaque regle à calculer dans idb
for rule in IDB:
  dictionary = {}
  print(rule)

  #Separe la regle
  head, body = split_rule(rule)

  #Separe les regles dans head
  rules_in_head = split_rules(head)

  #on sépare les regles du body
  rules_in_body = split_rules(body)

  ############### HEAD #####################################

  bool_isAggregate = False
  vars_in_head = []

  #pour chaque regle dans head
  for rule_in_head in rules_in_head:
    outside_head, inside_head = extract_parts_in_rule(rule_in_head)

    #on recupere les toutes les variables dans head
    vars_inside_head = extract_inside(inside_head)
    vars_in_head += vars_inside_head

    #true si il y a une aggregation
    if is_aggregate_function(outside_head):
      bool_isAggregate = True
 
 ################## BODY ################################

  if bool_isAggregate:

    #pour chaque regle dans body
    for rule_in_body in rules_in_body:

      dictionary_rule = {}

      #si c'est une regle à calculer
      if not condition_rule(rule_in_body):
        outside_body, inside_body = extract_parts_in_rule(rule_in_body)
        parameters_body = extract_inside(inside_body)

        #on trouve les lignes correspondantes dans l'edb et on ajoute dans dictionary
        dictionary_rule = find_in_edb(outside_body, parameters_body, tab_edb, dictionary)
        dictionary.update(dictionary_rule)
      
      #si c'est une condition de filtrage
      else:
        #on extrait la variable et la valeur de condition
        var, operator, value = extract_parts_in_condition(rule_in_body)
        print("Operator :", operator)
        #si la variable est dans le dictionnaire
        if var in dictionary:
          
          #on recupere les indices qui correspondent à la condition
          indice_to_keep = get_index_condition(var, operator, value, dictionary)
          #on filtre le dictionnaire
          dictionary = filtre_dictionary(indice_to_keep, dictionary)

        else:
          print("Rien à filtrer")
    
    #gerer l'agregation
    result_aggregation = calculate_aggregation(dictionary, vars_in_head[0], outside_head)
    print("Résultat :", result_aggregation)

  else:
    print("Rule not an aggregation")

  print("Liste :", dictionary)
  print("")
