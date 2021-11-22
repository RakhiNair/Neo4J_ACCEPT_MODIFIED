from neo4j import GraphDatabase
import amrlib
import pandas as pd
import time
import numpy

# NEO4J Import
# local file in import folder, otherwise https://neo4j.com/developer/kb/import-csv-locations/
file = 'file:///Webis-argument-framing.csv'

# Pandas Import for AMR generation
pandas_file = "O:/Arbeit/Webis-argument-framing.csv"


class Neo4J:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def create_node(self):
        with self.driver.session() as session:
            session.write_transaction(self._create_node)

    def create_relationship(self):
        with self.driver.session() as session:
            session.write_transaction(self._create_relationship)

    def create_amr(self, root, node):
        with self.driver.session() as session:
            session.write_transaction(self._create_amr, root, node)

    def create_amr_rel(self, root, node, info):
        with self.driver.session() as session:
            session.write_transaction(self._create_amr_rel, root, node, info)

    def connect_amr(self, text, info, path):
        with self.driver.session() as session:
            session.write_transaction(self._connect_amr, text, info, path)

    @staticmethod
    def _create_node(tx):
        tx.run("LOAD CSV WITH HEADERS FROM $file AS row "
               "WITH row WHERE row.argument_id IS NOT NULL "
               "MERGE (:argument {argument_id: toInteger(row.argument_id), frame_id: toInteger(row.frame_id), "
               "frame: row.frame, topic_id: toInteger(row.topic_id), topic: row.topic, premise: row.premise, "
               "stance: row.stance, "
               "conclusion: row.conclusion, source: $file}) "
               "MERGE (:frame {frame_id: toInteger(row.frame_id), name: row.frame}) "
               "MERGE (:topic {topic_id: toInteger(row.topic_id), name: row.topic}) "
               "MERGE (:stance {stance: row.stance}) "
               "MERGE (:amr {argument_id: toInteger(row.argument_id), source: $file, name: $amr})", file=file, amr="AMR")

    @staticmethod
    def _create_relationship(tx):
        tx.run("MATCH (a:argument), (b:frame) "
               "WHERE a.frame_id=b.frame_id "
               "MERGE (a)-[c:FRAME {name: a.topic + '<->' + b.name}]->(b)")
        tx.run("MATCH (a:argument), (b:topic) "
               "WHERE a.topic_id=b.topic_id "
               "MERGE (a)-[c:TOPIC {name: a.topic}]->(b)")
        tx.run("MATCH (a:argument), (b:stance) "
               "WHERE a.stance=b.stance "
               "MERGE (a)-[c:STANCE {name: a.stance}]->(b)")
        tx.run("MATCH (a:argument), (b:amr) "
               "WHERE a.argument_id=b.argument_id "
               "MERGE (a)-[c:AMR]->(b)")

    @staticmethod
    def _create_amr(tx, root, node):
        tx.run("MERGE (a:amr {argument_id: $id, source: $source, type: $type, name: $name1, "
               "identifier: $ident1, relationship: $rel1}) "
               "MERGE (b:amr {argument_id: $id, source: $source, type: $type, name: $name2, "
               "identifier: $ident2, relationship: $rel2}) "
               f"MERGE (a)-[:{node[6]}]->(b)", id=root[0], source=root[1], type=root[2], name1=root[3], name2=node[3],
               ident1=root[4], ident2=node[4], rel1=root[5], rel2=node[5])

    @staticmethod
    def _create_amr_rel(tx, root, node, info):
        tx.run("MERGE (a:amr {name: $name1, type: $type1, relationship: $rel1, argument_id: $id}) "
               "MERGE (b:amr {name: $name2, type: $type2, relationship: $rel2, argument_id: $id}) "
               "MERGE (a)-[c:RELATIONSHIP {name: b.relationship}]->(b)", name1=root[1], type1=root[0], name2=node[1],
               type2=node[0], rel1=root[2], rel2=node[2], id=info)

    @staticmethod
    def _connect_amr(tx, text, info, path):
        tx.run("MERGE (a:amr {name: $name1, type: $type1, relationship: $rel1, argument_id: $id}) ", name1=text[1],
               type1=text[0], rel1=text[2], id=info)
        tx.run("MATCH (a:amr), (b:amr) "
               "WHERE a.argument_id=b.argument_id AND a.source=$file AND b.name=$name "
               f"MERGE (a)-[:{path}]->(b)", file=file, name=text[1])


def create_amr_help(inputs):
    info_list = []  # 0 = type, 1 = name
    if inputs.find("(") != -1:
        info_list.append(inputs[inputs.find("(") + 1:inputs.rfind(" ") - 2])
        if inputs.find(")") == -1:
            info_list.append(inputs[inputs.find("/ ") + 2:])
        else:
            info_list.append(inputs[inputs.find("/ ") + 2:inputs.find(")")])
    else:
        if inputs.find(" \"") == -1:
            info_list.append(inputs[inputs.rfind(" ") + 1:])
            info_list.append("None")
        else:
            info_list.append("None")
            info_list.append(inputs[inputs.find(" \"") + 2:inputs.find(")") - 1])
    # relations
    if inputs.find(":") == -1:
        info_list.append("No relationships")
    else:
        if inputs.find("(") != -1:
            info_list.append(inputs[inputs.find(":"):inputs.find("(") - 1])
        else:
            info_list.append(inputs[inputs.find(":"):inputs.rfind(" ")])

    return info_list


def create_amr(amr_creation_input, path):
    # [AMR, id], premise/conclusion
    graph = amr_creation_input[0]
    arg_id = amr_creation_input[1]
    check = True
    i = 1
    app.connect_amr(create_amr_help(graph.splitlines()[i]), arg_id, path)
    current_space = 0
    while i + 1 < len(graph.splitlines()):
        if len(graph.splitlines()[i + 1]) - len(graph.splitlines()[i + 1].lstrip(' ')) > current_space:
            # print(inputs.splitlines()[i] + " -> " + inputs.splitlines()[i + 1])
            app.create_amr_rel(create_amr_help(graph.splitlines()[i]), create_amr_help(graph.splitlines()[i + 1]),
                               arg_id)
            current_space = len(graph.splitlines()[i + 1]) - len(graph.splitlines()[i + 1].lstrip(' '))
        elif len(graph.splitlines()[i + 1]) - len(graph.splitlines()[i + 1].lstrip(' ')) == current_space:
            j = i
            while check:
                if len(graph.splitlines()[i - 1]) - len(graph.splitlines()[i - 1].lstrip(' ')) < current_space:
                    # print(inputs.splitlines()[i - 1] + " -> " + inputs.splitlines()[j + 1])
                    app.create_amr_rel(create_amr_help(graph.splitlines()[i - 1]),
                                       create_amr_help(graph.splitlines()[j + 1]), arg_id)
                    check = False
                i = i - 1
            i = j
            check = True
            current_space = len(graph.splitlines()[i + 1]) - len(graph.splitlines()[i + 1].lstrip(' '))
        else:
            current_space = len(graph.splitlines()[i + 1]) - len(graph.splitlines()[i + 1].lstrip(' '))
            j = i
            while check:
                if len(graph.splitlines()[i - 1]) - len(graph.splitlines()[i - 1].lstrip(' ')) < current_space:
                    # print(inputs.splitlines()[i - 1] + " -> " + inputs.splitlines()[j + 1])
                    app.create_amr_rel(create_amr_help(graph.splitlines()[i - 1]),
                                       create_amr_help(graph.splitlines()[j + 1]), arg_id)
                    check = False
                i = i - 1
            i = j
            check = True
            current_space = len(graph.splitlines()[i + 1]) - len(graph.splitlines()[i + 1].lstrip(' '))
        i = i + 1


def create_some_amr(amr_creation_input, path):
    # [AMR, id], premise/conclusion
    graph = amr_creation_input[0]
    arg_id = amr_creation_input[1]
    # [id, source, type, name, identifier, relationship, relationship label, inserts)
    temp_array = numpy.empty(shape=(len(graph.splitlines()) - 1, 8), dtype=object)
    # i = 0 is the raw sentence
    i = 1
    while i < len(graph.splitlines()):
        line = graph.splitlines()[i]
        raw_line = line.lstrip(' ')
        # find amount of inserts
        inserts = len(line) - len(line.lstrip(' '))
        # name of relationship
        rel = raw_line[(raw_line.find(":")):(raw_line.find(' '))]
        rel_label = rel[1:].replace("-", "")
        # name and identifier
        if line.find("(") > -1:
            identifier_line = line[line.find("("):]
            identifier = identifier_line[1:identifier_line.find(" ")]
            name_line = line[line.find("/ "):]
            if name_line.find(")") > -1:
                name = name_line[2:name_line.find(")")]
            else:
                name = name_line[2:]
        else:
            identifier = "None"
            if line.find("\"") > -1:
                name = line[line.find("\""):line.rfind("\"") + 1]
            elif line.find(":quant") > -1:
                if line.find(")") > -1:
                    name = line[line.find(":quant") + 7:line.find(")")]
                else:
                    name = line[line.find(":quant") + 7:]
            elif line.find(":polarity") > -1:
                if line.find(")") > -1:
                    name = line[line.find(":polarity") + 10:line.find(")")]
                else:
                    name = line[line.find(":polarity") + 10:]
            else:
                if line.find(")") > -1:
                    identifier = raw_line[raw_line.find(" ") + 1:raw_line.find(")")]
                else:
                    identifier = raw_line[raw_line.find(" ") + 1:]
                name = "None"
        temp_array[i-1] = [arg_id, file, path, name, identifier, rel, rel_label, inserts]
        i += 1
    i = 1
    while i < len(temp_array):
        found_root = False
        found_ref = False
        j = i - 1
        ref = i - 1
        while not found_root:
            if temp_array[i][7] > temp_array[j][7]:
                if temp_array[i][3] == "None":
                    while not found_ref:
                        if ref == 0:
                            temp_array[i][3] = temp_array[i][4]
                            temp_array[i][4] = "None"
                            app.create_amr(temp_array[j], temp_array[i])
                            found_ref = True
                            found_root = True
                        elif temp_array[ref][4] == temp_array[i][4]:
                            app.create_amr(temp_array[j], temp_array[ref])
                            found_ref = True
                            found_root = True
                        else:
                            ref -= 1
                else:
                    app.create_amr(temp_array[j], temp_array[i])
                    found_root = True
            else:
                j -= 1
        i += 1


def generate_amr(inputs):
    # [premise, id]
    graphs = inputs[2].parse_sents([inputs[0]])
    split = [graphs[0], inputs[1]]
    print(graphs[0])
    create_amr(split, "premise")


def create_basic_database():
    start_time = time.time()
    app.create_node()
    app.create_relationship()
    print("Basic database took", time.time() - start_time, "secs to run")


def generate_some_amr(model, csv_input, iterations):
    start_time = time.time()
    i = 0
    while i < iterations:
        # premise
        graphs = model.parse_sents([csv_input.premise[i]])  # creates AMR graph
        print(graphs[0])  # control output
        amr_creation_input = [graphs[0], i]  # int64 not supported
        create_some_amr(amr_creation_input, "PREMISE")
        # conclusion
        graphs = model.parse_sents([csv_input.conclusion[i]])  # creates AMR graph
        print(graphs[0])  # control output
        amr_creation_input = [graphs[0], i]  # int64 not supported
        create_some_amr(amr_creation_input, "CONCLUSION")
        i += 1
    print("AMR took", time.time() - start_time, "secs to run")


if __name__ == "__main__":

    numpy.set_printoptions(linewidth=320)

    # Connection information

    scheme = "bolt"
    # localhost for local host
    host_name = "localhost"
    # 7687 for local host
    port = "7687"
    url = "{scheme}://{host_name}:{port}".format(scheme=scheme, host_name=host_name, port=port)
    user = "admin"
    password = "admin"

    app = Neo4J(url, user, password)
    # create_basic_database()

    # AMR models: https://amrlib.readthedocs.io/en/latest/install/
    amr_model = amrlib.load_stog_model()
    # load csv with pandas for amr generation
    csv_data = pd.read_csv(pandas_file)

    # for testing
    generate_some_amr(amr_model, csv_data, 1)

    app.close()

    # clean up code, Multisentence, more comments, better naming, look at ""
