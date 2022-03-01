from neo4j import GraphDatabase
import amrlib
import pandas as pd
import time
import numpy as np
import traceback
import logging

from nltk.tokenize import sent_tokenize

# NEO4J Import
# local file in import folder, otherwise https://neo4j.com/developer/kb/import-csv-locations/
file = 'file:///Webis-argument-framing.csv'
# file = "O:/Arbeit/Webis-argument-framing.csv"

# Pandas Import for AMR generation
pandas_file = "O:/Arbeit/Webis-argument-framing.csv"


class Neo4J:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def create_node(self, arg_id, f_id, frame, t_id, topic, premise, stance, conclusion, source):
        with self.driver.session() as session:
            session.write_transaction(self._create_node, arg_id, f_id, frame, t_id, topic, premise, stance, conclusion, source)

    def create_relationship(self):
        with self.driver.session() as session:
            session.write_transaction(self._create_relationship)

    def create_amr(self, root, node):
        with self.driver.session() as session:
            session.write_transaction(self._create_amr, root, node)

    def connect_amr(self, node, type):
        with self.driver.session() as session:
            session.write_transaction(self._connect_amr, node, type)

    @staticmethod
    def _create_node(tx, arg_id, f_id, frame, t_id, topic, premise, stance, conclusion, source):
        tx.run("MERGE (:argument {argument_id: $arg_id, frame_id: $f_id, frame: $frame, topic_id: $t_id, "
               "topic: $topic, premise: $premise, stance: $stance, conclusion: $conclusion, source: $source}) "
               "MERGE (:frame {frame_id: $f_id, name: $frame}) "
               "MERGE (:topic {topic_id: $t_id, name: $topic}) "
               "MERGE (:stance {stance: $stance}) "
               "MERGE (:amr {argument_id: $arg_id, source: $source, name: $amr})",
               arg_id=arg_id, f_id=f_id, frame=frame, t_id=t_id, topic=topic, premise=premise, stance=stance,
               conclusion=conclusion, source=source, amr="Argument structure")

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
               "WHERE a.argument_id=b.argument_id AND a.source=b.source AND b.name=$name "
               "MERGE (a)-[c:ISREALTEDTO]->(b)", name="Argument structure")

    @staticmethod
    def _create_amr(tx, root, node):
        tx.run("MERGE (a:amr {argument_id: $id, source: $source, type: $type, name: $name1, "
               "identifier: $ident1, relationship: $rel1}) "
               "MERGE (b:amr {argument_id: $id, source: $source, type: $type, name: $name2, "
               "identifier: $ident2, relationship: $rel2}) "
               f"MERGE (a)-[:{node[6]}]->(b)", id=root[0], source=root[1], type=root[2], name1=root[3], name2=node[3],
               ident1=root[4], ident2=node[4], rel1=root[5], rel2=node[5])

    @staticmethod
    def _connect_amr(tx, node, type):
        tx.run("MATCH (a:amr), (b:amr) "
               "WHERE a.argument_id=b.argument_id=$id AND a.source=b.source=$source AND a.name=$name2 AND "
               "b.name=$name1 AND b.type=$type AND b.identifier=$ident AND b.relationship=$rel "
               f"MERGE (a)-[:{type}]->(b)", id=node[0], source=node[1], type=node[2], name1=node[3],
               ident=node[4], rel=node[5], name2="Argument structure")


def create_some_amr(amr_creation_input, path, type):
    # [AMR, id], premise/conclusion
    graph = amr_creation_input[0]
    arg_id = amr_creation_input[1]
    # [id, source, type, name, identifier, relationship, relationship label, inserts)
    temp_array = np.empty(shape=(len(graph.splitlines()) - 1, 8), dtype=object)
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
            else:
                if line.find(")") > -1:
                    identifier = raw_line[raw_line.find(" ") + 1:raw_line.find(")")]
                else:
                    identifier = raw_line[raw_line.find(" ") + 1:]
                name = "None"
        temp_array[i - 1] = [arg_id, file, path, name, identifier, rel, rel_label, inserts]
        i += 1
    # connect references
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
                        elif (temp_array[ref][4] == temp_array[i][4]) and (temp_array[ref][3] != "None"):
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
    app.connect_amr(temp_array[0], type)


def create_basic_database(csv_input):
    start_time = time.time()
    i = 0
    while i < len(csv_input):
        arg_id = int(csv_input.argument_id[i])
        f_id = int(csv_input.frame_id[i])
        frame = csv_input.frame[i]
        t_id = int(csv_input.topic_id[i])
        topic = csv_input.topic[i]
        premise = csv_input.premise[i]
        stance = csv_input.stance[i]
        conclusion = csv_input.conclusion[i]
        source = file
        app.create_node(arg_id, f_id, frame, t_id, topic, premise, stance, conclusion, source)
        i += 1
    app.create_relationship()
    print("Basic database took", time.time() - start_time, "secs to run")


def generate_some_amr(model, csv_input, start, end):
    start_time = time.time()
    i = start
    try:
        while i < end:
            # premise
            j = 0
            while j < len(sent_tokenize((csv_input.premise[i]))):
                sentence_split = sent_tokenize(csv_input.premise[i])
                graphs = model.parse_sents([sentence_split[j]])  # creates AMR graph
                print(graphs[0])  # control output
                amr_creation_input = [graphs[0], i]  # int64 not supported
                create_some_amr(amr_creation_input, f"PREMISE{j}", "PREMISE")
                j += 1
            # conclusion
            j = 0
            while j < len(sent_tokenize((csv_input.conclusion[i]))):
                sentence_split = sent_tokenize(csv_input.conclusion[i])
                graphs = model.parse_sents([sentence_split[j]])  # creates AMR graph
                print(graphs[0])  # control output
                amr_creation_input = [graphs[0], i]  # int64 not supported
                create_some_amr(amr_creation_input, f"CONCLUSION{j}", "CONCLUSION")
                j += 1
            i += 1
    except Exception as e:
        logging.error(traceback.format_exc())
    print("AMR took", time.time() - start_time, "secs to run")


if __name__ == "__main__":
    # Better output
    np.set_printoptions(linewidth=320)

    # Needed to install nltk
    # nltk.download()

    # Connection information

    # scheme: "neo4j+ssc" for Heidelberg, "bolt" for local
    scheme = "bolt"
    # server address: "v17.cl.uni-heidelberg.de" for Heidelberg, "localhost" for local
    host_name = "localhost"
    # port: "7687" for both
    port = "7687"
    # url = "{scheme}://{host_name}:{port}".format(scheme=scheme, host_name=host_name, port=port)

    # use this for now
    url = "neo4j+ssc://v17.cl.uni-heidelberg.de:7687"
    # url = "bolt://localhost:7687"

    user = input("Username: ")
    password = input("Password: ")

    # Connecting
    app = Neo4J(url, user, password)

    # Load csv with pandas
    csv_data = pd.read_csv(pandas_file)
    create_basic_database(csv_data)
    # print(csv_data.shape)
    # AMR models: https://amrlib.readthedocs.io/en/latest/install/
    amr_model = amrlib.load_stog_model()

    # for testing (model, data, start, end), 12326 lines
    generate_some_amr(amr_model, csv_data, 0, 12326)

    app.close()
