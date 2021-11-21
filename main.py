from neo4j import GraphDatabase
import amrlib
import pandas as pd


class Neo4J:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def import_csv(self):
        with self.driver.session() as session:
            session.write_transaction(self._import_csv)

    def create_relationship(self):
        with self.driver.session() as session:
            session.write_transaction(self._create_relationship)

    def create_amr_node(self, text):
        with self.driver.session() as session:
            session.write_transaction(self._create_amr_node, text)

    def create_amr_rel(self, root, node, info):
        with self.driver.session() as session:
            session.write_transaction(self._create_amr_rel, root, node, info)

    def connect_amr(self, text, info):
        with self.driver.session() as session:
            session.write_transaction(self._connect_amr, text, info)

    @staticmethod
    def _import_csv(tx):
        tx.run("LOAD CSV WITH HEADERS FROM 'file:///Webis-argument-framing.csv' AS row "
               "WITH row WHERE row.argument_id IS NOT NULL "
               "MERGE (:argument {argument_id: row.argument_id, frame_id: row.frame_id, "
               "frame: row.frame, topic_id: row.topic_id, topic: row.topic, premise: row.premise, stance: row.stance, "
               "conclusion: row.conclusion}) "
               "MERGE (:frame {frame_id: row.frame_id, name: row.frame}) "
               "MERGE (:topic {topic_id: row.topic_id, name: row.topic}) "
               "MERGE (:stance {stance: row.stance})")

    @staticmethod
    def _create_relationship(tx):
        tx.run("MATCH (a:argument), (b:frame) "
               "WHERE a.frame_id=b.frame_id "
               "MERGE (a)-[c:CLASSIFICATION {name: a.topic + '<->' + b.name}]->(b)")
        tx.run("MATCH (a:argument), (b:topic) "
               "WHERE a.topic_id=b.topic_id "
               "MERGE (a)-[c:TOPIC {name: a.topic}]->(b)")
        tx.run("MATCH (a:argument), (b:stance) "
               "WHERE a.stance=b.stance "
               "MERGE (a)-[c:STANCE {name: a.stance}]->(b)")

    @staticmethod
    def _create_amr_node(tx, text):
        tx.run("MERGE (:AMR {name: $name, type: $type})", name=text[1], type=text[0])

    @staticmethod
    def _create_amr_rel(tx, root, node, info):
        tx.run("MERGE (a:AMR {name: $name1, type: $type1, relationship: $rel1, argument_id: $id}) "
               "MERGE (b:AMR {name: $name2, type: $type2, relationship: $rel2, argument_id: $id}) "
               "MERGE (a)-[RELATIONSHIP {name: b.relationship}]->(b)", name1=root[1], type1=root[0], name2=node[1],
               type2=node[0], rel1=root[2], rel2=node[2], id=info)

    @staticmethod
    def _connect_amr(tx, text, info):
        tx.run("MERGE (a:AMR {name: $name1, type: $type1, relationship: $rel1, argument_id: $id}) ", name1=text[1],
               type1=text[0], rel1=text[2], id=info)
        tx.run("MATCH (a:argument), (b:AMR) "
               "WHERE a.argument_id=b.argument_id AND b.name=$name1 AND b.type=$type1 AND b.relationship=$rel1 AND "
               "b.argument_id=$id "
               "MERGE (a)-[c:AMR]->(b)", name1=text[1], type1=text[0], rel1=text[2], id=info)


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


def create_amr(temp_inputs):
    # [AMR, id]
    inputs = temp_inputs[0]
    other_info = temp_inputs[1]
    check = True
    i = 1
    obj.connect_amr(create_amr_help(inputs.splitlines()[i]), other_info)
    current_space = 0
    while i + 1 < len(inputs.splitlines()):
        if len(inputs.splitlines()[i + 1]) - len(inputs.splitlines()[i + 1].lstrip(' ')) > current_space:
            # print(inputs.splitlines()[i] + " -> " + inputs.splitlines()[i + 1])
            obj.create_amr_rel(create_amr_help(inputs.splitlines()[i]), create_amr_help(inputs.splitlines()[i + 1]),
                               other_info)
            current_space = len(inputs.splitlines()[i + 1]) - len(inputs.splitlines()[i + 1].lstrip(' '))
        elif len(inputs.splitlines()[i + 1]) - len(inputs.splitlines()[i + 1].lstrip(' ')) == current_space:
            j = i
            while check:
                if len(inputs.splitlines()[i - 1]) - len(inputs.splitlines()[i - 1].lstrip(' ')) < current_space:
                    # print(inputs.splitlines()[i - 1] + " -> " + inputs.splitlines()[j + 1])
                    obj.create_amr_rel(create_amr_help(inputs.splitlines()[i - 1]),
                                       create_amr_help(inputs.splitlines()[j + 1]), other_info)
                    check = False
                i = i - 1
            i = j
            check = True
            current_space = len(inputs.splitlines()[i + 1]) - len(inputs.splitlines()[i + 1].lstrip(' '))
        else:
            current_space = len(inputs.splitlines()[i + 1]) - len(inputs.splitlines()[i + 1].lstrip(' '))
            j = i
            while check:
                if len(inputs.splitlines()[i - 1]) - len(inputs.splitlines()[i - 1].lstrip(' ')) < current_space:
                    # print(inputs.splitlines()[i - 1] + " -> " + inputs.splitlines()[j + 1])
                    obj.create_amr_rel(create_amr_help(inputs.splitlines()[i - 1]),
                                       create_amr_help(inputs.splitlines()[j + 1]), other_info)
                    check = False
                i = i - 1
            i = j
            check = True
            current_space = len(inputs.splitlines()[i + 1]) - len(inputs.splitlines()[i + 1].lstrip(' '))
        i = i + 1


def generate_amr(inputs):
    # [premise, id]
    graphs = inputs[2].parse_sents([inputs[0]])
    split = [graphs[0], inputs[1]]
    print(graphs[0])
    create_amr(split)


if __name__ == "__main__":
    obj = Neo4J("bolt://localhost:7687", "admin", "admin")
    # obj.import_csv()
    # obj.create_relationship()
    # obj.close()

    # stuff to install
    # Pytorch from https://pytorch.org/
    # amrlib with pip3 install amrlib
    # a model from https://amrlib.readthedocs.io/en/latest/install/

    csv_data = pd.read_csv("O:/Arbeit/Webis-argument-framing.csv")

    stog = amrlib.load_stog_model()
    # int64 not supported
    i = 0
    while i < 6:
        test = [csv_data.premise[i], str(i), stog]
        generate_amr(test)
        i += 1

    obj.close()


