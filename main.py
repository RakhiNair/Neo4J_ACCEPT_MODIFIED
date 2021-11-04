from neo4j import GraphDatabase
import amrlib


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

    def create_amr_rel(self, root, node):
        with self.driver.session() as session:
            session.write_transaction(self._create_amr_rel, root, node)

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
        tx.run("MERGE (:AMR {stuff: $text})", text=text)

    @staticmethod
    def _create_amr_rel(tx, root, node):
        tx.run("MATCH (a:AMR), (b:AMR) "
               "WHERE a.stuff=$rel1 AND b.stuff=$rel2 "
               "MERGE (a)-[c:RELATION]->(b)", rel1=root, rel2=node)


def create_amr(inputs):
    check = True
    i = 1
    current_space = 0
    obj.create_amr_node(inputs.splitlines()[i])
    while i + 1 < len(inputs.splitlines()):
        obj.create_amr_node(inputs.splitlines()[i + 1])
        if len(inputs.splitlines()[i + 1]) - len(inputs.splitlines()[i + 1].lstrip(' ')) > current_space:
            # print(inputs.splitlines()[i] + " -> " + inputs.splitlines()[i + 1])
            obj.create_amr_rel(inputs.splitlines()[i], inputs.splitlines()[i + 1])
            current_space = len(inputs.splitlines()[i + 1]) - len(inputs.splitlines()[i + 1].lstrip(' '))
        elif len(inputs.splitlines()[i + 1]) - len(inputs.splitlines()[i + 1].lstrip(' ')) == current_space:
            j = i
            while check:
                if len(inputs.splitlines()[i - 1]) - len(inputs.splitlines()[i - 1].lstrip(' ')) < current_space:
                    # print(inputs.splitlines()[i - 1] + " -> " + inputs.splitlines()[j + 1])
                    obj.create_amr_rel(inputs.splitlines()[i - 1], inputs.splitlines()[j + 1])
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
                    obj.create_amr_rel(inputs.splitlines()[i - 1], inputs.splitlines()[j + 1])
                    check = False
                i = i - 1
            i = j
            check = True
            current_space = len(inputs.splitlines()[i + 1]) - len(inputs.splitlines()[i + 1].lstrip(' '))
        i = i + 1


if __name__ == "__main__":
    obj = Neo4J("bolt://localhost:7687", "admin", "admin")
    # obj.import_csv()
    # obj.create_relationship()
    # obj.close()

    # stuff to install
    # Pytorch from https://pytorch.org/
    # armlib with pip3 install amrlib
    # a model from https://amrlib.readthedocs.io/en/latest/install/

    stog = amrlib.load_stog_model()
    graphs = stog.parse_sents(['"Obama\'s plan was criticized by some Democrats for including a heavy component of '
                               'tax cuts'])
    split = graphs[0]
    print(split)
    create_amr(split)

    obj.close()
