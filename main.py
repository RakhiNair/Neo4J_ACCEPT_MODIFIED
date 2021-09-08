from neo4j import GraphDatabase


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


if __name__ == "__main__":
    obj = Neo4J("bolt://localhost:7687", "admin", "admin")
    # obj.import_csv()
    obj.create_relationship()
    obj.close()
