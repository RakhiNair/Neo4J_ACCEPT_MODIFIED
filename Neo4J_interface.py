from neo4j import GraphDatabase


class Neo4J:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def amr_exists(self, node_id):
        with self.driver.session() as session:
            result = session.write_transaction(self._amr_exists, node_id)
            return result[0]["Exists"]

    def connect_amr(self, node, type, node_id):
        with self.driver.session() as session:
            session.write_transaction(self._connect_amr, node, type, node_id)

    def create_amr(self, root, node, node_id):
        with self.driver.session() as session:
            session.write_transaction(self._create_amr, root, node, node_id)

    def search_keyword(self, keyword):
        with self.driver.session() as session:
            session.write_transaction(self._search_keyword, keyword)

    def write_amr_start(self, node_id, node_data):
        with self.driver.session() as session:
            session.write_transaction(self._write_amr_start, node_id, node_data)

    def write_edge(self):
        with self.driver.session() as session:
            session.write_transaction(self._write_edge)

    def init_edges(self):
        with self.driver.session() as session:
            session.write_transaction(self._init_edges)

    def write_node(self, node_id, node_data):
        with self.driver.session() as session:
            session.write_transaction(self._write_node, node_id, node_data)

    def init_nodes(self, node_id, node_data):
        with self.driver.session() as session:
            session.write_transaction(self._init_nodes, node_id, node_data)

    # WIP
    @staticmethod
    def _amr_exists(tx, node_id):
        result = tx.run("OPTIONAL MATCH (a:amr) "
                        "WHERE a.sup_id = $node_id "
                        f"RETURN a IS NOT NULL AS Exists", node_id=node_id)
        return result.data()

    @staticmethod
    def _connect_amr(tx, node, type, sup_id):
        tx.run(f"MATCH (a:argument_unit), (b:amr) "
               "WHERE a.type=$type1 AND a.sup_id=b.sup_id=$id AND b.relationship=$rel AND b.type=$path "
               f"MERGE (a)-[:{type}]->(b)", id=sup_id, source=node[1], type1=type, name1=node[3],
               ident=node[4], rel=node[5], name2="Argument structure", type2=node[2], type=type, path=node[2])

    @staticmethod
    def _create_amr(tx, root, leaf, sup_id):
        """
        :param tx:
        :param root: [id, source, type, name, identifier, relationship, relationship label, inserts]
        :param leaf: [id, source, type, name, identifier, relationship, relationship label, inserts]
        :param sup_id:
        :return:
        """
        tx.run("MERGE (a:amr {id: $amr_id1, sup_id: $sup_id, type: $type, "
               "name: $name1, identifier: $ident1, relationship: $rel1}) "
               "MERGE (b:amr {id: $amr_id2, sup_id: $sup_id, type: $type, "
               "name: $name2, identifier: $ident2, relationship: $rel2}) "
               f"MERGE (a)-[:{leaf[6]}]->(b)",
               amr_id1=sup_id + "_" + root[2] + "_" + root[4], amr_id2=sup_id + "_" + leaf[2] + "_" + leaf[4],
               sup_id=sup_id,
               id=root[0], source=root[1], type=root[2], name1=root[3], name2=leaf[3],
               ident1=root[4], ident2=leaf[4], rel1=root[5], rel2=leaf[5])

    @staticmethod
    def _init_edges(tx):
        tx.run("MATCH (a:argument), (b:topic) "
               "WHERE a.topic = b.name "
               "MERGE (b)-[:SUBSTRUCTURE]->(a)")
        tx.run("MATCH (a:argument), (b:argument_structure) "
               "WHERE a.sup_id = b.sup_id "
               "MERGE (a)-[:SUBSTRUCTURE]->(b)")
        tx.run("MATCH (a:argument_structure), (b:argument_unit) "
               "WHERE a.sup_id = b.sup_id "
               "MERGE (a)-[:SUBSTRUCTURE]->(b)")

    @staticmethod
    def _init_nodes(tx, node_id, node_data):
        tx.run("MERGE (:argument {id: $node_id_argument, sup_id: $sup_id, frame: $frame, "
               "topic: $topic, stance: $stance, source: $source}) "
               "MERGE (:topic {name: $topic}) "
               "MERGE (:argument_structure {id: $node_id_argument_structure, sup_id: $sup_id}) "
               "MERGE (:argument_unit {id: $node_id_premise, sup_id: $sup_id, type: $type_premise, rawText: $premise}) "
               "MERGE (:argument_unit {id: $node_id_original_conclusion, sup_id: $sup_id, type: $type_conclusion, "
               "rawText: $conclusion})",
               node_id_argument=node_id + "_argument", node_id_argument_structure=node_id + "_argument_structure",
               node_id_premise=node_id + "_premise", node_id_original_conclusion=node_id + "_original_conclusion",
               sup_id=node_id, frame=node_data["frame"], topic=node_data["topic"],
               stance=node_data["stance"], source=node_data["source"],
               premise=node_data["premise"], conclusion=node_data["conclusion"],
               type_premise="premise", type_conclusion="original_conclusion")

    @staticmethod
    def _search_keyword(tx, keyword):
        result = tx.run("MATCH (a:argument_unit) "
                        "WHERE toLower(a.rawText) CONTAINS toLower($keyword) AND a.type=$type "
                        f"RETURN a.argument_id, a.rawText ", keyword=keyword, type="original_conclusion")
        f = open("search_result.txt", "w")
        f.write("Search result for " + keyword + "\n")
        for line in result:
            f.write(str(line) + "\n")
        f.close()

    # TEMP
    @staticmethod
    def _write_amr_start(tx, node_id, node_data):
        tx.run("MERGE (a:amr {id: $node_id, argument_id: $arg_id, source: $source, name: $name, type: $type})",
               node_id=node_id, arg_id=node_data["arg_id"], source=node_data["source"], name="Argument structure", type="premise")
        tx.run("MERGE (a:amr {id: $node_id, argument_id: $arg_id, source: $source, name: $name, type: $type, "
               "conclusion_number: 0})",
               node_id=node_id, arg_id=node_data["arg_id"], source=node_data["source"], name="Argument structure", type="conclusion")
        tx.run("MATCH (a:conclusion), (b:amr) "
               "WHERE a.id=b.id AND b.name=$name AND b.type=$type "
               "MERGE (a)-[c:ISREALTEDTO]->(b)", name="Argument structure", type="conclusion")
        tx.run("MATCH (a:premise), (b:amr) "
               "WHERE a.id=b.id AND b.name=$name AND b.type=$type "
               "MERGE (a)-[c:ISREALTEDTO]->(b)", name="Argument structure", type="premise")

    @staticmethod
    def _write_edge(tx):
        tx.run()

    @staticmethod
    def _write_node(tx, node_id, node_data):
        tx.run("MERGE (:$node_type {id: $node_id}")
