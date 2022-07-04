import amrlib
import pandas as pd
import time
import numpy as np
import argparse
from pathlib import Path

import AMR_controller
import Neo4J_interface


def create_basic_database(csv_input):
    """
    Extract data from csv, save them in a dictionary and create nodes+edges in Neo4J.
    :param csv_input: csv read from pandas
    :return:
    """
    start_time = time.time()
    i = 0
    while i < len(csv_input):
        sup_id = file_name + "_" + str(csv_input.argument_id[i])
        node_dict = {"frame": csv_input.frame[i],
                     "topic": csv_input.topic[i],
                     "premise": csv_input.premise[i],
                     "stance": csv_input.stance[i],
                     "conclusion": csv_input.conclusion[i],
                     "source": file_name}
        # create nodes
        app.init_nodes(sup_id, node_dict)
        i += 1
    # connect nodes
    app.init_edges()
    print("Basic database took", time.time() - start_time, "secs to run")


if __name__ == "__main__":
    # Better output in console
    np.set_printoptions(linewidth=320)

    # Parser
    parser = argparse.ArgumentParser()
    parser.add_argument("server", help="connect to specific server")
    parser.add_argument("username", help="username")
    parser.add_argument("password", help="password")
    parser.add_argument("command", choices=["base", "amr", "test"])
    parser.add_argument("arguments", nargs='+', help="path to csv first")
    args = parser.parse_args()
    pandas_file = args.arguments[0]
    # Data for database
    csv_data = pd.read_csv(pandas_file)
    p = Path(pandas_file)
    # File name
    file_name = p.stem

    # Server address
    if args.server == "heidelberg":
        url = "neo4j+ssc://v17.cl.uni-heidelberg.de:7687"
    else:
        url = "bolt://localhost:7687"
    # Connect to server
    app = Neo4J_interface.Neo4J(url, args.username, args.password)
    # Create the raw database with given csv
    if args.command == "base":
        print("Creating...")
        create_basic_database(csv_data)
    # Create AMR
    elif args.command == "amr":
        print("Loading model...")
        amr_model = amrlib.load_stog_model()
        # parameter: app, model, data, start, end
        # csv_data.shape[0]
        AMR_controller.generate(app, amr_model, csv_data, 0, csv_data.shape[0], file_name)
    # For testing purposes
    elif args.command == "test":
        print(csv_data.shape[0])
    else:
        print("Some Error")
    app.close()
