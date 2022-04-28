import amrlib
import pandas as pd
import time
import numpy as np
import argparse

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
        sup_id = pandas_file[pandas_file.rfind("/") + 1:pandas_file.rfind(".")] + "_" + str(csv_input.argument_id[i])
        node_dict = {"frame": csv_input.frame[i],
                     "topic": csv_input.topic[i],
                     "premise": csv_input.premise[i],
                     "stance": csv_input.stance[i],
                     "conclusion": csv_input.conclusion[i],
                     "source": pandas_file[pandas_file.rfind("/") + 1:pandas_file.rfind(".")]}
        # create nodes
        app.init_nodes(sup_id, node_dict)
        i += 1
    # connect nodes
    app.init_edges()
    print("Basic database took", time.time() - start_time, "secs to run")


if __name__ == "__main__":
    # Better output in console
    np.set_printoptions(linewidth=320)

    # Needed to install nltk
    # nltk.download()

    # Connection information

    parser = argparse.ArgumentParser()

    parser.add_argument("server", help="connect to specific server")
    parser.add_argument("username", help="username")
    parser.add_argument("password", help="password")
    parser.add_argument("path_to_csv", help="path to csv")
    args = parser.parse_args()
    pandas_file = args.path_to_csv
    csv_data = pd.read_csv(pandas_file)

    if args.server == "heidelberg":
        url = "neo4j+ssc://v17.cl.uni-heidelberg.de:7687"
        app = Neo4J_interface.Neo4J(url, args.username, args.password)
        print("Loading model...")
        amr_model = amrlib.load_stog_model()
        # for testing (app, model, data, start, end), 12326 lines
        AMR_controller.generate(app, amr_model, csv_data, 0, 12326, pandas_file)
        app.close()
    elif args.server == "local":
        print("Test stuff:")
        url = "bolt://localhost:7687"
        app = Neo4J_interface.Neo4J(url, args.username, args.password)
        print("Loading model...")
        amr_model = amrlib.load_stog_model()
        # for testing (app, model, data, start, end), 12326 lines
        AMR_controller.generate(app, amr_model, csv_data, 0, 20, pandas_file)
        app.close()
    else:
        print("Some Error")

    """
    print("\nWhere do you want to connect?\nh (Heidelberg)\nl (local)")
    url_input = input()
    if url_input == "h":
        url = "neo4j+ssc://v17.cl.uni-heidelberg.de:7687"
    else:
        url = "bolt://localhost:7687"

    user = input("Username: ")
    password = input("Password: ")

    # Connecting
    app = Neo4J_interface.Neo4J(url, user, password)

    print("\nWhat do you want to do?\nc (create a basic database)\na (generate amr)\ns (search for keyword in "
          "conclusion)\nadd (add alternative conclusion)")
    task_input = input()
    # Load csv with pandas
    csv_data = pd.read_csv(pandas_file)
    if task_input == "c":
        print("Creating...")
        create_basic_database(csv_data)
    elif task_input == "a":
        # AMR models: https://amrlib.readthedocs.io/en/latest/install/
        print("Loading model...")
        amr_model = amrlib.load_stog_model()
        # for testing (app, model, data, start, end), 12326 lines
        AMR_controller.generate(app, amr_model, csv_data, 0, 10)
    elif task_input == "s":
        search_input = input("\nWhat do you want to search for?\n")
        app.search_keyword(search_input)
        print("Results written to file")
    elif task_input == "add":
        print("WIP")
    elif task_input == "t":
        print(app.amr_exists(5))
    else:
        print("Unknown task")
    """
