import amrlib
import pandas as pd
import time
import numpy as np

import AMR_controller
import Neo4J_interface

# path to csv
pandas_file = "O:/Arbeit/Webis-argument-framing.csv"


def create_basic_database(csv_input):
    start_time = time.time()
    i = 0
    while i < len(csv_input):
        node_id = pandas_file[pandas_file.rfind("/")+1:] + "_" + str(csv_input.argument_id[i])
        node_dict = {"arg_id": int(csv_input.argument_id[i]),
                     "frame": csv_input.frame[i],
                     "topic": csv_input.topic[i],
                     "premise": csv_input.premise[i],
                     "stance": csv_input.stance[i],
                     "conclusion": csv_input.conclusion[i],
                     "source": pandas_file[pandas_file.rfind("/")+1:]}
        app.init_nodes(node_id, node_dict)
        # app.write_amr_start(node_id, node_dict)
        i += 1
    app.init_edges()
    print("Basic database took", time.time() - start_time, "secs to run")


if __name__ == "__main__":
    # Better output
    np.set_printoptions(linewidth=320)

    # Needed to install nltk
    # nltk.download()

    # Connection information

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
        AMR_controller.generate(app, amr_model, csv_data, 0, 12326)
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

    app.close()
