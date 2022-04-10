import amrlib
import time
import logging
import numpy as np
import traceback
from nltk.tokenize import sent_tokenize
from CSVInput import pandas_file


def add_to_graph(app, amr_creation_input, path, type):
    '''

    :param app:
    :param amr_creation_input: [graph, argument_id, node_id]
    :param path: type+number of sentence
    :param type: type
    :return:
    '''
    # [AMR, id, node_id], premise/conclusion
    graph = amr_creation_input[0]
    arg_id = amr_creation_input[1]
    node_id = amr_creation_input[2]
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
        temp_array[i - 1] = [arg_id, pandas_file, path, name, identifier, rel, rel_label, inserts]
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
                            app.create_amr(temp_array[j], temp_array[i], node_id)
                            found_ref = True
                            found_root = True
                        elif (temp_array[ref][4] == temp_array[i][4]) and (temp_array[ref][3] != "None"):
                            app.create_amr(temp_array[j], temp_array[ref], node_id)
                            found_ref = True
                            found_root = True
                        else:
                            ref -= 1
                else:
                    app.create_amr(temp_array[j], temp_array[i], node_id)
                    found_root = True
            else:
                j -= 1
        i += 1
    app.connect_amr(temp_array[0], type, node_id)


def generate(app, model, csv_input, start, end):
    start_time = time.time()
    i = start
    try:
        while i < end:
            node_id = pandas_file[pandas_file.rfind("/") + 1:] + "_" + str(csv_input.argument_id[i])
            # premise
            j = 0
            while j < len(sent_tokenize((csv_input.premise[i]))):
                sentence_split = sent_tokenize(csv_input.premise[i])
                graphs = model.parse_sents([sentence_split[j]])  # creates AMR graph
                print(graphs[0])  # control output
                amr_creation_input = [graphs[0], int(csv_input.argument_id[i]), node_id]  # int64 not supported
                add_to_graph(app, amr_creation_input, f"premise_{j}", "premise")
                j += 1
            # conclusion
            j = 0
            while j < len(sent_tokenize((csv_input.conclusion[i]))):
                sentence_split = sent_tokenize(csv_input.conclusion[i])
                graphs = model.parse_sents([sentence_split[j]])  # creates AMR graph
                print(graphs[0])  # control output
                amr_creation_input = [graphs[0], int(csv_input.argument_id[i]), node_id]  # int64 not supported
                add_to_graph(app, amr_creation_input, f"conclusion_{j}", "original_conclusion")
                j += 1
            i += 1
    except Exception as e:
        logging.error(traceback.format_exc())
    print("AMR took", time.time() - start_time, "secs to run")
