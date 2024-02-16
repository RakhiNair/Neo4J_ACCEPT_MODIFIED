import time
import logging
import numpy as np
import traceback
from nltk.tokenize import sent_tokenize


def add_to_graph(app, amr_creation_input, path, type, pandas_file):
    """

    :param pandas_file: needed for id
    :param app:
    :param amr_creation_input: [graph, argument_id, sup_id]
    :param path: type+number of sentence
    :param type: type
    :return:
    """
    # [AMR, id, node_id], premise/conclusion
    graph = amr_creation_input[0]
    arg_id = amr_creation_input[1]
    sup_id = amr_creation_input[2]
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
        while not found_root and np.abs(j) < len(temp_array):
            if temp_array[i][7] > temp_array[j][7]:
                if temp_array[i][3] == "None":
                    while not found_ref:
                        if ref == 0:
                            temp_array[i][3] = temp_array[i][4]
                            temp_array[i][4] = "None"
                            app.create_amr(temp_array[j], temp_array[i], sup_id)
                            found_ref = True
                            found_root = True
                        elif (temp_array[ref][4] == temp_array[i][4]) and (temp_array[ref][3] != "None"):
                            app.create_amr(temp_array[j], temp_array[ref], sup_id)
                            found_ref = True
                            found_root = True
                        else:
                            ref -= 1
                else:
                    app.create_amr(temp_array[j], temp_array[i], sup_id)
                    found_root = True
            else:
                j -= 1
        i += 1
    app.connect_amr(temp_array[0], type, sup_id)


def generate(app, model, csv_input, start, end, file_name):
    """
    Split up the sentences with nltk and generate the AMR.
    Add them to the graph
    :param file_name: file name needed for id
    :param app:
    :param model: model used for AMR
    :param csv_input: raw csv data
    :param start: starting line in csv
    :param end: ending line in csv
    :return:
    """
    start_time = time.time()
    i = start
    try:
        while i < end:
            sup_id = file_name + "_" + str(csv_input.argument_id[i])
            if app.amr_not_exists(sup_id):
                # premise
                j = 0
                sentence_split = sent_tokenize(csv_input.premise[i])
                while j < len(sentence_split):
                    try:
                        graphs = model.parse_sents([sentence_split[j]])  # creates AMR graph
                        print(graphs[0])  # control output
                        amr_creation_input = [graphs[0], str(csv_input.argument_id[i]), sup_id]  # int64 not supported
                        add_to_graph(app, amr_creation_input, f"premise_{j}", "premise", file_name)
                    except Exception as e:
                        logging.error(f"Error parsing sentence: {e}")
                        time.sleep(60)
                        continue
                    j += 1
                # conclusion
                j = 0
                sentence_split = sent_tokenize(csv_input.conclusion[i])
                while j < len(sentence_split):
                    try:
                        graphs = model.parse_sents([sentence_split[j]])  # creates AMR graph
                        print(graphs[0])  # control output
                        amr_creation_input = [graphs[0], str(csv_input.argument_id[i]), sup_id]  # int64 not supported
                        add_to_graph(app, amr_creation_input, f"original_conclusion_{j}", "original_conclusion", file_name)
                    except Exception as e:
                        logging.error(f"Error parsing sentence: {e}")
                        time.sleep(60)
                        continue
                    j += 1
            i += 1
    except Exception as e:
        logging.error(traceback.format_exc())
        time.sleep(60)
    print("AMR took", time.time() - start_time, "secs to run")
