import numpy as np


def count_selector(list_of_columns):
    return ["count(" + val + ")" for val in list_of_columns]


def min_selector(list_of_columns):
    return ["min(" + val + ")" for val in list_of_columns]


def max_selector(list_of_columns):
    return ["max(" + val + ")" for val in list_of_columns]


def average_selector(list_of_columns):
    return ["avg(" + val + ")" for val in list_of_columns]


def sum_selector(list_of_columns):
    return ["sum(" + val + ")" for val in list_of_columns]


def median(list_of_columns, data):
    info = []
    for col in list_of_columns:
        info.append({"median(" + col + ")": np.median([val[col] for val in data])})

    return info


def percentile(list_of_columns, data, q):
    # percentile value should be in the interval [0, 100]
    q = 0 if q < 0 else q
    q = 100 if q > 100 else q

    info = []
    for col in list_of_columns:
        info.append({"percentile_" + str(q) + "(" + col + ")": np.percentile([val[col] for val in data], q)})

    return info
