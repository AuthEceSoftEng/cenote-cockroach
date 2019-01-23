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
        info.append({"system.median(" + col + ")": np.median([val[col] for val in data])})
    
    return info