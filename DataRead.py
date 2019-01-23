import json
import datetime
from CassandraHandler.utils.CassandraHandler import CassandraHandler
from CassandraHandler.utils.helpers import get_time_in_ms, time_to_datetime_in_ms
from CassandraHandler.utils.operations import count_selector, min_selector, \
                             max_selector, average_selector, \
                             sum_selector, median

class ReadData:
    """
    This class implements the data read functionality in Cenote.
    """
    def __init__(self):
        """
        Initializes this data reader.
        """
        self.ch = CassandraHandler()

    def get_column_family(self, url):

        if(url.endswith('/')):
            url = url[:-1]

        info = url.split('/projects/')
        project_id = info[len(info) - 1].split('/queries/')[0]
        event_collection = info[len(info) - 1].split('/queries/')[1].replace('/extraction', '')
        column_family = project_id + '_' + event_collection

        return column_family

    def create_time_conditions(self, info):

        conditions = []
        if(type(info["timeframe_start"]) is int):
            conditions.append({
                "column": "cenote_timestamp",
                "operand": ">=",
                "value": time_in_ms_to_str(info["timeframe_start"])
            })
        if(type(info["timeframe_end"]) is int):
            conditions.append({
                "column": "cenote_timestamp",
                "operand": "<=",
                "value": time_in_ms_to_str(info["timeframe_end"])
            })
        
        return conditions

    def read_data(self, keyspace, list_of_columns, request_info):

        if(isinstance(request_info, str)):
            request_info = json.loads(request_info)
        column_family = self.get_column_family(request_info["cenote"]["url"])
        if(self.ch.check_if_table_exists(keyspace, column_family)):
            curr_state = {}
            conditions = self.create_time_conditions(request_info["cenote"])
            res = self.ch.read_data(keyspace, column_family, list_of_columns, conditions)
            
            return res
        else:
            return { "response": 400, "exception": "The column family: " + column_family + " does not exist" }
    
    def perform_operation(self, keyspace, list_of_columns, type, request_info):

        if(isinstance(request_info, str)):
            request_info = json.loads(request_info)
        column_family = self.get_column_family(request_info["cenote"]["url"])
        
        if(self.ch.check_if_table_exists(keyspace, column_family)):
            if(isinstance(list_of_columns, list) and len(list_of_columns) > 0):
                curr_state = {}
                conditions = self.create_time_conditions(request_info["cenote"])
                # Build-in operations
                if(type == 'count'):
                    list_of_columns = count_selector(list_of_columns)
                if(type == 'min'):
                    list_of_columns = min_selector(list_of_columns)
                if(type == 'max'):
                    list_of_columns = max_selector(list_of_columns)
                if(type == 'average'):
                    list_of_columns = average_selector(list_of_columns)
                if(type == 'sum'):
                    list_of_columns = sum_selector(list_of_columns)
                res = self.ch.read_data(keyspace, column_family, list_of_columns, conditions)
                
                # Custom operations
                if(type == 'median'):
                    res["data"] = median(list_of_columns, res["data"])
                
                return res
            else:
                return { "response": 400, "exception": "At least one column should be selected" }
        else:
            return { "response": 400, "exception": "The column family: " + column_family + " does not exist" }