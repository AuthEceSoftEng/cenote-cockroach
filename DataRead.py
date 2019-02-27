import json

from utils.CockroachHandler import CockroachHandler
from utils.operations import (average_selector, count_selector, max_selector,
                              median, min_selector, percentile, sum_selector)


class ReadData:
    """
    This class implements the data read functionality in Cenote.
    """

    def __init__(self):
        """
        Initializes this data reader.
        """
        self.ch = CockroachHandler()

    @staticmethod
    def get_table(url):

        if url.endswith('/'):
            url = url[:-1]

        info = url.split('/projects/')
        project_id = info[len(info) - 1].split('/queries/')[0]
        event_collection = info[len(info) - 1].split('/queries/')[1].replace('/extraction', '')
        table = project_id + '_' + event_collection

        return table

    @staticmethod
    def create_time_conditions(info):

        conditions = []
        if type(info["timeframe_start"]) is int:
            conditions.append({
                "column": "cenote_timestamp",
                "operand": ">=",
                "value": info["timeframe_start"]
            })
        if type(info["timeframe_end"]) is int:
            conditions.append({
                "column": "cenote_timestamp",
                "operand": "<=",
                "value": info["timeframe_end"]
            })

        return conditions

    def read_data(self, list_of_columns, request_info):

        if isinstance(request_info, str):
            request_info = json.loads(request_info)
        table = self.get_table(request_info["cenote"]["url"])
        if self.ch.check_if_table_exists(table):
            conditions = self.create_time_conditions(request_info["cenote"])
            res = self.ch.read_data(table, list_of_columns, conditions)

            return res
        else:
            return {"response": 400, "exception": "The table: " + table + " does not exist"}

    def perform_operation(self, list_of_columns, type, request_info):

        if isinstance(request_info, str):
            request_info = json.loads(request_info)
        table = self.get_table(request_info["cenote"]["url"])

        if self.ch.check_if_table_exists(table):
            if isinstance(list_of_columns, list) and len(list_of_columns) > 0:
                conditions = self.create_time_conditions(request_info["cenote"])
                # Build-in operations
                if type == 'count':
                    list_of_columns = count_selector(list_of_columns)
                if type == 'min':
                    list_of_columns = min_selector(list_of_columns)
                if type == 'max':
                    list_of_columns = max_selector(list_of_columns)
                if type == 'average':
                    list_of_columns = average_selector(list_of_columns)
                if type == 'sum':
                    list_of_columns = sum_selector(list_of_columns)
                res = self.ch.read_data(table, list_of_columns, conditions)

                # Custom operations
                if type == 'median':
                    res["data"] = median(list_of_columns, res["data"])
                if type == 'percentile':
                    if "percentile" in request_info["cenote"]:
                        res["data"] = percentile(list_of_columns, res["data"], request_info["cenote"]["percentile"])
                    else:
                        return {"response": 400,
                                "exception": "When operation is percentile, you must specify the percentile (e.g. 10)"}
                return res
            else:
                return {"response": 400, "exception": "At least one column should be selected"}
        else:
            return {"response": 400, "exception": "The table: " + table + " does not exist"}
