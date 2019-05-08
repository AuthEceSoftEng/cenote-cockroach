import json
from copy import deepcopy
from datetime import datetime

from CockroachHandler import CockroachHandler


class DataWrite:
    """
    This class implements the data write functionality in Cenote.
    """

    def __init__(self):
        """
        Initializes this data writer.
        """
        self.ch = CockroachHandler()
        self.excluded_columns = ["uuid"]
        self.nested_properties_sep = '$'

    def create_table(self, table_name, column_specs):

        # Every table should have a uuid column which will be used as primary key
        column_specs.append({"name": "uuid", "type": "UUID", "primary_key": "yes"})

        # Every table should have a cenote created_at column, a timestamp end column and an id column
        column_specs.append({"name": "cenote" + self.nested_properties_sep + "created_at", "type": "timestamp"})
        column_specs.append({"name": "cenote" + self.nested_properties_sep + "timestamp", "type": "timestamp"})
        column_specs.append({"name": "cenote" + self.nested_properties_sep + "id", "type": "uuid"})

        return self.ch.create_table(table_name, column_specs)

    @staticmethod
    def get_table(url):

        if url.endswith('/'):
            url = url[:-1]

        info = url.split('/projects/')
        project_id = info[len(info) - 1].split('/events/')[0]
        event_collection = info[len(info) - 1].split('/events/')[1]
        table = project_id + '_' + event_collection

        return table

    def create_column_specs(self, obj, col_specs=None, prev_key=''):
        if col_specs is None:
            col_specs = []
        for key in obj:
            if type(obj[key]) is dict:
                if prev_key != '':
                    self.create_column_specs(obj[key], col_specs,
                                             prev_key + self.nested_properties_sep + key.replace(' ', '').lower())
                else:
                    self.create_column_specs(obj[key], col_specs, key.replace(' ', '').lower())
            else:
                info = {}
                if prev_key != '':
                    info["name"] = prev_key + self.nested_properties_sep + key.replace(' ', '').lower()
                else:
                    info["name"] = key.replace(' ', '').lower()
                if type(obj[key]) is str:
                    info["type"] = "string"
                elif type(obj[key]) is bool:
                    info["type"] = "bool"
                else:
                    info["type"] = "decimal"
                col_specs.append(info)

        return col_specs

    def create_data_write_obj(self, obj, data=None, prev_key=''):
        if data is None:
            data = []
        for key in obj:
            if type(obj[key]) is dict:
                if prev_key != '':
                    self.create_data_write_obj(obj[key], data,
                                               prev_key + self.nested_properties_sep + key.replace(' ', '').lower())
                else:
                    self.create_data_write_obj(obj[key], data, key.replace(' ', '').lower())
            else:
                info = {}
                if prev_key != '':
                    info["column"] = prev_key + self.nested_properties_sep + key.replace(' ', '').lower()
                else:
                    info["column"] = key.replace(' ', '').lower()
                info["value"] = obj[key]

                data.append(info)

        return data

    def append_cenote_info(self, obj, data=None):
        if data is None:
            data = []

        if "timestamp" in obj["cenote"].keys():
            timestamp = obj["cenote"]["timestamp"]
        else:
            timestamp = datetime.utcnow().isoformat()

        data.append({"column": "cenote" + self.nested_properties_sep + "created_at",
                     "value": datetime.fromtimestamp(obj["cenote"]["created_at"] / 1e3).isoformat()})
        data.append({"column": "cenote" + self.nested_properties_sep + "timestamp", "value": timestamp})
        data.append({"column": "cenote" + self.nested_properties_sep + "id", "value": obj["cenote"]["id"]})
        data.append({"column": "uuid", "built_in_function": "gen_random_uuid()"})

        return data

    def write_data(self, data_instance_array):

        if type(data_instance_array) is str:
            data_instance_array = json.loads(data_instance_array)

        # Check/create table based on first event
        first_event = data_instance_array[0]
        table = self.get_table(first_event["cenote"]["url"])
        col_specs = self.create_column_specs(first_event["data"], [])
        res = self.create_table(table, col_specs)
        if res["response"] != 201:
            return {"response": 400, "exception": "Can't create table"}

        # Create missing columns in current schema
        if self.ch.describe_table(table) is None:
            current_schema_cols = [val["name"] for val in col_specs]
        else:
            current_schema_cols = list(self.ch.describe_table(table).keys())
        cols_to_be_added = [val for val in col_specs if val['name'] not in current_schema_cols]
        if len(cols_to_be_added) > 0:
            self.ch.alter_table(table, cols_to_be_added)
            current_schema_cols = list(self.ch.describe_table(table).keys())
        first_cols = current_schema_cols[:]

        # Write events
        def are_equal(arr1, arr2, n, m):
            if n != m:
                return False
            arr1.sort()
            arr2.sort()
            for i in range(n):
                if arr1[i] != arr2[i]:
                    return False
            return True

        def is_subarray(initial, this_one, n, m):
            if n <= m:
                return False
            for i in range(m):
                if not this_one[i] in initial:
                    return False
            return True

        try:
            data_to_write = []
            for data_instance in data_instance_array:
                # Basic check if data have same schema
                if data_instance["cenote"]["url"] != first_event["cenote"]["url"]:
                    raise Exception("Data don't belong to the same table!")

                data = self.create_data_write_obj(data_instance["data"], [])
                data = self.append_cenote_info(data_instance, data)
                this_cols = list(map(lambda x: x["column"], data))
                if (not are_equal(deepcopy(first_cols), deepcopy(this_cols), len(first_cols),
                                  len(this_cols))) and (not is_subarray(first_cols, this_cols, len(first_cols),
                                                                        len(this_cols))):
                    raise Exception("Data don't have the same attributes!")
                data = [val for val in data if val['column'] in current_schema_cols]
                data_to_write.append(data)

            res = self.ch.write_data(table, data_to_write)
            if res["response"] != 201:
                raise Exception(res["exception"])
            return {"response": 201}
        except Exception as e:
            return {"response": 400, "exception": repr(e)}
