import json
from datetime import datetime

from utils.CockroachHandler import CockroachHandler


class WriteData:
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

    def write_data(self, data_instance):

        if type(data_instance) is str:
            data_instance = json.loads(data_instance)
        table = self.get_table(data_instance["cenote"]["url"])
        col_specs = self.create_column_specs(data_instance["data"], [])
        res = self.create_table(table, col_specs)
        if res["response"] == 201:
            data = self.create_data_write_obj(data_instance["data"], [])
            data = self.append_cenote_info(data_instance, data)

            # Create missing columns in current schema
            if self.ch.describe_table(table) is None:
                current_schema_cols = [val["name"] for val in col_specs]
            else:
                current_schema_cols = list(self.ch.describe_table(table).keys())
            cols_to_be_added = [val for val in col_specs if val['name'] not in current_schema_cols]
            if len(cols_to_be_added) > 0:
                self.ch.alter_table(table, cols_to_be_added)
                current_schema_cols = list(self.ch.describe_table(table).keys())

            data = [val for val in data if val['column'] in current_schema_cols]
            res = self.ch.write_data(table, data)
        return res
