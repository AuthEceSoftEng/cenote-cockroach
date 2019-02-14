import json
import datetime
from CassandraHandler.utils.CassandraHandler import CassandraHandler
from CassandraHandler.utils.helpers import get_time_in_ms, time_to_datetime_in_ms

class WriteData:
    """
    This class implements the data write functionality in Cenote.
    """
    def __init__(self):
        """
        Initializes this data writer.
        """
        self.ch = CassandraHandler()
        self.excluded_columns = ["uuid"]
        self.nested_properties_sep = '$'

    def create_table(self, keyspace, table_name, column_specs):

        # Every table should have a uuid column which will be used as primary key
        column_specs.append({"name": "uuid", "type": "UUID", "primary_key": "yes"})

        # Every table should have a cenote created_at column, a timestamp end column and an id column
        column_specs.append({"name": "cenote" + self.nested_properties_sep + "created_at", "type": "timestamp"})
        column_specs.append({"name": "cenote" + self.nested_properties_sep + "timestamp", "type": "timestamp"})
        column_specs.append({"name": "cenote" + self.nested_properties_sep + "id", "type": "text"})

        return self.ch.create_table(keyspace, table_name, column_specs)

    def get_column_family(self, url):

        if(url.endswith('/')):
            url = url[:-1]

        info = url.split('/projects/')
        project_id = info[len(info) - 1].split('/events/')[0]
        event_collection = info[len(info) - 1].split('/events/')[1]
        column_family = project_id + '_' + event_collection

        return column_family

    def create_column_specs(self, obj, col_specs = [], prev_key = ''):

        for key in obj:
            if(type(obj[key]) is dict):
                if(prev_key != ''):
                    self.create_column_specs(obj[key], col_specs, prev_key + self.nested_properties_sep + key.replace(' ', '').lower())
                else:
                    self.create_column_specs(obj[key], col_specs, key.replace(' ', '').lower())
            else:
                info = {}
                if(prev_key != ''):
                    info["name"] = prev_key + self.nested_properties_sep + key.replace(' ', '').lower()
                else:
                    info["name"] = key.replace(' ', '').lower()
                if(type(obj[key]) is str):
                    info["type"] = "text"
                else:
                    info["type"] = "float"
                col_specs.append(info)

        return col_specs

    def create_data_write_obj(self, obj, data = [], prev_key = ''):

        for key in obj:
            if(type(obj[key]) is dict):
                if(prev_key != ''):
                    self.create_data_write_obj(obj[key], data, prev_key + self.nested_properties_sep + key.replace(' ', '').lower())
                else:
                    self.create_data_write_obj(obj[key], data, key.replace(' ', '').lower())
            else:
                info = {}
                if(prev_key != ''):
                    info["column"] = prev_key + self.nested_properties_sep + key.replace(' ', '').lower()
                else:
                    info["column"] = key.replace(' ', '').lower()
                info["value"] = obj[key]

                data.append(info)

        return data

    def append_cenote_info(self, obj, data = [], initial_state = False, curr_state = {}):

        if("timestamp" in obj["cenote"].keys()):
            timestamp = obj["cenote"]["timestamp"]
        else:
            timestamp = get_time_in_ms()

        data.append({"column": "cenote" + self.nested_properties_sep + "created_at", "value": obj["cenote"]["created_at"]})
        data.append({"column": "cenote" + self.nested_properties_sep + "timestamp", "value": timestamp})
        data.append({"column": "cenote" + self.nested_properties_sep + "id", "value": obj["cenote"]["id"]})
        data.append({"column": "uuid", "built_in_function": "now()"})

        return data

    def write_data(self, keyspace, data_instance):

        if(type(data_instance) is str):
            data_instance = json.loads(data_instance)
        column_family = self.get_column_family(data_instance["cenote"]["url"])
        
        col_specs = self.create_column_specs(data_instance["data"], [])
        
        if(not self.ch.check_if_table_exists(keyspace, column_family)):
            res = self.create_table(keyspace, column_family, col_specs)

            if(res["response"] == 201 or \
               "java.lang.RuntimeException: java.util.concurrent.ExecutionException: org.apache.cassandra.exceptions.ConfigurationException: Column family ID mismatch" in str(res['exception'])
               ):
                data = self.create_data_write_obj(data_instance["data"], [])
                data = self.append_cenote_info(data_instance, data, initial_state=True)
                
                res = self.ch.write_data(keyspace, column_family, data)

                return res
            else:
                
                return res
        else:
            curr_state = {}

            data = self.create_data_write_obj(data_instance["data"], [])
            data = self.append_cenote_info(data_instance, data, initial_state=True, curr_state=curr_state)
            
            # Create missing columns in current schema
            current_schema_cols = [val["column_name"] for val in self.ch.describe_table(keyspace, column_family)]
            cols_to_be_Added = [val for val in col_specs if val['name'] not in current_schema_cols]
            
#             if(len(cols_to_be_Added) > 0):
#                 self.ch.alter_table(keyspace, column_family, cols_to_be_Added, "ADD")
            
            data = [val for val in data if val['column'] in current_schema_cols]
            
            res = self.ch.write_data(keyspace, column_family, data)
            
            return res