import os
import re

import psycopg2
import psycopg2.extras
import redis
from dotenv import load_dotenv

from lua_scripts import lua_1, lua_2

load_dotenv()

class CockroachHandler:
    """
    This class implements a handler for the CockroachDB database.
    """

    def __init__(self):
        """
        Initializes this handler. The initialization uses the DATABASE_URL env variable
        """
        try:
            # Connect to cluster
            self.connection = psycopg2.connect(
                os.getenv('DATABASE_URL', ''), cursor_factory=psycopg2.extras.DictCursor)
            self.connection.set_session(autocommit=True)
            self.cur = self.connection.cursor()
            self.r = redis.Redis(host=os.getenv('REDIS_HOST', '155.207.19.237'), port=os.getenv('REDIS_PORT', 6379),
                                 db=os.getenv('REDIS_DB', 0), password=os.getenv('REDIS_PASSWORD', ''))
            self.update_running_values = self.r.register_script(lua_1)
            # Historical aggregates Lua script
            self.update_historical_aggregates = self.r.register_script(lua_2)
        except Exception as e:
            raise e

    def __del__(self):
        self.cur.close()
        self.connection.close()
        self.r.connection_pool.disconnect()

    def create_table(self, table_name, column_specs):
        """
        Registers a new table at the database

        :param table_name: the name of the table
        :param column_specs: An array of objects, each containing the column specifications
                Example object:
                    e.g.:{
                            "name": "name_of_column",
                            "type": "type_of_column",
                            "primary_key": "yes"
                        }
        """
        column_declarator = "("
        for column in column_specs:
            column_declarator += '"' + column["name"] + '" ' + column["type"]
            if "primary_key" in column:
                column_declarator += " PRIMARY KEY"
            column_declarator += ', '
        column_declarator = column_declarator[:-2] + ")"
        try:
            self.cur.execute(
                f"CREATE TABLE IF NOT EXISTS {table_name} {column_declarator}")
            self.cur.execute(
                f"CREATE INDEX IF NOT EXISTS timestamp_index ON {table_name} (cenote$timestamp)")
        except Exception as e:
            return {"response": 400, "exception": e}

        return {"response": 201}

    def alter_table(self, table_name, column_specs):
        """
        Alters a pre-existing table in the database

        :param table_name: the name of the table
        :param column_specs: An array of objects, each containing the column specifications
                Example object:
                    e.g.:{
                            "name": "name_of_column",
                            "type": "type_of_column",
                            "primary_key": "yes"
                        }
        """
        try:
            for column in column_specs:
                column_declarator = column["name"] + ' ' + column["type"]
                if "primary_key" in column:
                    column_declarator += " PRIMARY KEY"
                self.cur.execute(
                    f"ALTER TABLE IF EXISTS {table_name} ADD COLUMN IF NOT EXISTS {column_declarator}")
        except Exception as e:
            return {"response": 400, "exception": e}
        return {"response": 201}

    def describe_table(self, table_name):
        self.cur.execute(f"SELECT * FROM {table_name} LIMIT 1")
        return self.cur.fetchone()

    def write_data(self, table_name, data_instance_array):
        """
        Writes data into a certain table

        :param table_name: the name of the table
        :param data_instance_array: array of data_instance
               data_instance: An array of objects that contain the values to be inserted in each column
                Example object:
                    e.g.:{
                            "column": "name_of_column",
                            "value": "the_value_to_be_inserted",
                            "built_in_function": "current_timestamp()"
                        }
            The data registration process supports two types:
                1) value: Contains the raw value to be inserted into the table
                2) built_in_function: Provides the name of the built-in function to be used for generating the value
        """

        # Get info from first event only
        first_event = data_instance_array[0]
        column_list = "("
        pattern = re.compile(r'\'')
        for value_descriptor in first_event:
            column_list += '"' + value_descriptor["column"] + '", '
        column_list = column_list[:-2] + ")"
        all_values_to_write = []
        all_column_names = [value_descriptor["column"]
                            for value_descriptor in first_event]
        redis_fail = None

        for data_instance in data_instance_array:
            values_list = "("
            for column_name in all_column_names:
                value_descriptor = [
                    x for x in data_instance if x["column"] == column_name]
                if len(value_descriptor) > 0:
                    if 'value' in value_descriptor[0]:
                        if type(value_descriptor[0]["value"]) is str:
                            values_list += "'" + \
                                pattern.sub(
                                    "''", str(value_descriptor[0]["value"])) + "'"
                        else:
                            values_list += str(value_descriptor[0]["value"])
                    else:
                        values_list += value_descriptor[0]["built_in_function"]
                else:
                    values_list += 'NULL'
                values_list += ', '
            values_list = values_list[:-2] + ")"
            all_values_to_write.append(values_list)

            redis_fail = None
            for vd in data_instance:
                if 'value' in vd and not vd["column"].startswith("cenote") and (
                        type(vd["value"]) is int or type(vd["value"]) is float):
                    try:
                        with self.r.pipeline() as pipe:
                            while True:
                                try:
                                    pipe.watch(f"{table_name}_{vd['column']}")
                                    self.update_running_values(keys=[f"{table_name}_{vd['column']}"],
                                                               args=[
                                                                   vd['value']],
                                                               client=pipe)
                                    pipe.execute()
                                    break
                                except redis.WatchError:
                                    continue
                    except Exception as e:
                        redis_fail = e

            # Historical aggregates
            if('installations' in table_name):
                for vd in data_instance:
                    if vd["column"] == 'cenote$timestamp':
                        split = vd['value'].split(':')
                        date = split[0].split('T')[0]
                        month = date[:7]
                        hour = split[0].split('T')[1]

                redis_fail = None
                for vd in data_instance:
                    if 'value' in vd and vd["column"] == "active" and (type(vd["value"]) is int or type(vd["value"]) is float):
                        try:
                            with self.r.pipeline() as pipe:
                                while True:
                                    try:
                                        pipe.watch(
                                            f"{table_name}_{vd['column']}_hist")
                                        self.update_historical_aggregates(
                                            keys=[
                                                f"{table_name}_{vd['column']}_hist"],
                                            args=[vd['value'],
                                                  date, month, hour],
                                            client=pipe)
                                        pipe.execute()
                                        break
                                    except redis.WatchError:
                                        continue
                        except Exception as e:
                            redis_fail = e

        query = f"INSERT INTO {table_name} {column_list} VALUES {','.join(map(str, all_values_to_write))}"

        try:
            self.cur.execute(query)
        except Exception as e:
            return {"response": 400, "exception": e}
        return {"response": 400, "exception": repr(redis_fail)} if redis_fail else {"response": 201}
