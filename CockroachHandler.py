import os
import re

import psycopg2
import psycopg2.extras
import redis


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
            self.connection = psycopg2.connect(os.getenv(
                'DATABASE_URL', 'postgres://cockroach@155.207.19.234:30591/cenote?sslmode=disable'),
                cursor_factory=psycopg2.extras.DictCursor)
            self.connection.set_session(autocommit=True)
            self.cur = self.connection.cursor()
            self.r = redis.Redis(host=os.getenv('REDIT_HOST', 'localhost'), port=os.getenv('REDIS_PORT', 6379),
                                 db=os.getenv('REDIS_DB', 0))
            lua_script = """
                local old_vals = redis.call('get',KEYS[1])
                local new_vals = {}
                if (old_vals) then
                    old_vals = cjson.decode(old_vals)
                    new_vals["count"] = old_vals['count'] + 1
                    local delta = ARGV[1] - old_vals["mean"]
                    new_vals["mean"] = old_vals["mean"] + delta / new_vals["count"]
                    new_vals["M2"] = old_vals["M2"] + delta * (ARGV[1] - new_vals["mean"])
                    new_vals["variance"] = new_vals["M2"] / new_vals["count"]
                else
                    new_vals["count"] = 1
                    new_vals["mean"] = ARGV[1]
                    new_vals["M2"] = 0
                    new_vals["variance"] = 0
                end
                redis.call('set', KEYS[1], cjson.encode(new_vals))"""
            self.update_running_values = self.r.register_script(lua_script)
        except Exception as e:
            raise e

    def __del__(self):
        self.cur.close()
        self.connection.close()

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
            column_declarator += column["name"] + ' ' + column["type"]
            if "primary_key" in column:
                column_declarator += " PRIMARY KEY"
            column_declarator += ', '
        column_declarator = column_declarator[:-2] + ")"
        try:
            self.cur.execute("CREATE TABLE IF NOT EXISTS %s %s" % (table_name, column_declarator))
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
                self.cur.execute(f"ALTER TABLE IF EXISTS {table_name} ADD COLUMN IF NOT EXISTS {column_declarator}")
        except Exception as e:
            return {"response": 400, "exception": e}
        return {"response": 201}

    def describe_table(self, table_name):
        self.cur.execute(f"SELECT * FROM {table_name} LIMIT 1")
        return self.cur.fetchone()

    def write_data(self, table_name, data_instance):
        """
        Writes data into a certain table

        :param table_name: the name of the table
        :param data_instance: An array of objects that contain the values to be inserted in each column
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
        column_list = "("
        values_list = "("
        pattern = re.compile(r'\'')
        for value_descriptor in data_instance:

            column_list += '"' + value_descriptor["column"] + '"'
            if 'value' in value_descriptor:
                if type(value_descriptor["value"]) is str:
                    values_list += "'" + pattern.sub("''", str(value_descriptor["value"])) + "'"
                else:
                    values_list += str(value_descriptor["value"])
            else:
                values_list += value_descriptor["built_in_function"]
            column_list += ', '
            values_list += ', '
        column_list = column_list[:-2] + ")"
        values_list = values_list[:-2] + ")"

        query = f"INSERT INTO {table_name} {column_list} VALUES {values_list}"

        try:
            self.cur.execute(query)
        except Exception as e:
            return {"response": 400, "exception": e}

        redis_fail = False
        for vd in data_instance:
            if type(vd["value"]) is int or type(vd["value"]) is float:
                try:
                    self.update_running_values(keys=[f"{table_name}_{vd['column']}"], args=[vd['value']])
                except:
                    redis_fail = True

        return {"response": 400} if redis_fail else {"response": 201}