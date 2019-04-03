import os
import re

import psycopg2
import psycopg2.extras


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
                self.cur.execute(
                    "ALTER TABLE IF EXISTS %s ADD COLUMN IF NOT EXISTS %s" % (table_name, column_declarator))
        except Exception as e:
            return {"response": 400, "exception": e}
        return {"response": 201}

    def describe_table(self, table_name):
        self.cur.execute("SELECT * FROM %s LIMIT 1" % table_name)
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

        query = "INSERT INTO %s %s VALUES %s" % (table_name, column_list, values_list)

        try:
            self.cur.execute(query)
        except Exception as e:
            return {"response": 400, "exception": e}

        return {"response": 201}
