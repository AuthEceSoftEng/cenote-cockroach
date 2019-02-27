import re

import psycopg2
import psycopg2.extras

from utils.properties import DATABASE_URL


class CockroachHandler:
    """
    This class implements a handler for the CockroachDB database.
    """

    def __init__(self):
        """
        Initializes this handler. The initialization uses the DATABASE_URL variable set in
        the properties.py file
        """
        try:
            # Connect to cluster
            self.connection = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.NamedTupleCursor)
            self.connection.set_session(autocommit=True)
            self.cur = self.connection.cursor()
        except Exception as e:
            raise e

    def get_database_tables(self):
        """
        Retrieve the information of the tables of the database
        """
        query = "SELECT table_name from information_schema.tables WHERE table_schema='public';"
        self.cur.execute(query)
        results = self.cur.fetchall()
        tables_info = [table_info[0] for table_info in results]

        return tables_info

    def check_if_table_exists(self, table):
        """
        Check if a given table exists in the database

        :param table: the name of the table
        :returns: True/False depending on whether the table exists
        """
        query = "SELECT table_name from information_schema.tables WHERE table_schema='public';"
        self.cur.execute(query)
        results = self.cur.fetchall()
        tables_info = [table_info[0] for table_info in results]

        return table in tables_info

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
        for (i, column) in enumerate(column_specs):
            if "primary_key" in column:
                column_declarator += column["name"] + ' ' + column["type"] + " PRIMARY KEY"
            else:
                column_declarator += column["name"] + ' ' + column["type"]
            if i < (len(column_specs) - 1):
                column_declarator += ', '
        column_declarator += ")"
        query = "CREATE TABLE %s %s" % (table_name, column_declarator)
        try:
            self.cur.execute(query)
        except Exception as e:
            return {"response": 400, "exception": e}

        return {"response": 201}

    def alter_table(self, table_name, column_specs, alter_type):
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
        :param alter_type: ADD or DROP
        """
        try:
            if alter_type == "ADD":
                for column in column_specs:
                    column_declarator = ""
                    if "primary_key" in column:
                        column_declarator += column["name"] + ' ' + column["type"] + " PRIMARY KEY"
                    else:
                        column_declarator += column["name"] + ' ' + column["type"]
                    query = "ALTER TABLE %s ADD COLUMN %s" % (table_name, column_declarator)
                    self.cur.execute(query)
            elif alter_type == "DROP":
                for column in column_specs:
                    column_declarator = ""
                    if "primary_key" in column:
                        column_declarator += column["name"] + ' ' + column["type"] + " PRIMARY KEY"
                    else:
                        column_declarator += column["name"] + ' ' + column["type"]
                    query = "ALTER TABLE %s ADD COLUMN %s" % (
                        table_name, column_declarator)
                    self.cur.execute(query)
        except Exception as e:
            return {"response": 400, "exception": e}
        return {"response": 201}

    def describe_table(self, table_name):
        query = "SHOW COLUMNS FROM %s" % table_name
        self.cur.execute(query)
        return self.cur.fetchall()

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
                2) built_in_function: Provides the name of the built-in function to be used for auto-generating the value
        """
        column_list = "("
        values_list = "("
        for (i, value_descriptor) in enumerate(data_instance):

            column_list += '"' + value_descriptor["column"] + '"'
            if 'value' in value_descriptor:
                if type(value_descriptor["value"]) is str:
                    values_list += "'" + re.sub(r'\'', "\'\'", str(value_descriptor["value"])) + "'"
                else:
                    values_list += str(value_descriptor["value"])
            else:
                values_list += value_descriptor["built_in_function"]
            if i < (len(data_instance) - 1):
                column_list += ', '
                values_list += ', '
        column_list += ")"
        values_list += ")"

        query = "INSERT INTO %s %s VALUES %s" % (table_name, column_list, values_list)

        try:
            self.cur.execute(query)
        except Exception as e:
            return {"response": 400, "exception": e}

        return {"response": 201}

    def read_data(self, table_name, list_of_columns, conditions):
        """
        Reads data from a certain table

        :param table_name: the name of the table
        :param list_of_columns: An array containing the list of the columns to be returned
        :param conditions: An array of objects that contain the select specifications per column
                Example object:
                    e.g.:{
                            "column": "name_of_column",
                            "operand": ""
                            "value": "the_value_to_be_inserted",
                        }
            The supported operands are the following:
                1) value: Contains the raw value to be inserted into the table
                2) built_in_function: Provides the name of the built-in
                        function to be used for auto-generating the value
        """

        select_clause = ', '.join(list_of_columns) if (
           isinstance(list_of_columns, list) and len(list_of_columns) > 0) else "*"

        if len(conditions) > 0:
            where_clause = "WHERE"
            for (i, condition) in enumerate(conditions):
                if type(condition["value"]) is str:
                    where_clause += " " + condition["column"] + " " \
                                    + condition["operand"] + " '" + re.sub(r'\'', "\'\'", str(condition["value"])) + "'"
                else:
                    where_clause += " " + condition["column"] + " " + condition["operand"] + " " + str(
                        condition["value"])
                if i < (len(conditions) - 1):
                    where_clause += " AND"
        else:
            where_clause = ""

        query = "SELECT %s FROM %s %s" % (select_clause, table_name, where_clause)

        try:
            self.cur.execute(query)
            data = self.cur.fetchall()
            # data = [dict(item) for item in self.execute_query(query)]
        except Exception as e:
            return {"response": 400, "exception": e}

        return {"response": 200, "data": data}
