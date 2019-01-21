import json
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import ordered_dict_factory
from CassandraHandler.utils.properties import CassandraPort, ClusterIPs

class CassandraHandler:
    """
    This class implements a handler for the Apache Cassandra database.
    """
    def __init__(self):
        """
        Initializes this Cassandra handler. The initialization uses the ClusterIPs and the CassandraPort variables set in
        the properties.py file
        """
        try:
            # Connect to cluster
            self.cluster = Cluster(ClusterIPs, load_balancing_policy=DCAwareRoundRobinPolicy(), port = CassandraPort)
            self.session = self.cluster.connect()

            # Get available keyspaces
            self.keyspaces = [key.keyspace_name for key in self.session.execute('SELECT * FROM system_schema.keyspaces')]

            # Set Ordered dict as queries responses
            self.session.row_factory = ordered_dict_factory
        except Exception as e:
            raise(e)

    def check_if_keyspace_exists(self, keyspace):
        """
        Checks whether a keyspace exists

        :param keyspace: the name of the keyspace
        :returns: True/False
        """
        if(keyspace in self.keyspaces):
            return True
        else:
            return False

    def execute_query(self, query_string):
        """
        Executes a provided query and returns the response

        :param query_string: the query
        :returns: The query response
        """
        return self.session.execute(query_string)


    def set_keyspace(self, keyspace):
        """
        Sets the keyspace to be used queries

        :param keyspace: the name of the keyspace
        """
        if(self.check_if_keyspace_exists(keyspace)):
            self.session.set_keyspace(keyspace)
        else:
            raise Exception("The requested keyspace (" + keyspace + ") does not exist")

    def create_keyspace(self, keyspace_name):
        """
        Creates a new keyspace

        :param keyspace_name: the name of the keyspace
        """
        query = """
            CREATE KEYSPACE %s
                WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
        """ % keyspace_name

        # It will raise an error if keyspace already exists
        self.execute_query(query)

    def drop_keyspace(self, keyspace_name):
        """
        Deletes an existing keyspace

        :param keyspace_name: the name of the keyspace
        """
        query = "DROP KEYSPACE %s" % keyspace_name
        if(self.check_if_keyspace_exists(keyspace_name)):
            self.execute_query(query)
        else:
            raise Exception("The requested keyspace (" + keyspace_name + ") does not exist")

    def get_keyspace_tables(self, keyspace):
        """
        Retrieve the information of the tables for a given keyspace

        :param keyspace: the name of the keyspace
        """
        query = "SELECT * FROM system_schema.tables WHERE keyspace_name='%s'" % keyspace
        results = self.execute_query(query)
        tables_info = [table_info for table_info in results]

        return tables_info

    def check_if_table_exists(self, keyspace, table):
        """
        Check if a given table exists in a given keyspace

        :param keyspace: the name of the keyspace
        :param table: the name of the table
        :returns: True/False depending on whether the keyspace exists
        """
        query = "SELECT table_name FROM system_schema.tables WHERE keyspace_name='%s'" % keyspace
        results = self.execute_query(query)
        tables_info = [table_info["table_name"] for table_info in results]

        return table in tables_info

    def create_table(self, keyspace, table_name, column_specs):
        """
        Registers a new table in a given keyspace

        :param keyspace: the name of the keyspace
        :param table_name: the name of the table
        :param column_specs: An array of objects, each containing the column specifications
                Example object:
                    e.g.:{
                            "name": "name_of_column",
                            "type": "type_of_column",
                            "primary_key": "yes"
                        }
            For columns that should not be primary keys DO NOT USE the key: primary-key in the specifications
            The type can be any of the data types described in the following url:
            https://docs.datastax.com/en/cql/3.3/cql/cql_reference/cql_data_types_c.html
        """
        column_declarator = "("
        for (i, column) in enumerate(column_specs):
            if("primary_key" in column):
                column_declarator += column["name"] + " " + column["type"] + " PRIMARY KEY"
            else:
                column_declarator += column["name"] + " " + column["type"]
            if(i < (len(column_specs) - 1)):
                column_declarator += ', '
        column_declarator += ")"
        query = """
            CREATE TABLE %s.%s %s
        """ % (keyspace, table_name, column_declarator)

        try:
            self.execute_query(query)
        except Exception as e:
            return { "response":400, "exception": e }

        return { "response":201 }

    def write_data(self, keyspace, table_name, data_instance):
        """
        Writes data into a certain column family

        :param keyspace: the name of the keyspace
        :param table_name: the name of the table
        :param data_instance: An array of objects that contain the values to be inserted in each column
                Example object:
                    e.g.:{
                            "column": "name_of_column",
                            "value": "the_value_to_be_inserted",
                            "built_in_function": "now()"
                        }
            The data registration process supports two types:
                1) value: Contains the raw value to be inserted into the table
                2) built_in_function: Provides the name of the cassandra built-in
                        function to be used for auto-generating the value
        """
        column_list = "("
        values_list = "("
        for (i, value_descriptor) in enumerate(data_instance):

            column_list += value_descriptor["column"]
            if('value' in value_descriptor):
                if(type(value_descriptor["value"]) is str):
                    values_list += "'" + str(value_descriptor["value"]) + "'"
                else:
                    values_list += str(value_descriptor["value"])
            else:
                values_list += value_descriptor["built_in_function"]
            if(i < (len(data_instance) - 1)):
                column_list += ', '
                values_list += ', '
        column_list += ")"
        values_list += ")"

        query = """
            INSERT INTO %s.%s %s VALUES %s
        """ % (keyspace, table_name, column_list, values_list)
        try:
            self.execute_query(query)
        except Exception as e:
            return { "response":400, "exception": e }

        return { "response":201 }
    
    def read_data(self, keyspace, table_name, list_of_columns, conditions):
        """
        Reads data from a certain column family

        :param keyspace: the name of the keyspace
        :param table_name: the name of the table
        :param list_of_columns: An array containing the list of the columns to be returned
        :param condtitions: An array of objects that contain the select specifications per column
                Example object:
                    e.g.:{
                            "column": "name_of_column",
                            "operand": ""
                            "value": "the_value_to_be_inserted",
                        }
            The supported operands are the following:
                1) value: Contains the raw value to be inserted into the table
                2) built_in_function: Provides the name of the cassandra built-in
                        function to be used for auto-generating the value
        """
        
        select_clause = ', '.join(list_of_columns) if (isinstance(list_of_columns, list) and len(list_of_columns) > 0) else "*"
        
        if(len(conditions) > 0):
            where_clause = "WHERE"
            for (i, condition) in enumerate(conditions):
                if(type(condition["value"]) is str):
                    where_clause += " " + condition["column"] + " " + condition["operand"] + " '" + str(condition["value"]) + "'"
                else:
                    where_clause += " " + condition["column"] + " " + condition["operand"] + " " + str(condition["value"])
                if(i < (len(conditions) - 1)):
                    where_clause += " AND"
        else:
            where_clause = ""
        
        query = """
             SELECT %s FROM %s.%s %s ALLOW FILTERING
        """ % (select_clause, keyspace, table_name, where_clause)
        
        try:
            data = [dict(item) for item in self.execute_query(query)]
        except Exception as e:
            return { "response":400, "exception": e }
  
        return { "response":200, "data": data }


    def update_data(self, keyspace, table_name, data_instance, uuid):
        """
        Updated a table row in a given keyspace based on the uuid

        :param keyspace: the name of the keyspace
        :param table_name: the name of the table
        :param data_instance: An array of objects that contain the values to be inserted in each column
                Example object:
                    e.g.:{
                            "column": "name_of_column",
                            "value": "the_value_to_be_inserted",
                            "built_in_function": "now()"
                        }
            The data registration process supports two types:
                1) value: Contains the raw value to be inserted into the table
                2) built_in_function: Provides the name of the cassandra built-in
                        function to be used for auto-generating the value
        """

        args = ""
        for (i, value_descriptor) in enumerate(data_instance):

            args += value_descriptor["column"] + "="
            if('value' in value_descriptor):
                if(type(value_descriptor["value"]) is str):
                    args += "'" + str(value_descriptor["value"]) + "'"
                else:
                    args += str(value_descriptor["value"])
            else:
                args += value_descriptor["built_in_function"]
            if(i < (len(data_instance) - 1)):
                args += ', '


        query = """
            UPDATE %s.%s SET %s WHERE uuid=%s
        """ % (keyspace, table_name, args, uuid)
        try:
            self.execute_query(query)
        except Exception as e:
            return { "response":400, "exception": e }

        return { "response":201 }