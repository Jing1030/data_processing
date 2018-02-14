import math
import pyodbc
import mysql.connector
import getpass
import pkg_resources
import pandas as pd

from mysql.connector import errorcode
from sshtunnel import SSHTunnelForwarder

###########################
#                         #
# Functions:              #
# connect_db              #
# close_db_connection     #
# excute_with_error_check #
# check_table_exists      #
# make_table              #
# check_fk                #
# delete_table            #
# clear_table             #
# make_row                #
# make_many_rows          #
# update_row              #
# update_many_rows        #
# format_value            #
#                         #
###########################

class DatabaseConnection:

    def __init__(self, sql_version, db_name=None, firewall=False, 
                 key_loc=None, db_pwd=None, key_pwd=None):
        self.sql_version = sql_version
        self.db_name = db_name
        self.firewall = firewall
        self.key_loc = key_loc
        self.db_pwd = db_pwd
        self.key_pwd = key_pwd
        self.connection = None
        self.server = None
        self.cursor = None

    def get_connection(self):
        if self.connection is None:
            self.start_connection()
        return self.connection, self.server

    def get_cursor(self):
        if self.cursor is None:
            con, serv = self.get_connection()
            self.cursor = con.cursor()
        return self.cursor

    def start_connection(self):
        """
            Connects to database
        """
        config_df = self.load_config()
        if self.db_name is None:
            self.db_name = config_df.loc["default_db", "value"]
        
        if self.sql_version == "MySQL":
            host_ip = config_df.loc["host_ip", "value"]
            user_name = config_df.loc["host_user_name", "value"]
            sql_user_name = config_df.loc["mysql_user_name", "value"]
            db_prefix = config_df.loc["db_prefix", "value"]
            if self.db_pwd is None:
                try: 
                    self.db_pwd = config_df.loc["db_pwd", "value"]
                except KeyError:
                    self.db_pwd = getpass.getpass("MySQL password: ")
            
            # Connect to MySQL not behind a firewall
            if self.firewall == False:
                self.connection = mysql.connector.connect(user=sql_user_name, password=self.db_pwd, 
                                                          host=host_ip,
                                                          database="{}{}".format(db_prefix, self.db_name))
                self.server = None
            # Connect to MySQL behind a firewall by ssh tunneling
            else:
                if self.key_loc is None:
                    self.key_loc = config_df.loc["key_loc", "value"]
                if self.key_pwd is None:
                    try:
                        self.key_pwd = config_df.loc["key_pwd", "value"]
                    except KeyError:
                        self.key_pwd = getpass.getpass("ssh key password: ")
                self.server = SSHTunnelForwarder((host_ip, 22), ssh_pkey=self.key_loc, 
                                                    ssh_private_key_password=self.key_pwd, 
                                                    ssh_username=user_name, 
                                                    remote_bind_address=("127.0.0.1", 3306))
                self.server.start()
                self.connection = mysql.connector.connect(user=sql_user_name, password=self.db_pwd, 
                                                          host="127.0.0.1",
                                                          database="{}{}".format(db_prefix, self.db_name), 
                                                          port=self.server.local_bind_port)
        elif self.sql_version == "MSSQL":
            server_str = config_df.loc["ms_server", "value"]
            connection_string = "DRIVER={{SQL Server Native Client 11.0}};SERVER={};DATABASE={};\
            Trusted_Connection=yes".format(server_str, self.db_name)
            self.connection = pyodbc.connect(connection_string)
            self.server = None
        else:
            print "Error: Please enter valid sql_version (MySQL or MSSQL)"
    
    def load_config(self):
        """
            Load database configuration from data/config.csv file
            Returns dataframe with configuration data
        """
        config_file = pkg_resources.resource_filename("data_processing", "data/config.csv")
        df = pd.read_csv(config_file, header=0, index_col=0)

        return df

    def close_connection(self):
        """
            Close the conncection to the database and the ssh tunnel if necessary
        """
        if self.connection is not None:
            self.connection.close()
            self.connection = None
        if self.server is not None:
            self.server.stop()
            self.server = None

    def close_cursor(self):
        """
            Close the cursor
        """
        if self.cursor is not None:
            self.cursor.close()
            self.cursor = None

    def check_table_exists(self, table_name):
        """
            Checks if a table with table_name already exists in the database
        """

        if self.sql_version == "MySQL":
            config_df = self.load_config()
            check_str = """SELECT * 
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME = '{}' AND TABLE_SCHEMA = '{}{}';
""".format(table_name, config_df.loc["db_prefix", "value"], self.db_name)
        else:
            check_str = """SELECT * 
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME = '{}' AND TABLE_SCHEMA = '{}';
""".format(table_name, self.db_name)
        cursor = self.get_cursor()
        cursor.execute(check_str)
        if cursor.fetchone() is None:
            return False
        else:
            return True

    def make_table(self, table_name, columns_dict, other_conditions=[]):
        """
            Creates a table

            table_name: name of the table to be created
            columns_dict: dictionary of column names with a list of conditions and datatypes
            other_conditions [optional]: a list of other conditions for the table, such as primary key
        """
        cursor = self.get_cursor()

        if self.check_table_exists(table_name):
            drop_in = raw_input("The table {} already exists. Would you like to drop and recreate it? (Y/N)".format(table_name))
            # Make sure input is only N or Y
            while True:
                if drop_in.upper() == "N" or drop_in.upper() == "Y":
                    break
                else:
                    drop_in = raw_input("""PLEASE ENTER Y or N. The table {} already exists. 
            Would you like to drop and recreate it? (Y/N)""".format(table_name))
            if drop_in.upper() == "N":
                print "No new table created"
                return False
            elif drop_in.upper() == "Y":
                print "Deleting table {}".format(table_name)
                deleted = self.delete_table(table_name)
                if deleted:
                    print "Table {} sucessfully deleted".format(table_name)
                else:
                    print "Table {} was not deleted".format(table_name)
                    return False
        print "Creating table {}".format(table_name)
        columns_list = []
        for key in columns_dict:
            cond = columns_dict[key]
            cond_str = " ".join(cond)
            columns_list += ["{} {}".format(key, cond_str)]
        columns_list += other_conditions
        column_str = """,
    """.join(columns_list)

        if self.sql_version == "MySQL":
            create_str = "CREATE TABLE {} ({}) ENGINE=InnoDB;".format(table_name, column_str)
        else:
            create_str = "CREATE TABLE {} ({});".format(table_name, column_str)
        print create_str
        success = self.excute_with_error_check(create_str)

        if success:
            print "Sucessfully created table {}".format(table_name)

        return True

    def check_fk(self, table):
        """
            Checks if the table has foreign key constraints 
        """
        cursor = self.get_cursor()
        if self.sql_version == "MySQL":
            config_df = self.load_config()
            check_str = """SELECT
ke.REFERENCED_TABLE_NAME parent,
ke.TABLE_NAME child,
ke.CONSTRAINT_NAME
FROM
INFORMATION_SCHEMA.KEY_COLUMN_USAGE ke
WHERE
ke.REFERENCED_TABLE_NAME LIKE '{}' AND ke.REFERENCED_TABLE_SCHEMA ='{}{}';""".format(table, 
                                                                                   config_df.loc["db_prefix", "value"],
                                                                                   self.db_name)
            cursor.execute(check_str)
            fk_tables = []
            for (parent, child, constraint) in cursor:
                fk_tables += [child]
        else:
            check_str = "EXEC sp_fkeys '{}'".format(table)
            cursor.execute(check_str)
            fk_tables = [row.FKTABLE_NAME for row in cursor.fetchall()]
        return fk_tables

    def delete_table(self, table):
        """
            Deletes the passed in table from the database
        """
        cursor = self.get_cursor()
        connection, server = self.get_connection()
        fk_tables = self.check_fk(table)
        if fk_tables == []:
            cursor.execute("DROP TABLE {};".format(table))
            connection.commit()
            return True
        else:
            drop_fk = raw_input("""The table(s) {} have foreign key contstrants on table {}. 
It is necessary to drop these tables to drop {}. 
Would you like to continue and drop these tables? (Y/N)""".format(fk_tables, table, table))
            # Make sure input is only N or Y
            while True:
                if drop_fk.upper() == "N" or drop_fk.upper() == "Y":
                    break
                else:
                    drop_fk = raw_input("""PLEASE ENTER Y or N. The tables {} have foreign key contstrants on table {}. 
It is necessary to drop these tables to drop {}. 
Would you like to continue and drop these tables? (Y/N)""".format(fk_tables, table, table))
            if drop_fk.upper() == "N":
                return False
            elif drop_fk.upper() == "Y":
                for child_table in fk_tables:
                    # recursively delete dependent tables 
                    child_del = self.delete_table(child_table)
                    if not child_del:
                        return False 
                cursor.execute("DROP TABLE {};".format(table))
                connection.commit()
                return True

    def excute_with_error_check(self, execute_str):
        """
            Executes the passed in string with SQL type dependent error check

            execute_str: string to execute
        """
        cursor = self.get_cursor()
        connection, server = self.get_connection()
        if self.sql_version == "MSSQL":
            try:
                cursor.execute(execute_str)
                connection.commit()
                return True
            except pyodbc.ProgrammingError:
                print "Could not execute string {}".format(execute_str)
                return False
        else:
            try:
                cursor.execute(execute_str)
                connection.commit()
                return True
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    print "Table already exits"
                    return False
                else:
                    print err.msg
                    return False

    def clear_table(self, table):
        """
            Removes all of the data from the table, will also reset auto increment

            table: name of the table to clear
        """
        
        connection, server = self.get_connection()
        cursor = self.get_cursor()
        fks = self.check_fk(table)
        # can not truncate with foreign key contstraint
        if fks == []:
            cursor.execute("TRUNCATE TABLE {};".format(table))
            connection.commit()
        else:
            # remove contstraint, truncate table, add back constraint
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
            connection.commit()
            cursor.execute("TRUNCATE TABLE {};".format(table))
            connection.commit()
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
            connection.commit()
        # Too slow, leads to connection timeout
        """else:
            # deletion is slower and the auto increment needs to be reset
            cursor.execute("DELETE FROM {};".format(table))
            connection.commit()
            cursor.execute("ALTER TABLE {} AUTO_INCREMENT = 1;".format(table))
            connection.commit()"""

    def add_column(self, column_name, column_type, table):
        """
            Creates a new column in the given table

            column_name: name of the column to add 
            column_type: type of the new column
            table: name of table to add column to
        """
        connection, server = self.get_connection()
        cursor = self.get_cursor()

        add_str = "ALTER TABLE {} ADD {} {};".format(table, column_name, column_type)

        cursor.execute(add_str)
        connection.commit()


    def make_row(self, insert_dict, table):
        """
            Creates a new row with the passed in values in the given table

            insert_dict: dictionary of column name: value to insert
            table: name of table to insert row into
        """
        connection, server = self.get_connection()
        cursor = self.get_cursor()

        columns = insert_dict.keys()
        values = insert_dict.values()
        formated_val = [str(self.format_value(val)) for val in values]

        columns_str = ",".join(columns)
        values_str = ",".join(formated_val)

        insert_str = "INSERT INTO {} ({}) VALUES ({});".format(table, columns_str, values_str)

        cursor.execute(insert_str)
        connection.commit()

    def make_many_rows(self, insert_dict, table):
        """
            Instead of inserting a single row into the database, inserts up to 950
            in one statement, looping until all rows are inserted. Returns True if successful.

            insert_dict: dictionary of column name: list of value to insert
            table: name of table to insert rows into
        """
        MAX_VAL = 950.0

        connection, server = self.get_connection()
        cursor = self.get_cursor()

        columns = insert_dict.keys()
        values_list = insert_dict.values()

        # check to make sure length is the same
        len_val = len(values_list[0])
        same_len = all(len(x)==len_val for x in values_list)
        if not same_len:
            print "The list of values to be inserted are of unequal length"
            return False
        # split lists if length longer than 950
        num_chunk = math.ceil(len_val/MAX_VAL)
        for i in range(int(num_chunk)):
            chunk_start = int(i*MAX_VAL)
            chunk_end = min(int(i*MAX_VAL + MAX_VAL), len_val)
            chunk_val_list = [x[chunk_start:chunk_end] for x in values_list]
            
            # formats and inserts values
            total_val_list = []
            for j in range(len(chunk_val_list[0])):
                values = [col[j] for col in chunk_val_list]
                formated_val = [str(self.format_value(val)) for val in values]
                values_str = "({})".format(",".join(formated_val))
                total_val_list += [values_str]
            total_val = ",".join(total_val_list)
            columns_str = ",".join(columns)

            insert_str = "INSERT INTO {} ({}) VALUES {};".format(table, columns_str, total_val)

            cursor.execute(insert_str)
            connection.commit()

        return True


    def update_row(self, update_dict, condition_dict, table):
        """
            Updates a row in the passed in table

            update_dict: dictionary of column names and values to be updated
            condition_dict: dictionary of column names and values to identify the row to be updated
            table: name of table with row to be updated
        """
        connection, server = self.get_connection()
        cursor = self.get_cursor()

        update_list = []
        for key in update_dict:
            val = update_dict[key]
            val = self.format_value(val)
            update_list += ["{} = {}".format(key, val)]
        update_vals = ",".join(update_list)

        # can use to empty column
        if condition_dict == {}:
            update_str = "UPDATE {} SET {};".format(table, update_vals)

        else:
            condition_list = []
            for key in condition_dict:
                val = condition_dict[key]
                val = self.format_value(val)
                condition_list += ["{} = {}".format(key, val)]
            update_condition = " AND ".join(condition_list)

            update_str = "UPDATE {} SET {} WHERE {};".format(table, update_vals, update_condition)

        cursor.execute(update_str)
        connection.commit()

    def update_many_rows(self, update_dict, condition_dict, table):
        """
            Instead of updating a single row into the database, updates up to 950
            in one database connection.

            update_dict: dictionary of column name: list of value to add
            condition_dict: dictionary of column name: list of WHERE clause values
            table: name of table to update rows in
        """
        CHUNK_SIZE = 1000
        connection, server = self.get_connection()
        cursor = self.get_cursor()

        # check to make sure length is the same
        len_val = len(update_dict[update_dict.keys()[0]])
        same_len = all(len(val)==len_val for key, val in update_dict.items())
        cond_len = all(len(val)==len_val for key, val in condition_dict.items())
        if not same_len or not cond_len:
            print "The list of updates and/or conditions are of unequal length"
            return False
        
        num_chunk = int(math.ceil(len_val/float(CHUNK_SIZE)))
        for i in range(num_chunk):
            chunk_start = i*CHUNK_SIZE
            chunk_end = min(i*CHUNK_SIZE+CHUNK_SIZE, len_val)
            chunk_update = {}
            for key, val in update_dict.iteritems():
                chunk_update[key] = val[chunk_start:chunk_end]
            chunk_condition = {}
            for key, val in condition_dict.iteritems():
                chunk_condition[key] = val[chunk_start:chunk_end]

            # SET clause formating    
            update_list = []
            for key, val_list in chunk_update.iteritems():
                set_list = []
                for i in range(len(val_list)):
                    val = val_list[i]
                    val = self.format_value(val)
                    condition_list = []
                    for con_key in chunk_condition:
                        con = chunk_condition[con_key][i]
                        con = self.format_value(con)
                        condition_list += ["{} = {}".format(con_key, con)]
                    con_str = " AND ".join(condition_list)
                    set_list += ["WHEN {} THEN {}".format(con_str, val)]
                set_str = """
        """.join(set_list)
                update_list += ["{} = CASE {} ELSE {} END".format(key, set_str, key)]
            update_vals = ",".join(update_list)

            # WHERE clause formatting
            condition_list = []
            for key, val_list in chunk_condition.iteritems():
                val_list = [str(self.format_value(val)) for val in val_list]
                val_str = ",".join(val_list)
                condition_list += ["{} IN ({})".format(key, val_str)]
            update_condition = " AND ".join(condition_list)

            update_str = "UPDATE {} SET {} WHERE {}".format(table, update_vals, update_condition)

            cursor.execute(update_str)
            connection.commit()
        return True


    def format_value(self, value):
        """
            Formats the value for use in SQL statements
        """
        if isinstance(value, str):
            value = "'{}'".format(value)
        elif value is None:
            value = "NULL"
        return value

    def fetch_query(self, query_str, var=None):
        """
            Query the database and return rows

            query_str: string with database query
            var [optional]: values to be inserted into the query string

        """
        cursor = self.get_cursor()

        if var:
            cursor.execute(query_str, var)
        else:
            cursor.execute(query_str)
        rows = cursor.fetchall()

        return rows

    def fetch_query_as_df(self, query_str, index_col, var=None):
        """
            Query the database and return pandas dataframe

            query_str: string with database query
            index_col: name of database column to use as dataframe index
        """
        connection, server = self.get_connection()

        df = pd.read_sql(query_str, connection, index_col=index_col)

        return df