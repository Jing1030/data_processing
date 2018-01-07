import pandas as pd
import numpy as np

from data_processing.edit_db import connect_db, close_db_connection

def fetch_query(query_str, var=None, db_name=None, sql_version="MSSQL", firewall=False, 
                key_loc=None, db_pwd=None, key_pwd=None):
    """
        Query the database and return rows

        query_str: string with database query
        var [optional]: values to be inserted into the query string
        db_name [optional]: name of the database to query
        sql_version [optional]: version of SQL to connect to (MSSQL or MySQL)
        firewall [optional]: if behind a firewall, will use ssh tunneling to connect to database; default: False
        key_loc [optional]: file location of the ssh key for accessing MySQL database; default: None
        db_pwd [optional]: database user password; default: None
        key_pwd [optional]: password for the ssh key; default: None
    """
    connection, server = connect_db(sql_version, db_name=db_name, firewall=firewall, 
                                    key_loc=key_loc, db_pwd=db_pwd, key_pwd=key_pwd)
    cursor = connection.cursor()

    if var:
        cursor.execute(query_str, var)
    else:
        cursor.execute(query_str)
    rows = cursor.fetchall()

    cursor.close()
    close_db_connection(connection, server)

    return rows

def fetch_query_as_df(query_str, index_col, var=None, db_name=None, sql_version="MSSQL", firewall=False, 
                      key_loc=None, db_pwd=None, key_pwd=None):
    """
        Query the database and return pandas dataframe

        query_str: string with database query
        index_col: name of database column to use as dataframe index
        db_name [optional]: name of the database to query
        sql_version [optional]: version of SQL to connect to (MSSQL or MySQL)
        firewall [optional]: if behind a firewall, will use ssh tunneling to connect to database; default: False
        key_loc [optional]: file location of the ssh key for accessing MySQL database; default: None
        db_pwd [optional]: database user password; default: None
        key_pwd [optional]: password for the ssh key; default: None
    """
    connection, server = connect_db(sql_version, db_name=db_name, firewall=firewall, 
                                    key_loc=key_loc, db_pwd=db_pwd, key_pwd=key_pwd)

    df = pd.read_sql(query_str, connection, index_col=index_col)

    close_db_connection(connection, server)
    return df

def fetch_read_count(samp_names, add_read_samps, return_bool_ser=False, 
                     var=None, db_name=None, sql_version="MSSQL", firewall=False, 
                     key_loc=None, db_pwd=None, key_pwd=None):
    """
        Fetches the read counts for each sgRNA per sample as a pandas dataframe

        samp_names: list of samples to fetch the reads for
        add_read_samps: list of samples which were sequenced twice to increase total read count
        return_bool_ser [optional]: return a series with the True for control sgRNAs and
                                    False for experimental
        db_name [optional]: name of the database to query
        sql_version [optional]: version of SQL to connect to (MSSQL or MySQL)
        firewall [optional]: if behind a firewall, will use ssh tunneling to connect to database; default: False
        key_loc [optional]: file location of the ssh key for accessing MySQL database; default: None
        db_pwd [optional]: database user password; default: None
        key_pwd [optional]: password for the ssh key; default: None
    """
    connection, server = connect_db(sql_version, db_name=db_name, firewall=firewall, 
                                    key_loc=key_loc, db_pwd=db_pwd, key_pwd=key_pwd)

    all_samp = samp_names+add_read_samps
    query_str = "SELECT SgID, SgRNAName, {} FROM SgRNAReadCounts".format(",".join(all_samp))
    # get pandas dataframe directly
    df = pd.read_sql(query_str, connection, index_col=["SgID", "SgRNAName"])

    # get rid of multi-level index
    ind = df.index.get_level_values(0)
    ind_name = df.index.get_level_values(1)
    new_ind = []
    bool_val = []
    for i in range(len(ind)):
        if np.isnan(ind[i]):
            new_ind += ["sg"+ind_name[i]]
            bool_val += [True]
        else:
            new_ind += ["sg{}".format(int(ind[i]))]
            bool_val += [False]
    df.index = new_ind
    if return_bool_ser:
        bool_ser = pd.Series(data=bool_val, index=new_ind)

    for col2 in add_read_samps:
        col1 = col2.replace("_add", "", 1)
        df[col1] = df[col1] + df[col2] # Add the columns together

    out_df = df[samp_names]

    close_db_connection(connection, server)

    if return_bool_ser:
        return out_df, bool_ser
    else:
        return out_df
