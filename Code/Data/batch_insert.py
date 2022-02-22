import pymysql
import sqlite3
import pandas as pd
import numpy as np
import os


def batch_insert(database_connection, dataframe, destination_table):
    """
    :param database_connection: The object for the database connection.
    :param dataframe: The dataframe to be inserted.
    :param database_name: The database name that holds the table
    :param destination_schema: The schema name that holds the table
    :param destination_table: The table to be inserted into
    :return: bool
    A function that inserts a pandas dataframe into a SQL table. It is paramount to partition the dataframe into
    projections with n = 1,000 since a batch insert SQL statement zenith length is 1,000 tuples. Secondly, an identity
    property exists on a table feature such that it auto-increments, we must disable this property to batch insert;
    however, the primary key constraint holds.
    """

    modulo_divisor = 1000  # The 1,000 tuples batch insert constraint
    # A numpy array that holds the index steps for projecting the dataframe
    partition_slice_nodes = np.arange(len(dataframe.index) + modulo_divisor, step=modulo_divisor)
    for j in range(len(partition_slice_nodes)):
        if j == 0:  # Skip the first iteration
            pass
        else:
            instance_df = dataframe.iloc[partition_slice_nodes[j - 1]:partition_slice_nodes[j]]  # Slice the dataframe
            # Convert the dataframe into numpy arrays. Then, convert the numpy into a list. String the list
            # Replace the leading brackets with parenthesis since that is the input SQL requires.
            staged_insert = str(instance_df.values.tolist()).replace('[', '(')
            # Replace the trailing brackets with parenthesis since that is the input SQL require
            staged_insert = staged_insert.replace(']', ')')
            # Since this is a string: '((x, y, z), (x2, y2, z2), (x3, y3, z3))
            # We don't want the first and last parenthesis; therefore, we slice the string
            staged_insert = staged_insert[1:-1]
            staged_insert = staged_insert.replace('nan', 'NULL')  # Replace all nan values with NULL
            # Insert statement, but with no table features specified.
            # The dataframe should cover the domain of features
            database_connection.cursor().execute(
                'INSERT INTO ' + destination_table +
                ' VALUES ' + staged_insert + ';')
    # Commit the insert
    database_connection.commit()
    return True
