from typing import Optional

import yaml
import psycopg2 as pg
import pandas as pd
import re

pd.set_option('display.expand_frame_repr', False)


class GenericDBOperations:
    """
    GenericDBOperations is responsible for generic DB operations that are used by various categories of endpoint db operations.
    """

    def __init__(self, path=None):
        self.db_config = None
        self.connection = None

        self._init_db_config(path=path)

    def _init_db_config(self, path=None):
        if not path:
            path = './config/db_config.yml'
        with open(path) as config_stream:
            self.db_config = yaml.full_load(config_stream)

    def _connect_to_db(self):
        db_credentials = self.db_config['credentials']
        if self.connection is None or self.connection.closed:
            try:
                self.connection = pg.connect(**db_credentials)
            except Exception as e:
                print('Connection to database not successful. Halting...')
                raise e

    def _close_connection(self):
        if self.connection is not None and not self.connection.closed:
            self.connection.close()

    @staticmethod
    def check_db_result_sanity(db_res: list) -> bool:
        """
        Provided with the result of query execution, this method checks whether it's valid and contains data.
        :param db_res: A list containing the result of query execution.
        :return: A boolean demonstrating the sanity of the query execution result.
        """
        return not db_res or len(db_res) == 0

    @staticmethod
    def _check_df_sanity(df: pd.DataFrame) -> bool:
        """
        Given a Pandas DF, this method checks whether it is a valid instance of Pandas.DataFrame() with actual data in it or not.
        :param df: An instance of Pandas DF to check its sanity.
        :return: A boolean demonstrating the sanity of the given Pandas DF.
        """
        if isinstance(df, pd.DataFrame):
            return df.empty
        return not df

    def get_columns_of_table(self, table_name: str) -> list:
        """
        Given a table name, this method extracts the column names of the table.
        :param table_name: The name of the table.
        :return: A list of the columns in the table.
        """
        query = f"""
        SELECT *
        FROM {table_name}
        LIMIT 0;
        """
        if self.connection is None or self.connection.closed:
            self._connect_to_db()
        try:
            cur = self.connection.cursor()
            cur.execute(query)
            columns = [desc[0] for desc in cur.description]
            cur.close()
        except Exception as e:
            print('Query execution error:\n{}\n'.format(query))
            raise e

        return columns

    def execute_query(self, query: str, fetch_one=False, fetch_all=False, row_count=False):
        """
        Provided with a raw SQL query, whether to fetch one or all rows of the result of query execution and whether to fetch
        the number of affected rows of the query execution, this method facilitates the execution of a raw query.
        :param query: The raw SQL query.
        :param fetch_one: Whether or not to fetch the last result of query execution
        :param fetch_all: Whether or not to fetch all of the query execution results
        :param row_count: Whether or not to fetch the number of affected rows
        :return: A two-tuple (if asked for affected rows) of the result of query and affected rows, or simply the result of the
        query (if not asked for affected rows)
        """
        rows = None
        affected_rows = 0
        if self.connection is None or self.connection.closed:
            self._connect_to_db()
        try:
            cur = self.connection.cursor()
            cur.execute(query)

            affected_rows = cur.rowcount

            if cur.rowcount == 0:
                rows = None
            elif fetch_one:
                rows = [cur.fetchone()]
            elif fetch_all:
                rows = cur.fetchall()

            cur.close()
            self.connection.commit()
        except Exception as e:
            print('Query execution error:\n{}\n'.format(query))
            print(e)
            self._close_connection()
            raise e

        if row_count:
            return rows, affected_rows
        else:
            return rows

    def insert_into_table(self, table_name: str, data: pd.DataFrame, fetch_one=False, fetch_all=False, id_col=''):
        """
        Provided with the table name, the data to insert in the format of a Pandas DF, and whether to fetch one or all of the ids
        of the inserted rows, this method facilitates insertion into tables.
        :param table_name: The table to insert.
        :param data: The data to insert (Pandas DF).
        :param fetch_one: Whether or not to fetch the id of the last row inserted
        :param fetch_all: Whether or not to fetch the id of the all inserted rows
        :param id_col: The name of the id_col.
        :return: A list of fetched info (or None if not asked for).
        """
        if data.empty:
            return None

        df_cols = data.columns.tolist()
        cols = '({})'.format(', '.join(df_cols)).lower()
        values = ''
        for r in data.itertuples(index=False):
            r2 = []
            for item in r:
                if item and isinstance(item, str):
                    item = re.sub(' +', ' ', item.replace('\'', ''))
                r2.append(item)
            values += '({}),\n'.format(', '.join(f"'{item}'" for item in r2))
        values = values.strip()[:-1]
        if fetch_one or fetch_all:
            q = f'''
            INSERT INTO {table_name} {cols} VALUES
            {values} RETURNING {id_col}
            ON CONFLICT ({id_col}) DO NOTHING;
            '''
        else:
            q = f'''
            INSERT INTO {table_name} {cols} VALUES
            {values}
            ON CONFLICT ({id_col}) DO NOTHING;
            '''
        rows = self.execute_query(q, fetch_one=fetch_one, fetch_all=fetch_all)
        return rows

    def lookup_table(self, table: str, fetch_cols: list, lookup_cols: list, lookup_values: list, fetch_one=False,
                     fetch_all=False) -> list:
        """
        Given the table name, columns to fetch, columns to lookup and their values, and whether to fetch all results or only one,
        this generic method is used to lookup a table based on some values and extracting records.
        :param table: The table to lookup.
        :param fetch_cols: The columns to fetch.
        :param lookup_cols: The columns to lookup.
        :param lookup_values: The values to lookup.
        :param fetch_one: Whether or not to fetch only one record.
        :param fetch_all: Whether or not to fetch all records.
        :return: A list of extracted rows.
        """
        cols = ', '.join(fetch_cols)
        condition = ' AND '.join(
            ["{} = '{}'".format(lookup_cols[i], lookup_values[i]) for i in range(len(lookup_cols))])

        q = f'''
        SELECT {cols} FROM {table} WHERE {condition};
        '''
        rows = self.execute_query(q, fetch_one=fetch_one, fetch_all=fetch_all)
        return rows

    def lookup_table_col_in(self, table: str, fetch_cols: list, lookup_col: str, lookup_values: list, fetch_one=False,
                            fetch_all=False) -> list:
        """
        Given the table name, columns to fetch, columns to lookup and their values, and whether to fetch all results or only one,
        this generic method is used to lookup a table based on some values and extracting records.
        :param table: The table to lookup.
        :param fetch_cols: The columns to fetch.
        :param lookup_col: The single column to lookup.
        :param lookup_values: The values of the single column to lookup.
        :param fetch_one: Whether or not to fetch only one record.
        :param fetch_all: Whether or not to fetch all records.
        :return: A list of extracted rows.
        """
        cols = ', '.join(fetch_cols)
        lookups = "({})".format(', '.join(f"'{i}'" for i in lookup_values))
        q = f"""
            SELECT {cols}
            FROM {table}
            WHERE {lookup_col} in {lookups};
            """
        rows = self.execute_query(q, fetch_one=fetch_one, fetch_all=fetch_all)
        return rows
