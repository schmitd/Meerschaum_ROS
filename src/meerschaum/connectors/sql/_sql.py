#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
This module contains SQLConnector functions for executing SQL queries.
"""

import lazy_import
import sqlalchemy

### database flavors that can use bulk insert
bulk_flavors = {'postgres', 'timescaledb'}

def read(
        self,
        query_or_table : str,
        chunksize=-1,
        debug=False,
        **kw
    ) -> 'pd.DataFrame':
    """
    Read a SQL query or table into a pandas dataframe.
    """
    chunksize = chunksize if chunksize != -1 else self.sys_config['chunksize']
    if debug:
        import time
        start = time.time()
        print(query_or_table)
        print(f"Fetching with chunksize: {chunksize}")
    try:
        chunk_generator = self.pd.read_sql(
                    query_or_table,
                    self.engine,
                    chunksize=chunksize
                )
    except Exception as e:
        print(f"Failed to execute query:\n\n{query_or_table}\n\n")
        print(e)
        return False

    chunk_list = []
    for chunk in chunk_generator:
        chunk_list.append(chunk)

    ### chunksize is not None so must iterate
    if debug:
        end = time.time()
        print(f"Fetched {len(chunk_list)} chunks in {round(end - start, 2)} seconds.")

    return self.pd.concat(chunk_list)

def exec(self, query, debug=False) -> bool:
    """
    Execute SQL code and return success status. e.g. calling stored procedures
    """
    try:
        with self.engine.connect() as connection:
            result = connection.execute(
                sqlalchemy.text(query).execution_options(autocommit=True)
            )
    except Exception as e:
        print(f"Failed to execute query:\n\n{query}\n\n")
        print(e)
        result = False
    return result

def to_sql(
        self,
        df : 'pd.DataFrame',
        name : str = None,
        index : bool = False,
        if_exists : str = 'replace',
        method : str = "",
        chunksize : int = -1,
        debug=False,
        **kw
    ):
    """
    Upload a DataFrame's contents to the SQL server

    df : pandas.DataFrame
        The DataFrame to be uploaded
    name : str
        The name of the table to be created
    index : bool (False)
        If True, creates the DataFrame's indices as columns (default False)
    if_exists : str ('replace')
        ['replace', 'append', 'fail']
        Drop and create the table ('replace') or append if it exists ('append') or raise Exception ('fail')
        (default 'replace')
    method : str
        None or multi. Details 
    **kw : keyword arguments
        Additional arguments will be passed to the DataFrame's `to_sql` function
    """
    if name is None:
        raise Exception("Name must not be None to submit to the SQL server")

    ### resort to defaults if None
    if method == "":
        if self.flavor in bulk_flavors:
            method = psql_insert_copy
        else:
            method = self.sys_config['method']
    chunksize = chunksize if chunksize != -1 else self.sys_config['chunksize']

    #  df_len = len(df)
    #  if df_len > self.sys_config['bulk_insert_threshold'] and self.type in bulk_types:
        #  print(f'DataFrame is {df_len} rows tall. Resorting to bulk insert...')
        #  return self.bulk_insert(df, name=name, index=index, if_exists=if_exists, **kw)

    if debug:
        import time
        start = time.time()
        print(f"Inserting {len(df)} rows with chunksize: {chunksize}...", end="")

    try:
        df.to_sql(
            name=name,
            con=self.engine,
            index=index,
            if_exists=if_exists,
            method=method,
            chunksize=chunksize,
            **kw
        )
    except Exception as e:
        print(f'Failed to commit dataframe with name: {name}')
        print(e)
        import traceback
        traceback.print_exception(type(e), e, e.__traceback__)
        return False

    if debug:
        end = time.time()
        print(f" done.")
        print(f"It took {round(end - start, 2)} seconds.")

    return True

### DEPRECIATED in favor of psql_insert_copy
###
#  def bulk_insert(
        #  self,
        #  df : 'pd.DataFrame',
        #  name : str = None,
        #  #  index : bool = False,
        #  if_exists : str = 'replace',
        #  debug=False,
        #  **kw
    #  ):
    #  """
    #  If possible, upload via copy_from rather than to_sql
    #  """
    
    #  if name is None:
        #  raise Exception("Name must not be None to submit to the SQL server")

    #  if self.flavor not in bulk_flavors:
        #  raise Exception(f"SQLConnector flavor '{self.flavor}' must be one of the following: {bulk_flavors}. Use `to_sql` with a large chunksize instead.")

    #  import io
    #  if debug:
        #  import time
        #  start = time.time()

    #  ### ensure the table exists
    #  df[:0].to_sql(name, self.engine, if_exists=if_exists)

    #  output = io.StringIO()
    #  if debug: print("Parsing DataFrame to stream via to_csv...", end="")
    #  df.to_csv(output, sep=',', header=False, index=False)
    #  if debug: print(" done.")

    #  output.seek(0)
    #  contents = output.getvalue()
    #  connection = self.engine.raw_connection()
    #  cursor = connection.cursor()
    #  if debug: print("Copying to database via copy_from...", end="")
    #  cursor.copy_from(output, name, null="", sep=",")
    #  connection.commit()
    #  cursor.close()
    #  if debug:
        #  end = time.time()
        #  print(" done.")
        #  print("It took {round(end - start, 2)} seconds to upload {len(df)} rows.")
    #  return True

def psql_insert_copy(table, conn, keys, data_iter):
    """
    Execute SQL statement inserting data

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """
    import csv
    from io import StringIO
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)

