#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Interact with Pipes metadata via SQLConnector.
"""
from __future__ import annotations
from meerschaum.utils.typing import (
    Union, Any, Sequence, SuccessTuple, Mapping, Tuple, Dict, Optional, List
)

def register_pipe(
        self,
        pipe: meerschaum.Pipe,
        debug: bool = False,
    ) -> SuccessTuple:
    """
    Register a new pipe.
    A pipe's attributes must be set before registering.
    """
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.sql import json_flavors

    ### ensure pipes table exists
    from meerschaum.connectors.sql.tables import get_tables
    pipes = get_tables(mrsm_instance=self, debug=debug)['pipes']

    if pipe.get_id(debug=debug) is not None:
        return False, f"{pipe} is already registered."

    ### NOTE: if `parameters` is supplied in the Pipe constructor,
    ###       then `pipe.parameters` will exist and not be fetched from the database.

    ### 1. Prioritize the Pipe object's `parameters` first.
    ###    E.g. if the user manually sets the `parameters` property
    ###    or if the Pipe already exists
    ###    (which shouldn't be able to be registered anyway but that's an issue for later).
    parameters = None
    try:
        parameters = pipe.parameters
    except Exception as e:
        if debug:
            dprint(str(e))
        parameters = None

    ### ensure `parameters` is a dictionary
    if parameters is None:
        parameters = {}

    import json
    sqlalchemy = attempt_import('sqlalchemy')
    values = {
        'connector_keys' : pipe.connector_keys,
        'metric_key'     : pipe.metric_key,
        'location_key'   : pipe.location_key,
        'parameters'     : (
            json.dumps(parameters) if self.flavor in ('duckdb',) else parameters
            #  json.dumps(parameters) if self.flavor not in json_flavors else parameters
        ),
    }
    query = sqlalchemy.insert(pipes).values(**values)
    result = self.exec(query, debug=debug)
    if result is None:
        return False, f"Failed to register {pipe}."
    return True, f"Successfully registered {pipe}."

def edit_pipe(
        self,
        pipe : meerschaum.Pipe = None,
        patch: bool = False,
        debug: bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Persist a Pipe's parameters to its database.

    Parameters
    ----------
    pipe: meerschaum.Pipe, default None
        The pipe to be edited.
    patch: bool, default False
        If patch is `True`, update the existing parameters by cascading.
        Otherwise overwrite the parameters (default).
    debug: bool, default False
        Verbosity toggle.
    """

    if pipe.id is None:
        return False, f"{pipe} is not registered and cannot be edited."

    from meerschaum.utils.debug import dprint
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.sql import json_flavors
    if not patch:
        parameters = pipe.parameters
    else:
        from meerschaum import Pipe
        from meerschaum.config._patch import apply_patch_to_config
        original_parameters = Pipe(
            pipe.connector_keys, pipe.metric_key, pipe.location_key,
            mrsm_instance=pipe.instance_keys
        ).parameters
        parameters = apply_patch_to_config(
            original_parameters,
            pipe.parameters
        )

    ### ensure pipes table exists
    from meerschaum.connectors.sql.tables import get_tables
    pipes = get_tables(mrsm_instance=self, debug=debug)['pipes']

    import json
    sqlalchemy = attempt_import('sqlalchemy')

    values = {
        'parameters' : (
            json.dumps(parameters) if self.flavor in ('duckdb',)
            else parameters
        ),
    }
    q = sqlalchemy.update(pipes).values(**values).where(
        pipes.c.pipe_id == pipe.id
    )

    result = self.exec(q, debug=debug)
    message = (
        f"Successfully edited {pipe}."
        if result is not None else f"Failed to edit {pipe}."
    )
    return (result is not None), message


def fetch_pipes_keys(
        self,
        connector_keys: Optional[List[str]] = None,
        metric_keys: Optional[List[str]] = None,
        location_keys: Optional[List[str]] = None,
        tags: Optional[List[str]] = None,
        params: Optional[Dict[str, Any]] = None,
        debug: bool = False
    ) -> Optional[List[Tuple[str, str, Optional[str]]]]:
    """
    Return a list of tuples corresponding to the parameters provided.

    Parameters
    ----------
    connector_keys: Optional[List[str]], default None
        List of connector_keys to search by.

    metric_keys: Optional[List[str]], default None
        List of metric_keys to search by.

    location_keys: Optional[List[str]], default None
        List of location_keys to search by.

    params: Optional[Dict[str, Any]], default None
        Dictionary of additional parameters to search by.
        E.g. `--params pipe_id:1`

    debug: bool, default False
        Verbosity toggle.
    """
    from meerschaum.utils.warnings import error
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.misc import separate_negation_values
    from meerschaum.utils.sql import OMIT_NULLSFIRST_FLAVORS
    from meerschaum.config.static import _static_config
    sqlalchemy = attempt_import('sqlalchemy')
    import json

    if connector_keys is None:
        connector_keys = []
    if metric_keys is None:
        metric_keys = []
    if location_keys is None:
        location_keys = []
    else:
        location_keys = [
            (lk if lk not in ('[None]', 'None', 'null') else None)
                for lk in location_keys
        ]
    if tags is None:
        tags = []

    if params is None:
        params = {}

    ### Add three primary keys to params dictionary
    ###   (separated for convenience of arguments).
    cols = {
        'connector_keys': connector_keys,
        'metric_key': metric_keys,
        'location_key': location_keys,
    }

    ### Make deep copy so we don't mutate this somewhere else.
    parameters = params.copy()
    for col, vals in cols.items():
        ### Allow for IS NULL to be declared as a single-item list ([None]).
        if vals == [None]:
            vals = None
        if vals not in [[], ['*']]:
            parameters[col] = vals
    cols = {k: v for k, v in cols.items() if v != [None]}

    from meerschaum.connectors.sql.tables import get_tables
    pipes = get_tables(mrsm_instance=self, debug=debug)['pipes']

    _params = {}
    for k, v in parameters.items():
        _v = json.dumps(v) if isinstance(v, dict) else v
        _params[k] = _v

    negation_prefix = _static_config()['system']['fetch_pipes_keys']['negation_prefix']
    ### Parse regular params.
    ### If a param begins with '_', negate it instead.
    _where = [
        (
            (pipes.c[key] == val) if not str(val).startswith(negation_prefix)
            else (pipes.c[key] != key)
        ) for key, val in _params.items()
            if not isinstance(val, (list, tuple)) and key in pipes.c
    ]
    q = sqlalchemy.select(
        [pipes.c.connector_keys, pipes.c.metric_key, pipes.c.location_key]
        + ([pipes.c.parameters] if tags else [])
    ).where(sqlalchemy.and_(True, *_where))

    ### Parse IN params and add OR IS NULL if None in list.
    for c, vals in cols.items():
        if not isinstance(vals, (list, tuple)) or not vals or not c in pipes.c:
            continue
        _in_vals, _ex_vals = separate_negation_values(vals)
        ### Include params (positive)
        q = (
            q.where(pipes.c[c].in_(_in_vals)) if None not in _in_vals
            else q.where(sqlalchemy.or_(pipes.c[c].in_(_in_vals), pipes.c[c].is_(None)))
        ) if _in_vals else q
        ### Exclude params (negative)
        q = q.where(pipes.c[c].not_in(_ex_vals)) if _ex_vals else q

    ### Finally, parse tags.
    _in_tags, _ex_tags = separate_negation_values(tags)
    ors = []
    for nt in _in_tags:
        ors.append(
            sqlalchemy.cast(
                pipes.c['parameters'],
                sqlalchemy.String,
            ).like(f'%"tags":%"{nt}"%')
        )
    q = q.where(sqlalchemy.and_(sqlalchemy.or_(*ors).self_group())) if ors else q
    ors = []
    for xt in _ex_tags:
        ors.append(
            sqlalchemy.cast(
                pipes.c['parameters'],
                sqlalchemy.String,
            ).not_like(f'%"tags":%"{xt}"%')
        )
    q = q.where(sqlalchemy.and_(sqlalchemy.or_(*ors).self_group())) if ors else q
    loc_asc = sqlalchemy.asc(pipes.c['location_key'])
    if self.flavor not in OMIT_NULLSFIRST_FLAVORS:
        loc_asc = sqlalchemy.nullsfirst(loc_asc)
    q = q.order_by(
        sqlalchemy.asc(pipes.c['connector_keys']),
        sqlalchemy.asc(pipes.c['metric_key']),
        loc_asc,
    )

    ### execute the query and return a list of tuples
    if debug:
        dprint(q.compile(compile_kwargs={'literal_binds': True}))
    try:
        rows = self.engine.execute(q).fetchall()
    except Exception as e:
        error(str(e))

    _keys = [(row['connector_keys'], row['metric_key'], row['location_key']) for row in rows]
    if not tags:
        return _keys
    ### Make 100% sure that the tags are correct.
    keys = []
    for row in rows:
        ktup = (row['connector_keys'], row['metric_key'], row['location_key'])
        _actual_tags = (
            json.loads(row['parameters']) if isinstance(row['parameters'], str)
            else row['parameters']
        ).get('tags', [])
        for nt in _in_tags:
            if nt in _actual_tags:
                keys.append(ktup)
        for xt in _ex_tags:
            if xt in _actual_tags:
                keys.remove(ktup)
            else:
                keys.append(ktup)
    return keys


def create_indices(
        self,
        pipe: meerschaum.Pipe,
        indices: Optional[List[str]] = None,
        debug: bool = False
    ) -> bool:
    """
    Create a pipe's indices.
    """
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.debug import dprint
    if debug:
        dprint(f"Creating indices for {pipe}...")
    if not pipe.columns:
        warn(f"Unable to create indices for {pipe} without columns.", stack=False)
        return False
    ix_queries = {
        ix: queries
        for ix, queries in self.get_create_index_queries(pipe, debug=debug).items()
        if indices is None or ix in indices
    }
    success = True
    for ix, queries in ix_queries.items():
        ix_success = all(self.exec_queries(queries, debug=debug, silent=True))
        if not ix_success:
            success = False
            if debug:
                dprint(f"Failed to create index on column: {ix}")
    return success


def drop_indices(
        self,
        pipe: meerschaum.Pipe,
        indices: Optional[List[str]] = None,
        debug: bool = False
    ) -> bool:
    """
    Drop a pipe's indices.
    """
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.debug import dprint
    if debug:
        dprint(f"Dropping indices for {pipe}...")
    if not pipe.columns:
        warn(f"Unable to drop indices for {pipe} without columns.", stack=False)
        return False
    ix_queries = {
        ix: queries
        for ix, queries in self.get_drop_index_queries(pipe, debug=debug).items()
        if indices is None or ix in indices
    }
    success = True
    for ix, queries in ix_queries.items():
        ix_success = all(self.exec_queries(queries, debug=debug, silent=True))
        if not ix_success:
            success = False
            if debug:
                dprint(f"Failed to drop index on column: {ix}")
    return success


def get_create_index_queries(
        self,
        pipe: meerschaum.Pipe,
        debug: bool = False,
    ) -> Dict[str, List[str]]:
    """
    Return a dictionary mapping columns to a `CREATE INDEX` or equivalent query.

    Parameters
    ----------
    pipe: meerschaum.Pipe
        The pipe to which the queries will correspond.

    Returns
    -------
    A dictionary of column names mapping to lists of queries.
    """
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.sql import sql_item_name, get_distinct_col_count
    from meerschaum.utils.warnings import warn
    from meerschaum.config import get_config
    index_queries = {}

    indices = pipe.get_indices()

    _datetime = pipe.get_columns('datetime', error=False)
    _datetime_name = sql_item_name(_datetime, self.flavor) if _datetime is not None else None
    _datetime_index_name = (
        sql_item_name(indices['datetime'], self.flavor) if indices.get('datetime', None)
        else None
    )
    _id = pipe.get_columns('id', error=False)
    _id_name = sql_item_name(_id, self.flavor) if _id is not None else None

    _id_index_name = sql_item_name(indices['id'], self.flavor) if indices.get('id') else None
    _pipe_name = sql_item_name(pipe.target, self.flavor)
    _create_space_partition = get_config('system', 'experimental', 'space')

    ### create datetime index
    if _datetime is not None:
        if self.flavor == 'timescaledb':
            _id_count = (
                get_distinct_col_count(_id, f"SELECT {_id_name} FROM {_pipe_name}", self)
                if (_id is not None and _create_space_partition) else None
            )
            dt_query = (
                f"SELECT create_hypertable('{_pipe_name}', " +
                f"'{_datetime}', "
                + (
                    f"'{_id}', {_id_count}, " if (_id is not None and _create_space_partition)
                    else ''
                ) +
                "migrate_data => true);"
            )
        else: ### mssql, sqlite, etc.
            dt_query = (
                f"CREATE INDEX {_datetime_index_name} "
                + f"ON {_pipe_name} ({_datetime_name})"
            )

        index_queries[_datetime] = [dt_query]

    ### create id index
    if _id_name is not None:
        if self.flavor == 'timescaledb':
            ### Already created indices via create_hypertable.
            id_query = (
                None if (_id is not None and _create_space_partition)
                else (
                    f"CREATE INDEX {_id_index_name} ON {_pipe_name} ({_id_name})" if _id is not None
                    else None
                )
            )
            pass
        elif self.flavor == 'citus':
            id_query = [(
                f"CREATE INDEX {_id_index_name} "
                + f"ON {_pipe_name} ({_id_name});"
            ), (
                f"SELECT create_distributed_table('{_pipe_name}', '{_id}');"
            )]
        else: ### mssql, sqlite, etc.
            id_query = f"CREATE INDEX {_id_index_name} ON {_pipe_name} ({_id_name})"

        if id_query is not None:
            index_queries[_id] = [id_query]


    ### Create indices for other label in `pipe.columns`.
    other_indices = {
        ix_key: ix_unquoted
        for ix_key, ix_unquoted in pipe.get_indices().items()
        if ix_key not in ('datetime', 'id')
    }
    for ix_key, ix_unquoted in other_indices.items():
        ix_name = sql_item_name(ix_unquoted, self.flavor)
        col = pipe.columns[ix_key]
        col_name = sql_item_name(col, self.flavor)
        index_queries[col] = [f"CREATE INDEX {ix_name} ON {_pipe_name} ({col_name})"]

    return index_queries


def get_drop_index_queries(
        self,
        pipe: meerschaum.Pipe,
        debug: bool = False,
    ) -> Dict[str, List[str]]:
    """
    Return a dictionary mapping columns to a `DROP INDEX` or equivalent query.

    Parameters
    ----------
    pipe: meerschaum.Pipe
        The pipe to which the queries will correspond.

    Returns
    -------
    A dictionary of column names mapping to lists of queries.
    """
    if not pipe.exists(debug=debug):
        return {}
    from meerschaum.utils.sql import sql_item_name, table_exists, hypertable_queries
    drop_queries = {}
    indices = pipe.get_indices()
    pipe_name = sql_item_name(pipe.target, self.flavor)

    if self.flavor not in hypertable_queries:
        is_hypertable = False
    else:
        is_hypertable_query = hypertable_queries[self.flavor].format(table_name=pipe_name)
        is_hypertable = self.value(is_hypertable_query, silent=True, debug=debug) is not None

    if is_hypertable:
        nuke_queries = []
        temp_table = '_' + pipe.target + '_temp_migration'
        temp_table_name = sql_item_name(temp_table, self.flavor)

        if table_exists(temp_table, self, debug=debug):
            nuke_queries.append(f"DROP TABLE {temp_table_name}")
        nuke_queries += [
            f"SELECT * INTO {temp_table_name} FROM {pipe_name}",
            f"DROP TABLE {pipe_name}",
            f"ALTER TABLE {temp_table_name} RENAME TO {pipe_name}",
        ]
        nuke_ix_keys = ('datetime', 'id')
        nuked = False
        for ix_key in nuke_ix_keys:
            if ix_key in indices and not nuked:
                drop_queries[ix_key] = nuke_queries
                nuked = True

    drop_queries.update({
        ix_key: ["DROP INDEX " + sql_item_name(ix_unquoted, self.flavor)]
        for ix_key, ix_unquoted in indices.items()
        if ix_key not in drop_queries
    })
    return drop_queries


def delete_pipe(
        self,
        pipe: meerschaum.Pipe,
        debug: bool = False,
    ) -> SuccessTuple:
    """
    Delete a Pipe's registration and drop its table.
    """
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.sql import sql_item_name
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')

    ### try dropping first
    drop_tuple = pipe.drop(debug=debug)
    if not drop_tuple[0]:
        return drop_tuple

    if not pipe.id:
        return False, f"{pipe} is not registered."

    ### ensure pipes table exists
    from meerschaum.connectors.sql.tables import get_tables
    pipes = get_tables(mrsm_instance=self, debug=debug)['pipes']

    q = sqlalchemy.delete(pipes).where(pipes.c.pipe_id == pipe.id)
    if not self.exec(q, debug=debug):
        return False, f"Failed to delete registration for '{pipe}'."

    return True, "Success"


def get_backtrack_data(
        self,
        pipe: Optional[meerschaum.Pipe] = None,
        backtrack_minutes: int = 0,
        begin: Optional[datetime.datetime] = None,
        params: Optional[Dict[str, Any]] = None,
        chunksize: Optional[int] = -1,
        debug: bool = False
    ) -> Union[pandas.DataFrame, None]:
    """
    Get the most recent backtrack_minutes' worth of data from a Pipe.

    Parameters
    ----------
    pipe: meerschaum.Pipe:
        The pipe to get data from.

    backtrack_minutes: int, default 0
        How far into the past to look for data.

    begin: Optional[datetime.datetime], default None
        Where to start traversing from. Defaults to `None`, which uses the
        `meerschaum.Pipe.get_sync_time` value.

    params: Optional[Dict[str, Any]], default None
        Additional parameters to filter by.
        See `meerschaum.connectors.sql.build_where`.

    chunksize: Optional[int], default -1
        The size of dataframe chunks to load into memory.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A `pd.DataFrame` of backtracked data.

    """
    from meerschaum.utils.warnings import error, warn
    if pipe is None:
        error("Pipe must be provided.")
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.sql import dateadd_str
    if begin is None:
        begin = pipe.get_sync_time(debug=debug)
    da = dateadd_str(
        flavor = self.flavor,
        datepart = 'minute',
        number = (-1 * backtrack_minutes),
        begin = begin
    )

    from meerschaum.utils.sql import sql_item_name, build_where
    table = sql_item_name(pipe.target, self.flavor)
    if not pipe.columns.get('datetime', None):
        _dt = pipe.guess_datetime()
        dt = sql_item_name(_dt, self.flavor) if _dt else None
        is_guess = True
    else:
        _dt = pipe.get_columns('datetime')
        dt = sql_item_name(_dt, self.flavor)
        is_guess = False

    query = (
        f"SELECT * FROM {table}\n"
        + (build_where(params, self) if params else '')
        + (((" AND " if params else " WHERE ") + f"{dt} >= {da}") if da else "")
    )

    df = self.read(query, chunksize=chunksize, debug=debug)

    if self.flavor == 'sqlite':
        from meerschaum.utils.misc import parse_df_datetimes
        df = parse_df_datetimes(df, debug=debug)

    return df


def get_pipe_data(
        self,
        pipe: Optional[meerschaum.Pipe] = None,
        begin: Union[datetime.datetime, str, None] = None,
        end: Union[datetime.datetime, str, None] = None,
        params: Optional[Dict[str, Any]] = None,
        debug: bool = False,
        **kw: Any
    ) -> Union[pd.DataFrame, None]:
    """
    Access a pipe's data from the SQL instance.

    Parameters
    ----------
    pipe: meerschaum.Pipe:
        The pipe to get data from.

    begin: Optional[datetime.datetime], default None
        If provided, get rows newer than or equal to this value.

    end: Optional[datetime.datetime], default None
        If provided, get rows older than or equal to this value.

    params: Optional[Dict[str, Any]], default None
        Additional parameters to filter by.
        See `meerschaum.connectors.sql.build_where`.

    chunksize: Optional[int], default -1
        The size of dataframe chunks to load into memory.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A `pd.DataFrame` of the pipe's data.

    """
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.sql import sql_item_name, dateadd_str
    from meerschaum.utils.packages import import_pandas
    from meerschaum.utils.warnings import warn
    pd = import_pandas()

    query = f"SELECT * FROM {sql_item_name(pipe.target, self.flavor)}"
    where = ""

    existing_cols = pipe.get_columns_types(debug=debug)

    if not pipe.columns.get('datetime', None):
        _dt = pipe.guess_datetime()
        dt = sql_item_name(_dt, self.flavor) if _dt else None
        is_guess = True
    else:
        _dt = pipe.get_columns('datetime')
        dt = sql_item_name(_dt, self.flavor)
        is_guess = False

    if begin is not None or end is not None:
        if is_guess:
            if _dt is None:
                warn(
                    f"No datetime could be determined for {pipe}."
                    + "\n    Ignoring begin and end...",
                    stack = False,
                )
                begin, end = None, None
            else:
                warn(
                    f"A datetime wasn't specified for {pipe}.\n"
                    + f"    Using column \"{_dt}\" for datetime bounds...",
                    stack = False,
                )


    if begin is not None and dt in existing_cols:
        begin_da = dateadd_str(
            flavor = self.flavor,
            datepart = 'minute',
            number = 0,
            begin = begin
        )
        where += f"{dt} >= {begin_da}" + (" AND " if end is not None else "")

    if end is not None and dt in existing_cols:
        end_da = dateadd_str(
            flavor = self.flavor,
            datepart = 'minute',
            number = 0,
            begin = end
        )
        where += f"{dt} < {end_da}"

    if params is not None:
        from meerschaum.utils.sql import build_where
        where += build_where(params, self).replace(
            'WHERE', ('AND' if (begin is not None or end is not None) else "")
        )

    if len(where) > 0:
        query += "\nWHERE " + where

    if _dt and dt in existing_cols:
        query += "\nORDER BY " + dt + " DESC"

    if debug:
        dprint(f"Getting pipe data with begin = '{begin}' and end = '{end}'")

    existing_cols = pipe.get_columns_types(debug=debug)
    dtypes = pipe.dtypes
    if dtypes:
        if self.flavor == 'sqlite':
            if _dt and 'datetime' not in dtypes.get(_dt, 'object'):
                dtypes[_dt] = 'datetime64[ns]'
    if existing_cols:
        dtypes = {col: typ for col, typ in dtypes.items() if col in existing_cols}
    if dtypes:
        kw['dtypes'] = dtypes

    df = self.read(
        query,
        debug = debug,
        **kw
    )
    if self.flavor == 'sqlite':
        from meerschaum.utils.misc import parse_df_datetimes
        ### NOTE: We have to consume the iterator here to ensure that datatimes are parsed correctly
        df = parse_df_datetimes(df, debug=debug) if isinstance(df, pd.DataFrame) else (
            [parse_df_datetimes(c, debug=debug) for c in df]
        )
    return df


def get_pipe_id(
        self,
        pipe: meerschaum.Pipe,
        debug: bool = False,
    ) -> int:
    """
    Get a Pipe's ID from the pipes table.
    """
    from meerschaum.utils.packages import attempt_import
    import json
    sqlalchemy = attempt_import('sqlalchemy')
    from meerschaum.connectors.sql.tables import get_tables
    pipes = get_tables(mrsm_instance=self, debug=debug)['pipes']

    query = sqlalchemy.select([pipes.c.pipe_id]).where(
        pipes.c.connector_keys == pipe.connector_keys
    ).where(
        pipes.c.metric_key == pipe.metric_key
    ).where(
        (pipes.c.location_key == pipe.location_key) if pipe.location_key is not None
        else pipes.c.location_key.is_(None)
    )
    _id = self.value(query, debug=debug)
    if _id is not None:
        _id = int(_id)
    return _id


def get_pipe_attributes(
        self,
        pipe: meerschaum.Pipe,
        debug: bool = False,
    ) -> Dict[str, Any]:
    """
    Get a Pipe's attributes dictionary.
    """
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.debug import dprint
    from meerschaum.connectors.sql.tables import get_tables
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')
    pipes = get_tables(mrsm_instance=self, debug=debug)['pipes']

    if pipe.get_id(debug=debug) is None:
        return {}

    try:
        q = sqlalchemy.select([pipes]).where(pipes.c.pipe_id == pipe.id)
        if debug:
            dprint(q)
        attributes = (
            dict(self.exec(q, silent=True, debug=debug).first())
            if self.flavor != 'duckdb'
            else self.read(q, debug=debug).to_dict(orient='records')[0]
        )
    except Exception as e:
        import traceback
        traceback.print_exc()
        warn(e)
        print(pipe)
        return {}

    ### handle non-PostgreSQL databases (text vs JSON)
    if not isinstance(attributes.get('parameters', None), dict):
        try:
            import json
            parameters = json.loads(attributes['parameters'])
            if isinstance(parameters, str) and parameters[0] == '{':
                parameters = json.loads(parameters)
            attributes['parameters'] = parameters
        except Exception as e:
            attributes['parameters'] = {}

    return attributes


def sync_pipe(
        self,
        pipe: meerschaum.Pipe,
        df: Union[pandas.DataFrame, str, Dict[Any, Any], None] = None,
        begin: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        chunksize: Optional[int] = -1,
        check_existing: bool = True,
        blocking: bool = True,
        debug: bool = False,
        **kw: Any
    ) -> SuccessTuple:
    """
    Sync a pipe using a database connection.

    Parameters
    ----------
    pipe: meerschaum.Pipe
        The Meerschaum Pipe instance into which to sync the data.

    df: Union[pandas.DataFrame, str, Dict[Any, Any], List[Dict[str, Any]]]
        An optional DataFrame or equivalent to sync into the pipe.
        Defaults to `None`.

    begin: Optional[datetime.datetime], default None
        Optionally specify the earliest datetime to search for data.
        Defaults to `None`.

    end: Optional[datetime.datetime], default None
        Optionally specify the latest datetime to search for data.
        Defaults to `None`.

    chunksize: Optional[int], default -1
        Specify the number of rows to sync per chunk.
        If `-1`, resort to system configuration (default is `900`).
        A `chunksize` of `None` will sync all rows in one transaction.
        Defaults to `-1`.

    check_existing: bool, default True
        If `True`, pull and diff with existing data from the pipe. Defaults to `True`.

    blocking: bool, default True
        If `True`, wait for sync to finish and return its result, otherwise asyncronously sync.
        Defaults to `True`.

    debug: bool, default False
        Verbosity toggle. Defaults to False.

    kw: Any
        Catch-all for keyword arguments.

    Returns
    -------
    A `SuccessTuple` of success (`bool`) and message (`str`).
    """
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.packages import import_pandas
    from meerschaum.utils.misc import parse_df_datetimes
    from meerschaum.utils.sql import get_update_queries, sql_item_name
    from meerschaum import Pipe
    import time
    if df is None:
        msg = f"DataFrame is None. Cannot sync {pipe}."
        warn(msg)
        return False, msg

    start = time.perf_counter()

    ## allow syncing for JSON or dict as well as DataFrame
    pd = import_pandas()
    if isinstance(df, dict):
        df = parse_df_datetimes(df, debug=debug)
    elif isinstance(df, str):
        df = parse_df_datetimes(pd.read_json(df, debug=debug))

    ### if Pipe is not registered
    if not pipe.get_id(debug=debug):
        register_tuple = pipe.register(debug=debug)
        if not register_tuple[0]:
            return register_tuple

    ### quit here if implicitly syncing MQTT pipes.
    ### (pipe.sync() is called in the callback of the MQTTConnector.fetch() method)
    if df is None and pipe.connector.type == 'mqtt':
        return True, "Success"

    ### df is the dataframe returned from the remote source
    ### via the connector
    if debug:
        dprint("Fetched data:\n" + str(df))

    if not isinstance(df, pd.DataFrame):
        df = pipe.enforce_dtypes(df, debug=debug)

    ### if table does not exist, create it with indices
    is_new = False
    add_cols_query = None
    if not pipe.exists(debug=debug):
        check_existing = False
        is_new = True
    else:
        ### Check for new columns.
        add_cols_queries = self.get_add_columns_queries(pipe, df, debug=debug)
        if add_cols_queries:
            if not self.exec_queries(add_cols_queries, debug=debug):
                warn(f"Failed to add new columns to {pipe}.")

    unseen_df, update_df, delta_df = (
        pipe.filter_existing(
            df, chunksize=chunksize, begin=begin, end=end, debug=debug, **kw
        ) if check_existing else (df, None, df)
    )
    if debug:
        dprint("Delta data:\n" + str(delta_df))
        dprint("Unseen data:\n" + str(unseen_df))
        if update_df is not None:
            dprint("Update data:\n" + str(update_df))

    if update_df is not None and not update_df.empty:
        temp_target = '_' + pipe.target
        self.to_sql(
            update_df,
            name = temp_target,
            if_exists = 'append',
            chunksize = chunksize,
            debug = debug,
            **kw
        )
        temp_pipe = Pipe(
            pipe.connector_keys + '_', pipe.metric_key, pipe.location_key,
            instance = pipe.instance_keys,
            columns = pipe.columns,
            target = temp_target,
        )

        join_cols = [col for col_key, col in pipe.columns.items() if col_key != 'value']

        queries = get_update_queries(
            pipe.target,
            temp_target,
            self,
            join_cols,
            debug = debug
        )
        success = all(self.exec_queries(queries, break_on_error=True, debug=debug))
        if not success:
            return False, f"Failed to apply update to {pipe}."
        temp_pipe.drop(debug=debug)

    if_exists = kw.get('if_exists', 'append')
    if 'if_exists' in kw:
        kw.pop('if_exists')
    if 'name' in kw:
        kw.pop('name')

    ### Insert new data into Pipe's table.
    stats = self.to_sql(
        unseen_df,
        name = pipe.target,
        if_exists = if_exists,
        debug = debug,
        as_dict = True,
        chunksize = chunksize,
        **kw
    )
    if is_new:
        if not self.create_indices(pipe, debug=debug):
            if debug:
                dprint(f"Failed to create indices for {pipe}. Continuing...")

    end = time.perf_counter()
    success = stats['success']
    if not success:
        return success, stats['msg']
    msg = (
        f"Inserted {len(unseen_df)}, "
        + f"updated {len(update_df) if update_df is not None else 0} rows."
    )
    if debug:
        msg = msg[:-1] + (
            f"\non table {sql_item_name(pipe.target, self.flavor)}\n"
            + f"in {round(end-start, 2)} seconds."
        )
    return success, msg


def get_sync_time(
        self,
        pipe: 'meerschaum.Pipe',
        params: Optional[Dict[str, Any]] = None,
        newest: bool = True,
        round_down: bool = True,
        debug: bool = False,
    ) -> 'datetime.datetime':
    """Get a Pipe's most recent datetime.

    Parameters
    ----------
    pipe: meerschaum.Pipe
        The pipe to get the sync time for.

    params: Optional[Dict[str, Any]], default None
        Optional params dictionary to build the `WHERE` clause.
        See `meerschaum.utils.sql.build_where`.

    newest: bool, default True
        If `True`, get the most recent datetime (honoring `params`).
        If `False`, get the oldest datetime (ASC instead of DESC).

    round_down: bool, default True
        If `True`, round the resulting datetime value down to the nearest minute.
        Defaults to `True`.

    Returns
    -------
    A `datetime.datetime` object if the pipe exists, otherwise `None`.
    """
    from meerschaum.utils.sql import sql_item_name, build_where
    from meerschaum.utils.warnings import warn
    import datetime
    table = sql_item_name(pipe.target, self.flavor)

    if not pipe.columns.get('datetime', None):
        _dt = pipe.guess_datetime()
        dt = sql_item_name(_dt, self.flavor) if _dt else None
        is_guess = True
    else:
        _dt = pipe.get_columns('datetime')
        dt = sql_item_name(_dt, self.flavor)
        is_guess = False

    if _dt is None:
        return None

    ASC_or_DESC = "DESC" if newest else "ASC"
    where = "" if params is None else build_where(params, self)
    q = f"SELECT {dt}\nFROM {table}{where}\nORDER BY {dt} {ASC_or_DESC}\nLIMIT 1"
    if self.flavor == 'mssql':
        q = f"SELECT TOP 1 {dt}\nFROM {table}{where}\nORDER BY {dt} {ASC_or_DESC}"
    elif self.flavor == 'oracle':
        q = (
            "SELECT * FROM (\n"
            + f"    SELECT {dt}\nFROM {table}{where}\n    ORDER BY {dt} {ASC_or_DESC}\n"
            + ") WHERE ROWNUM = 1"
        )

    try:
        from meerschaum.utils.misc import round_time
        import datetime
        db_time = self.value(q, silent=True, debug=debug)

        ### No datetime could be found.
        if db_time is None:
            return None
        ### sqlite returns str.
        if isinstance(db_time, str):
            from meerschaum.utils.packages import attempt_import
            dateutil_parser = attempt_import('dateutil.parser')
            st = dateutil_parser.parse(db_time)
        ### Do nothing if a datetime object is returned.
        elif isinstance(db_time, datetime.datetime):
            if hasattr(db_time, 'to_pydatetime'):
                st = db_time.to_pydatetime()
            else:
                st = db_time
        ### Sometimes the datetime is actually a date.
        elif isinstance(db_time, datetime.date):
            st = datetime.datetime.combine(db_time, datetime.datetime.min.time())
        ### Convert pandas timestamp to Python datetime.
        else:
            st = db_time.to_pydatetime()

        ### round down to smooth timestamp
        sync_time = (
            round_time(st, date_delta=datetime.timedelta(minutes=1), to='down')
            if round_down else st
        )

    except Exception as e:
        sync_time = None
        warn(str(e))

    return sync_time

def pipe_exists(
        self,
        pipe : meerschaum.Pipe,
        debug : bool = False
    ) -> bool:
    """
    Check that a Pipe's table exists.

    Parameters
    ----------
    pipe: meerschaum.Pipe:
        The pipe to check.
        
    debug: bool:, default False
        Verbosity toggle.

    Returns
    -------
    A `bool` corresponding to whether a pipe's table exists.

    """
    from meerschaum.utils.sql import table_exists
    exists = table_exists(pipe.target, self, debug=debug)
    if debug:
        from meerschaum.utils.debug import dprint
        dprint(f"{pipe} " + ('exists.' if exists else 'does not exist.'))
    return exists


def get_pipe_rowcount(
        self,
        pipe: meerschaum.Pipe,
        begin: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        remote: bool = False,
        params: Optional[Dict[str, Any]] = None,
        debug: bool = False
    ) -> int:
    """
    Get the rowcount for a pipe in accordance with given parameters.

    Parameters
    ----------
    pipe: meerschaum.Pipe
        The pipe to query with.
        
    begin: Optional[datetime.datetime], default None
        The beginning datetime value.

    end: Optional[datetime.datetime], default None
        The beginning datetime value.

    remote: bool, default False
        If `True`, get the rowcount for the remote table.

    params: Optional[Dict[str, Any]], default None
        See `meerschaum.utils.sql.build_where`.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    An `int` for the number of rows if the `pipe` exists, otherwise `None`.

    """
    from meerschaum.utils.sql import dateadd_str, sql_item_name
    from meerschaum.utils.warnings import error, warn
    from meerschaum.connectors.sql._fetch import get_pipe_query
    if remote:
        msg = f"'fetch:definition' must be an attribute of {pipe} to get a remote rowcount."
        if 'fetch' not in pipe.parameters:
            error(msg)
            return None
        if 'definition' not in pipe.parameters['fetch']:
            error(msg)
            return None

    _pipe_name = sql_item_name(pipe.target, self.flavor)

    if not pipe.columns.get('datetime', None):
        _dt = pipe.guess_datetime()
        dt = sql_item_name(_dt, self.flavor) if _dt else None
        is_guess = True
    else:
        _dt = pipe.get_columns('datetime')
        dt = sql_item_name(_dt, self.flavor)
        is_guess = False

    if begin is not None or end is not None:
        if is_guess:
            if _dt is None:
                warn(
                    f"No datetime could be determined for {pipe}."
                    + "\n    Ignoring begin and end...",
                    stack = False,
                )
                begin, end = None, None
            else:
                warn(
                    f"A datetime wasn't specified for {pipe}.\n"
                    + f"    Using column \"{_dt}\" for datetime bounds...",
                    stack = False,
                )


    _datetime_name = sql_item_name(
        _dt,
        pipe.instance_connector.flavor if not remote else pipe.connector.flavor
    )
    _cols_names = [
        sql_item_name(col, pipe.instance_connector.flavor if not remote else pipe.connector.flavor)
        for col in set(
            ([_dt] if _dt else [])
            + ([] if params is None else list(params.keys()))
        )
    ]
    if not _cols_names:
        _cols_names = ['*']

    src = (
        f"SELECT {', '.join(_cols_names)} FROM {_pipe_name}"
        if not remote else get_pipe_query(pipe)
    )
    query = f"""
    WITH src AS ({src})
    SELECT COUNT(*)
    FROM src
    """
    if begin is not None or end is not None:
        query += "WHERE"
    if begin is not None:
        query += f"""
        {dt} >= {dateadd_str(self.flavor, datepart='minute', number=0, begin=begin)}
        """
    if end is not None and begin is not None:
        query += "AND"
    if end is not None:
        query += f"""
        {dt} < {dateadd_str(self.flavor, datepart='minute', number=0, begin=end)}
        """
    if params is not None:
        from meerschaum.utils.sql import build_where
        query += build_where(params, self).replace('WHERE', (
            'AND' if (begin is not None or end is not None)
                else 'WHERE'
            )
        )
        
    result = self.value(query, debug=debug)
    try:
        return int(result)
    except Exception as e:
        return None


def drop_pipe(
        self,
        pipe: meerschaum.Pipe,
        debug: bool = False,
        **kw
    ) -> SuccessTuple:
    """
    Drop a pipe's tables but maintain its registration.

    Parameters
    ----------
    pipe: meerschaum.Pipe
        The pipe to drop.
        
    """
    from meerschaum.utils.sql import table_exists, sql_item_name
    success = True
    target, temp_target = pipe.target, '_' + pipe.target
    target_name, temp_name = (
        sql_item_name(target, self.flavor),
        sql_item_name(temp_target, self.flavor),
    )
    if table_exists(target, self, debug=debug):
        success = self.exec(f"DROP TABLE {target_name}", silent=True, debug=debug) is not None
    if table_exists(temp_target, self, debug=debug):
        success = (
            success
            and self.exec(f"DROP TABLE {temp_name}", silent=True, debug=debug) is not None
        )

    msg = "Success" if success else f"Failed to drop {pipe}."
    return success, msg


def clear_pipe(
        self,
        pipe: meerschaum.Pipe,
        begin: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        params: Optional[Dict[str, Any]] = None,
        debug: bool = False,
        **kw
    ) -> SuccessTuple:
    """
    Delete a pipe's data within a bounded or unbounded interval without dropping the table.

    Parameters
    ----------
    pipe: meerschaum.Pipe
        The pipe to clear.
        
    begin: Optional[datetime.datetime], default None
        Beginning datetime. Inclusive.

    end: Optional[datetime.datetime], default None
         Ending datetime. Exclusive.

    params: Optional[Dict[str, Any]], default None
         See `meerschaum.utils.sql.build_where`.

    """
    if not pipe.exists(debug=debug):
        return True, f"{pipe} does not exist, so nothing was cleared."

    from meerschaum.utils.sql import sql_item_name, build_where, dateadd_str
    from meerschaum.utils.warnings import warn
    pipe_name = sql_item_name(pipe.target, self.flavor)

    if not pipe.columns.get('datetime', None):
        _dt = pipe.guess_datetime()
        dt_name = sql_item_name(_dt, self.flavor) if _dt else None
        is_guess = True
    else:
        _dt = pipe.get_columns('datetime')
        dt_name = sql_item_name(_dt, self.flavor)
        is_guess = False

    if begin is not None or end is not None:
        if is_guess:
            if _dt is None:
                warn(
                    f"No datetime could be determined for {pipe}."
                    + "\n    Ignoring datetime bounds...",
                    stack = False,
                )
                begin, end = None, None
            else:
                warn(
                    f"A datetime wasn't specified for {pipe}.\n"
                    + f"    Using column \"{_dt}\" for datetime bounds...",
                    stack = False,
                )


    clear_query = (
        f"DELETE FROM {pipe_name}\nWHERE 1 = 1\n"
        + ('  AND ' + build_where(params, self, with_where=False) if params is not None else '')
        + (
            f'  AND {dt_name} >= ' + dateadd_str(self.flavor, 'day', 0, begin)
            if begin is not None else ''
        ) + (
            f'  AND {dt_name} < ' + dateadd_str(self.flavor, 'day', 0, end)
            if end is not None else ''
        )
    )
    success = self.exec(clear_query, silent=True, debug=debug) is not None
    msg = "Success" if success else f"Failed to clear {pipe}."
    return success, msg


def get_pipe_table(
        self,
        pipe: meerschaum.Pipe,
        debug: bool = False,
    ) -> sqlalchemy.Table:
    """
    Return the `sqlalchemy.Table` object for a `meerschaum.Pipe`.

    Parameters
    ----------
    pipe: meerschaum.Pipe:
        The pipe in question.
        

    Returns
    -------
    A `sqlalchemy.Table` object. 

    """
    from meerschaum.utils.sql import get_sqlalchemy_table
    return get_sqlalchemy_table(pipe.target, connector=self, debug=debug, refresh=True)


def get_pipe_columns_types(
        self,
        pipe: meerschaum.Pipe,
        debug: bool = False,
    ) -> Optional[Dict[str, str]]:
    """
    Get the pipe's columns and types.

    Parameters
    ----------
    pipe: meerschaum.Pipe:
        The pipe to get the columns for.
        
    Returns
    -------
    A dictionary of columns names (`str`) and types (`str`).

    Examples
    --------
    >>> conn.get_pipe_columns_types(pipe)
    {
      'dt': 'TIMESTAMP WITHOUT TIMEZONE',
      'id': 'BIGINT',
      'val': 'DOUBLE PRECISION',
    }
    >>> 
    """
    table_columns = {}
    try:
        pipe_table = self.get_pipe_table(pipe, debug=debug)
        for col in pipe_table.columns:
            table_columns[str(col.name)] = str(col.type)
    except Exception as e:
        import traceback
        traceback.print_exc()
        from meerschaum.utils.warnings import warn
        warn(e)
        table_columns = None

    return table_columns


def get_add_columns_queries(
        self,
        pipe: mrsm.Pipe,
        df: pd.DataFrame,
        debug: bool = False,
    ) -> List[str]:
    """
    Add new null columns of the correct type to a table from a dataframe.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe to be altered.

    df: pd.DataFrame
        The pandas DataFrame which contains new columns.

    connector: mrsm.connectors.SQLConnector
        The connector to the database on which the table resides.

    Returns
    -------
    A list of the `ALTER TABLE` SQL query or queries to be executed on the provided connector.
    """
    if not pipe.exists(debug=debug):
        return []
    from meerschaum.utils.sql import get_pd_type, get_db_type, sql_item_name
    from meerschaum.utils.misc import flatten_list
    table_obj = self.get_pipe_table(pipe, debug=debug)
    df_cols_types = {col: str(typ) for col, typ in df.dtypes.items()}
    db_cols_types = {col: get_pd_type(str(typ)) for col, typ in table_obj.columns.items()}
    new_cols = set(df_cols_types) - set(db_cols_types)
    if not new_cols:
        return []

    new_cols_types = {
        col: get_db_type(
            df_cols_types[col],
            self.flavor
        ) for col in new_cols
    }

    query = "ALTER TABLE " + sql_item_name(pipe.target, self.flavor)
    for col, typ in new_cols_types.items():
        query += "\nADD " + sql_item_name(col, self.flavor) + " " + typ + ","
    query = query[:-1]
    if self.flavor != 'duckdb':
        return [query]

    drop_index_queries = list(flatten_list(
        [q for ix, q in self.get_drop_index_queries(pipe, debug=debug).items()]
    ))
    create_index_queries = list(flatten_list(
        [q for ix, q in self.get_create_index_queries(pipe, debug=debug).items()]
    ))

    return drop_index_queries + [query] + create_index_queries
