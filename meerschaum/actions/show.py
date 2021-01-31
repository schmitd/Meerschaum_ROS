#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
This module contains functions for printing elements.
"""
from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, Union, Sequence, Any, Optional

def show(
        action : Sequence[str] = [],
        **kw : Any
    ) -> SuccessTuple:
    """
    Show elements of a certain type.

    Command:
        `show {option}`

    Example:
        `show pipes`
    """

    from meerschaum.utils.misc import choose_subaction
    show_options = {
        'actions'    : _show_actions,
        'pipes'      : _show_pipes,
        'config'     : _show_config,
        'modules'    : _show_modules,
        'version'    : _show_version,
        'connectors' : _show_connectors,
        'arguments'  : _show_arguments,
        'data'       : _show_data,
        'plugins'    : _show_plugins,
        'help'       : _show_help,
        'users'      : _show_users,
    }
    return choose_subaction(action, show_options, **kw)

def _show_actions(**kw : Any) -> SuccessTuple:
    """
    Show available actions
    """
    from meerschaum.actions import actions
    from meerschaum.utils.misc import print_options
    print_options(options=actions, name='actions', **kw)
    return True, "Success"

def _show_help(**kw : Any) -> SuccessTuple:
    """
    Print the --help menu from argparse
    """
    from meerschaum.actions.arguments._parser import parser
    print(parser.format_help())
    return True, "Success"

def _show_config(
        action : Sequence[str] = [],
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Show the configuration dictionary.
    Sub-actions defined in the action list are index in the config dictionary.

    E.g. `show config pipes` -> cf['pipes']
    """
    from meerschaum.utils.formatting import pprint
    from meerschaum.config import get_config
    from meerschaum.config._paths import CONFIG_PATH
    from meerschaum.utils.debug import dprint
    if debug: dprint(f"Configuration loaded from {CONFIG_PATH}")

    keys = list(action)
    if keys == ['']: keys = []

    pprint(get_config(*keys))
    return (True, "Success")

def _show_modules(**kw : Any) -> SuccessTuple:
    """
    Show the currently imported modules
    """
    import sys
    from meerschaum.utils.formatting import pprint
    pprint(list(sys.modules.keys()))
    return (True, "Success")

def _show_pipes(
        nopretty : bool = False,
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Print a stylized tree of available Meerschaum pipes.
    Respects global ANSI and UNICODE settings.
    """
    from meerschaum import get_pipes
    from meerschaum.utils.misc import flatten_pipes_dict
    pipes = get_pipes(debug=debug, **kw)
    from meerschaum.utils.formatting import ANSI, pprint_pipes

    if len(pipes) == 0:
        return False, "No pipes to show."

    if len(flatten_pipes_dict(pipes)) == 1:
        return flatten_pipes_dict(pipes)[0].show(debug=debug, nopretty=nopretty, **kw)

    if not nopretty:
        pprint_pipes(pipes)
    else:
        pipes_list = flatten_pipes_dict(pipes)
        for p in pipes_list:
            print(p)

    return (True, "Success")

def _show_version(nopretty : bool = False, **kw : Any) -> SuccessTuple:
    """
    Show the Meerschaum doc string.
    """
    from meerschaum import __version__ as version
    _print = print
    if nopretty:
        msg = version
    else:
        from meerschaum.utils.warnings import info
        msg = "Meerschaum v" + version
        _print = info
    _print(msg)
    return (True, "Success")

def _show_connectors(
        action : Sequence[str] = [],
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Show connectors configuration and, if requested, specific connector attributes.

    Examples:
        `show connectors`
            Display the connectors configuration.

        `show connectors sql:main`
            Show the connectors configuration and the attributes for the connector 'sql:main'.
    """
    from meerschaum.connectors import connectors
    from meerschaum.config import config
    from meerschaum.utils.formatting import make_header
    from meerschaum.utils.formatting import pprint
    print(make_header("\nConfigured connectors:"))
    pprint(config['meerschaum']['connectors'])
    print(make_header("\nActive connectors:"))
    pprint(connectors)

    from meerschaum.connectors.parse import parse_instance_keys
    if action != []:
        conn = parse_instance_keys(action[0], debug=debug)
        if conn:
            print(make_header("\n" + f"Attributes for connector '{conn}':"))
            pprint(conn.__dict__)

    return True, "Success"

def _show_arguments(
        **kw : Any
    ) -> SuccessTuple:
    """
    Show the parsed keyword arguments.
    """
    from meerschaum.utils.formatting import pprint
    pprint(kw)
    return True, "Success"

def _show_data(
        action : Sequence[str] = [],
        gui : bool = False,
        begin : Optional[datetime.datetime] = None,
        end : Optional[datetime.datetime] = None,
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Show pipes data as Pandas DataFrames.

    Usage:
        - Use --gui to open an interactive window.

        - `show data all` to grab all data for the chosen Pipes.
          WARNING: This may be dangerous!

        - `show data 60` to grab the last 60 (or any number) minutes of data for all pipes.

        - `show data --begin 2020-01-01 --end 2021-01-01` to specify date rangers.
          NOTE: You must specify to at least the day, otherwise the date parser will assume you mean today's date.

        - Regular pipes parameters (-c, -m, -l, etc.)

    Examples:
        - show data -m weather --gui
            Open an interactive pandasgui window for the last 1440 minutes of data for all pipes of metric 'weather'.
    """
    import sys
    from meerschaum import get_pipes
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.warnings import warn, info
    from meerschaum.utils.formatting import pprint
    pipes = get_pipes(as_list=True, debug=debug, **kw)
    try:
        backtrack_minutes = float(action[0])
    except:
        backtrack_minutes = (
            1440 if (
                begin is None and end is None and (action and action[0] != 'all')
            ) else None
        )

    for p in pipes:
        try:
            if backtrack_minutes is not None:
                df = p.get_backtrack_data(backtrack_minutes=backtrack_minutes, debug=debug)
            else:
                df = p.get_data(begin=begin, end=end, debug=debug)
        except:
            df = None
        if df is None:
            warn(f"Failed to fetch data for pipe '{p}'.", stack=False)
            continue

        info_msg = (
            f"Last {backtrack_minutes} minutes of data for pipe '{p}':"
            if backtrack_minutes is not None else
            (
                f"Data for pipe '{p}'" +
                    (f" from {begin}" if begin is not None else '') +
                    (f" to {end}" if end is not None else '') + ':'
            )
        )

        info(info_msg)
        pprint(df, file=sys.stderr)
        if gui:
            pandasgui = attempt_import('pandasgui')
            try:
                pandasgui.show(df)
            except:
                df.plot()
    return True, "Success"

def _show_plugins(
        action : Sequence[str] = [],
        repository : Optional[str] = None,
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Show the installed plugins.
    """
    from meerschaum.actions.plugins import get_plugins_names
    from meerschaum.utils.misc import print_options
    from meerschaum.connectors.parse import parse_repo_keys
    from meerschaum.utils.warnings import info
    from meerschaum._internal import User
    repo_connector = parse_repo_keys(repository)

    if action == [''] or len(action) == 0:
        _to_print = get_plugins_names()
        header = "Installed plugins:"
        info("To see all installable plugins, run `show plugins all`")
        info("To see plugins created by a certain user, run `show plugins [username]`")
    elif action[0] in ('all'):
        _to_print = repo_connector.get_plugins(debug=debug)
        header = f"Available plugins from Meerschaum repository '{repo_connector}':"
    else:
        username = action[0]
        user_id = repo_connector.get_user_id(User(username, ''))
        _to_print = repo_connector.get_plugins(user_id=user_id, debug=debug)
        header = f"Plugins from user '{username}' at Meerschaum repository '{repo_connector}':"

    print()
    print_options(_to_print, header=header, debug=debug, **kw)
    print()

    return True, "Success"

def _show_users(
        repository : Optional[str] = None,
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Show the registered users in a Meerschaum repository (default is mrsm.io).
    """
    from meerschaum.config import get_config
    from meerschaum.connectors.parse import parse_repo_keys
    from meerschaum.utils.misc import print_options
    try:
        repo_connector = parse_repo_keys(repository)
        users_list = repo_connector.get_users(debug=debug)
    except:
        return False, f"Failed to get users from repository '{repository}'"
    print_options(users_list, header=f"Registered users for repository '{repo_connector}':")
    return True, "Success"

### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
show.__doc__ += _choices_docstring('show')
