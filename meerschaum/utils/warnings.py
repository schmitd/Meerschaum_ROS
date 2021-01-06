#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Handle all things warnings and errors here
"""

import warnings

warnings.filterwarnings(
    "always",
    category = UserWarning
)
warnings.filterwarnings(
    "ignore",
    category = DeprecationWarning
)
warnings.filterwarnings(
    "always",
    category = ImportWarning
)
warnings.filterwarnings(
    "ignore",
    category = RuntimeWarning
)

def enable_depreciation_warnings(name):
    import meerschaum.actions
    warnings.filterwarnings(
        "always",
        category = DeprecationWarning,
        module = name
    )

def warn(*args, stacklevel=2, stack=True, **kw):
    """
    Raise a warning with custom Meerschaum formatting
    """
    from meerschaum.utils.formatting import CHARSET, ANSI, colored
    from meerschaum.config import config as cf, get_config
    import sys

    warn_config = get_config('system', 'warnings', patch=True)
    a = list(args)
    a[0] = ' ' + warn_config[CHARSET]['icon'] + ' ' + str(a[0])
    if ANSI:
        a[0] = colored(a[0], *warn_config['ansi']['color'])
    if stacklevel is None or not stack: print(a[0], file=sys.stderr)
    else: return warnings.warn(*a, stacklevel=stacklevel, **kw)

def error(message : str, exception_class = Exception):
    """
    Raise an error with custom Meerschaum formatting
    """
    from meerschaum.utils.formatting import CHARSET, ANSI, colored
    from meerschaum.config import config as cf, get_config
    error_config = get_config('system', 'errors', patch=True)
    message = ' ' + error_config[CHARSET]['icon'] + ' ' + message
    if ANSI:
        message = colored(message, *error_config['ansi']['color'])
    raise exception_class(message)

def info(message : str, **kw):
    """
    Print an informative message
    """
    from meerschaum.utils.formatting import CHARSET, ANSI, colored
    from meerschaum.config import get_config
    import sys
    info_config = get_config('system', 'info', patch=True)
    message = ' ' + info_config[CHARSET]['icon'] + ' ' + message
    if ANSI:
        message = colored(message, *info_config['ansi']['color'])
    print(message, file=sys.stderr)
