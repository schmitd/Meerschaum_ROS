#! /usr/bin/env python
# PYTHON_ARGCOMPLETE_OK
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
mrsm CLI entrypoint
"""

def main():
    import sys
    from meerschaum.actions import entry, get_shell
    from meerschaum.utils.formatting import print_tuple
    sysargs = sys.argv[1:]
    if len(sysargs) == 0: return get_shell().cmdloop()
    return_tuple = entry(sysargs)
    if isinstance(return_tuple, tuple):
        print_tuple(return_tuple)

    ### final step: close global pools
    from meerschaum.utils.pool import get_pools
    for class_name, pool in get_pools().items():
        pool.close()
        pool.join()

if __name__ == "__main__":
    main()
