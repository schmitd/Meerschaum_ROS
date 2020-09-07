#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
This module is the entry point for the interactive shell
"""

import cmd
from meerschaum.config import __doc__
from meerschaum.actions.arguments import parse_arguments
import inspect

class Shell(cmd.Cmd):
    prompt = "mrsm —> "
    intro = __doc__
    debug = False

    def precmd(self, line):
        """
        Pass line string to parent actions.
        Pass parsed arguments to custom actions

        Overrides `default`: if action does not exist,
            assume the action is `bash`
        """
        if line is None or len(line) == 0: return line

        args = parse_arguments(line.split())

        ### if debug is not set on the command line,
        ### default to shell setting
        if not args['debug']: args['debug'] = self.debug

        action = args['action'][0]
        try:
            func = getattr(self, 'do_' + action)
            func_param_kinds = inspect.signature(func).parameters.items()
        except AttributeError as ae:
            ### if function is not found, default to `bash`
            action = "bash"
            args['action'].insert(0, "bash")
            func = getattr(self, 'do_bash')
            func_param_kinds = inspect.signature(func).parameters.items()

        ### delete the first action
        ### e.g. 'show actions' -> ['actions']
        del args['action'][0]
        if len(args['action']) == 0: args['action'] = ['']

        positional_only = True
        for param in func_param_kinds:
            ### if variable keyword arguments found,
            ### use meerschaum parser, else just pass
            ### the line string without parsing
            if str(param[1].kind) == "VAR_KEYWORD":
                positional_only = False
                break
        
        if positional_only: return line
        
        ### execute the meerschaum action
        ### and print the response message in case of failure
        response = func(**args)
        if isinstance(response, tuple) and not response[0]:
            print("\nError message:", response[1])
        return ""

    def default(self, line):
        """
        If an action has not been declared, preprend 'bash' to the line
        and execute in a subshell
        """
        self.do_default(line)

    def do_default(self, action=[''], **kw):
        """
        If `action` is not implemented, execute in a subprocess.
        (preprends 'bash' to the actions)
        """
        pass

    def do_debug(self, action=[''], **kw):
        """
        Toggle the shell's debug mode.
        If debug = on, append `--debug` to all commands.
        """
        on_commands = {'on', 'true'}
        off_commands = {'off', 'false'}
        state = action[0]
        if state == '':
            self.debug = not self.debug
        elif state.lower() in on_commands: self.debug = True
        elif state.lower() in off_commands: self.debug = False
        else: print(f"Unknown state '{state}'. Ignoring...")

        print(f"Debug mode is {'on' if self.debug else 'off'}.")

    def do_exit(self, params):
        """
        Exit the Meerschaum shell
        """
        return True

    def do_quit(self, params):
        """
        Exit the Meerschuam shell
        """
        return True

    def do_EOF(self, line):
        """
        Exit the Meerschaum shell
        """
        return True

    def emptyline(self):
        pass
