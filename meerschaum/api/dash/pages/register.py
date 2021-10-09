#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Create new user accounts.
"""

import dash_bootstrap_components as dbc
from meerschaum.utils.packages import import_html, import_dcc
html, dcc = import_html(), import_dcc()

layout = dbc.Container([
    html.Div([
        html.Br(),
        html.H3("Register your account"),
        dbc.Row(
            [
                dbc.Col([
                    #  dbc.FormGroup([
                        dbc.Label("Username", html_for='username-input'),
                        dbc.Input(id="username-input", type="text", placeholder="Enter username"),
                        dbc.FormFeedback("Username is available.", type="valid"),
                        dbc.FormFeedback("Username is unavailable.", type="invalid"),
                    #  ]),
                    ],
                    width = 6,
                ),
                dbc.Col([
                    #  dbc.FormGroup([
                        dbc.Label("Password", html_for='password-input'),
                        dbc.Input(
                            id="password-input", type="password", value="",
                            placeholder='Enter password'
                        ),
                        dbc.FormFeedback("Password is acceptable.", type="valid"),
                        dbc.FormFeedback("Password is too short.", type="invalid"),
                    ],
                    width = 6,
                ),
            ],
            #  form = True,
        ),
        dbc.Row(
            dbc.Col([
                #  dbc.FormGroup([
                    dbc.Label("Email", html_for="email-input"),
                    dbc.Input(id="email-input", type="email", placeholder="Optional"),
                #  ]),
                dbc.FormFeedback("", type="valid"),
                dbc.FormFeedback("", type="invalid"),
                ],
            ),
            #  form = True,
        )
    ])


])