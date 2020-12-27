#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Wrappers for requests.post
"""

def patch(
        self,
        r_url : str,
        **kw
    ):
    """
    Wrapper for requests.post
    """
    if 'auth' in kw:
        print('Ignoring auth, using existing configuration')
        del kw['auth']
    return self.session.patch(self.url + r_url, auth=self.auth, **kw)
