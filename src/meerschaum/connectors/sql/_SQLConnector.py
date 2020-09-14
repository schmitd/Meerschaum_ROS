#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Interface with SQL servers using sqlalchemy
"""

from meerschaum.connectors import Connector

class SQLConnector(Connector):
    """
    Create and utilize sqlalchemy engines
    """
    from ._create_engine import flavor_configs, create_engine
    from ._sql import read, exec, to_sql
    
    def __init__(
            self,
            label : str ='main',
            flavor : str= None,
            debug : bool = False,
            **kw
        ):
        """
        Build the SQLConnector engine and connect to the database
        """
        import databases, sqlalchemy, asyncio
        ### set __dict__ in base class
        super().__init__('sql', label=label, **kw)

        ### ensure flavor and label are set accordingly
        if 'flavor' not in self.__dict__ and flavor is None:
            raise Exception("Missing flavor. Update config.yaml or provide flavor as an argument")
        elif 'flavor' not in self.__dict__: self.flavor = flavor

        ### verify the flavor's requirements are met
        if self.flavor not in self.flavor_configs:
            raise Exception(f'Flavor {self.flavor} is not supported by Meerschaum SQLConnector')
        self.verify_attributes(self.flavor_configs[self.flavor]['requirements'], debug=debug)

        ### build the sqlalchemy engine and set DATABASE_URL
        self.engine, self.DATABASE_URL = self.create_engine(include_uri=True, debug=debug)

    @property
    def metadata(self):
        import sqlalchemy
        if '_metadata' not in self.__dict__:
            self._metadata = sqlalchemy.MetaData(self.engine)
        return self._metadata

    @property
    def db(self):
        import databases
        if '_db' not in self.__dict__:
            self._db = databases.Database(self.DATABASE_URL)
        return self._db
