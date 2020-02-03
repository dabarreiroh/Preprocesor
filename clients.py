import configfile as database
from database import Database
from table import Table


class Clients(Table):
    def __init__(self,):
        Database.__init__(self, database.Portal)
        self.table_name='clients'
        self.dataframe=Database.db_get_data(self,self.table_name)


