import psycopg2
import os
import pandas as pd
from database import Database
from datetime import datetime
from datetime import timedelta
class Data_Timmer_Request(Database):

    def __init__(self, database_name):
        Database.__init__(self, database_name)
        self.current_hour = datetime.now()


    def db_get_data(self, table_name, hours, column_name,condition_add,index_col):
        try:
            self.cursor.execute("""SELECT {} FROM "{}" WHERE ("{}" >= '{}' AND "{}" < '{}') {} """.format(",".join(index_col),table_name, column_name, self.current_hour - timedelta(hours=hours), column_name, self.current_hour,condition_add))
            result= [content for content in self.cursor.fetchall()]
            return pd.DataFrame(result,columns=index_col)
        except(Exception, psycopg2.Error) as error:
            if error:
                return error


