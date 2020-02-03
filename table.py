#This class only proccess dataframes. dont try a SQL command here
import configfile as database
from database import Database
import pandas as pd
from operations import Operations
from url import URL
class Table(Database):
    def __init__(self,database_name,table_name, dataframe):
        Database.__init__(self,database_name)
        self.table_name = table_name
        self.dataframe = dataframe
    def tb_ins_col(self,loc,col,val):
        return self.dataframe.insert(loc,col,val,allow_duplicates=False)
    def tb_isnull(self):
        return self.dataframe.isnull()
    def tb_notnull(self):
        return self.dataframe.notnull()
    def tb_join(self,externdf, on):
        ndf = self.dataframe.join(externdf, on=on)
        return ndf
    def tb_acumulative(self,dataframe):
        sum=0
        for i in range(0,len(dataframe.index)):
            sum=sum+dataframe.at[i,0]
        return sum



