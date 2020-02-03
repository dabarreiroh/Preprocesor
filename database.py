import psycopg2
from psycopg2 import sql
import configfile as database_name
from datetime import datetime
import pandas as pd

class Database:
    def __init__(self, database_name):
        self.database_name = database_name
        self.connector=psycopg2.connect(user=self.database_name['user'], password=self.database_name['password'], database=self.database_name['database'], host=self.database_name['host'], port=self.database_name['port'])
        self.cursor = self.connector.cursor()
        self.current_hour = datetime.now()# For time operations

    def db_create_table(self,table_name, fields,data_types):
        try:
            self.cursor.execute("""CREATE TABLE  {}  {};""".format(table_name,tuple([fields[l]+" "+ data_types[l] for l in range(len(fields))])))
        except (Exception, psycopg2.Error) as error:
            return error

    def db_indexcol(self, table_name):
        try:
            self.cursor.execute('Select * FROM {} LIMIT 0'.format(table_name))
            index = [content[0] for content in self.cursor.description]
            return index
        except(Exception, psycopg2.Error) as error:
            if error:
                return error

    def db_ins_data(self, table_name, db_data,enable):
        try:
             if enable:
                self.cursor.execute("""INSERT INTO "{}" ({}) VALUES ({});""".format(table_name, str(self.db_indexcol(table_name)[1:]).replace("'", "").replace("[", "").replace("]", ""), "%s," * (len(self.db_indexcol(table_name)) - 2) + "%s"), db_data)
                self.connector.commit()

        except(Exception, psycopg2.Error) as error:
            if error:
                return error

    def db_get_data(self, table_name):
        try:
            self.cursor.execute('SELECT * FROM "{}"'.format( table_name))
            df = [content for content in self.cursor.fetchall()]
            return pd.DataFrame(df, columns=self.db_indexcol(table_name))
        except(Exception, psycopg2.Error) as error:
            if error:
                return error

#Modificar campos de una columna por otra relacionada
    def db_update_data(self,table_name, update, refcolumns, refdata,enable):
        a = ("""UPDATE "{}" SET {} WHERE {};""".format(table_name,update,tuple(['(' + refcolumns[i] + ' = ' + "'" + str(refdata[i]) + "')" for i in range(0, len(refdata))]))).replace('[','').replace(']', '').replace(',', ' AND ').replace("\"", '')
        try:
            if enable:
                self.cursor.execute(a)
                return a
        except (Exception, psycopg2.Error) as error:
            if error:
                return error

    def db_compare_data(self,table_name, columns,data):
        if len(data)!=len(columns):
            raise RuntimeError('In:db_compare_data :There are not the same length between data and columns')
        try:
            a=("""SELECT * FROM "{}" WHERE {};""".format(table_name,tuple(['('+columns[i]+' = '+"'"+str(data[i])+"')" for i in range(0,len(data))]))).replace('[','').replace(']','').replace(',',' AND ').replace("\"",'')
            self.cursor.execute(a)
            if self.cursor.fetchall():
                return False
            else:
                return True
        except(Exception,psycopg2.Error) as error:
            if error:
                return error

    def db_perform_search(self,table_name,return_cols,columns,data):
        try:
            a = ("""SELECT {} FROM "{}" WHERE {};""".format(return_cols,table_name, tuple(['(' + columns[i] + ' = ' + "'" + str(data[i]) + "')" for i in range(0, len(data))]))).replace('[','').replace(']', '').replace(',', ' AND ').replace("\"", '').replace('AND )',')')
            self.cursor.execute(a)
            df = [content for content in self.cursor.fetchall()]
            return pd.DataFrame(df)#,columns=return_cols)
        except(Exception, psycopg2.Error) as error:
            if error:
                return error




