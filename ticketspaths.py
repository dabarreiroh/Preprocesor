import configfile as database
from database import Database
from table import Table
from url import URL

class Ticketpaths(Table):
    def __init__(self,clients,tickets):
        self.table_name = 'tickets_paths'
        Database.__init__(self, database.Services)
        self.dataframe=((tickets.tb_join(clients.dataframe.set_index('cli_id_client'),'tic_id_client'))[['cli_public_id','tic_public_id_ticket','tic_url']])
        Table.__init__(self,database.Services,self.table_name,self.tp_ticketpaths())

    def tp_ticketpaths(self):
        for i in range(0, 10):
            Table.tb_ins_col(self,3 + i,self.table_name + str(i),[URL(self.dataframe[['tic_url']].iloc[url].tic_url).path[0 + i] for url in range(len(self.dataframe))])
        return self.dataframe

    def tp_insert_data(self):
        for i in range(0, len(self.dataframe)):
            Database.db_ins_data(self,
                                 self.table_name,
                                 (self.dataframe.loc[i][['tickets_paths0', 'tic_public_id_ticket', 'tickets_paths1', 'tickets_paths2','tickets_paths3','tickets_paths4','tickets_paths5','tickets_paths6','tickets_paths7','tickets_paths8','tickets_paths9','cli_public_id']]).tolist(),
                                 Database.db_compare_data(self,
                                                          self.table_name,
                                                          Database.db_indexcol(self,self.table_name)[1:3],
                                                          [self.dataframe.at[i,'tickets_paths0'],self.dataframe.at[i, 'tic_public_id_ticket']]
                                                          )
                                 )
