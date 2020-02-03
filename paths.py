import configfile as database
from database import Database
from table import Table
from url import URL
from operations import Operations
import pandas as pd

class Paths(Table, Operations):

    def __init__(self, ticketpaths):
        self.table_name = 'analizer_paths'
        Database.__init__(self, database.Services)
        Operations.__init__(self)
        self.dataframe = self.paths_dataframe()
        self.ticpaths = ticketpaths
        Table.__init__(self, database.Services, self.table_name, self.tb_paths())

    def paths_dataframe(self):
        return pd.DataFrame(columns=Database.db_indexcol(self, self.table_name)[1:])

    def printself(self):
        return self.dataframe

    def tb_paths(self):
        for url in range(len(self.ticpaths.dataframe)):
            for i in range(0, 10):
                if URL(self.ticpaths.dataframe[['tic_url']].iloc[url].tic_url).path[0 + i] != "NaN":
                    self.dataframe.at[len(self.dataframe), 'alz_path_path'] =URL(self.ticpaths.dataframe[['tic_url']].iloc[url].tic_url).path[0 + i]
                    self.dataframe.at[len(self.dataframe) - 1, 'alz_path_public_id'] = self.ticpaths.dataframe.at[url, 'cli_public_id']
                    self.dataframe.at[len(self.dataframe) - 1, 'alz_path_level'] = i + 1
                    self.dataframe.at[len(self.dataframe) - 1, 'alz_path_recurrence'] = 1
                    self.dataframe.at[len(self.dataframe) - 1, 'alz_path_ticket_id'] = self.ticpaths.dataframe.at[url, 'tic_public_id_ticket']
                    self.dataframe.at[len(self.dataframe) - 1, 'alz_client_score'] = Operations.clientscore(self,
                                                                                                            Table.tb_acumulative(self,
                                                                                                                                 Database.db_perform_search(self,
                                                                                                                                                            self.table_name,
                                                                                                                                                            'alz_path_recurrence',
                                                                                                                                                            ['alz_path_public_id','alz_path_path'],
                                                                                                                                                            (self.dataframe.loc[len(self.dataframe) - 1][['alz_path_public_id','alz_path_path']]).tolist()
                                                                                                                                                            )
                                                                                                                                 ),
                                                                                                            Table.tb_acumulative(self,
                                                                                                                                 Database.db_perform_search(self,
                                                                                                                                                            self.table_name,
                                                                                                                                                            'alz_path_recurrence',
                                                                                                                                                            ['alz_path_path'],
                                                                                                                                                            (self.dataframe.loc[len(self.dataframe) - 1][['alz_path_path']]).tolist()
                                                                                                                                                            )
                                                                                                                                 )
                                                                                                            )
        return self.dataframe

    def paths_recurrence(self):
        [Database.db_update_data(self,
                                 self.table_name,
                                 'alz_path_recurrence=alz_path_recurrence+1',
                                 Database.db_indexcol(self,
                                                      self.table_name)[1:3],
                                 (self.dataframe.loc[i][Database.db_indexcol(self,
                                                                             self.table_name)[1:3]]).tolist(),
                                 not (Database.db_compare_data(self,
                                                               self.table_name,
                                                               Database.db_indexcol(self,
                                                                                    self.table_name)[1:3] + [Database.db_indexcol(self,
                                                                                                                                  self.table_name)[4]],
                                                               (self.dataframe.loc[i][['alz_path_path', 'alz_path_public_id','alz_path_level']])
                                                               )
                                      )
                                 )
         for i in range(0, len(self.dataframe))]

    def paths_client_score(self):
        [Database.db_update_data(self,
                                 self.table_name,
                                 'alz_client_score={}'.format(str(Operations.clientscore(self,
                                                                                         Table.tb_acumulative(self,
                                                                                                              Database.db_perform_search(self,
                                                                                                                                         self.table_name,
                                                                                                                                         'alz_path_recurrence',
                                                                                                                                         ['alz_path_public_id','alz_path_path'],
                                                                                                                                         (self.dataframe.loc[i][['alz_path_public_id','alz_path_path']]).tolist()
                                                                                                                                         )
                                                                                                              ),
                                                                                         Table.tb_acumulative(self,
                                                                                                              Database.db_perform_search(self,
                                                                                                                                         self.table_name,
                                                                                                                                         'alz_path_recurrence',
                                                                                                                                         ['alz_path_path'],
                                                                                                                                         (self.dataframe.loc[i][['alz_path_path']]).tolist()
                                                                                                                                         )
                                                                                                              )
                                                                                         )
                                                                  )
                                                              ),
                                 Database.db_indexcol(self,
                                                      self.table_name)[1:3],
                                 (self.dataframe.loc[i][Database.db_indexcol(self,
                                                                             self.table_name)[1:3]]).tolist(),
                                 not (Database.db_compare_data(self,
                                                               self.table_name,
                                                               Database.db_indexcol(self,
                                                                                    self.table_name)[1:3],
                                                               (self.dataframe.loc[i][['alz_path_path', 'alz_path_public_id']])
                                                               )
                                      )
                                 )
         for i in range(0, len(self.dataframe))]

    def paths_insert_data(self):
        [Database.db_ins_data(self,
                              self.table_name,
                              self.dataframe.loc[i][Database.db_indexcol(self,
                                                                         self.table_name)[1:]],
                              Database.db_compare_data(self,
                                                       self.table_name,
                                                       Database.db_indexcol(self,
                                                                            self.table_name)[1:3] + [Database.db_indexcol(self,
                                                                                                                          self.table_name)[4]],
                                                       (self.dataframe.loc[i][['alz_path_path', 'alz_path_public_id', 'alz_path_level']])
                                                       )
                              )
         for i in range(0, len(self.dataframe))]