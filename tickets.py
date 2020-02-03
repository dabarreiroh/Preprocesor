import configfile as database
from data_timmer_request import Data_Timmer_Request
from table import Table


class Tickets(Table,Data_Timmer_Request):
    def __init__(self,hours):
        Data_Timmer_Request.__init__(self,database.Portal)
        self.table_name = 'tickets'
        self.dataframe = Data_Timmer_Request.db_get_data(self, self.table_name,hours, 'tic_creation_date', "AND (tic_attack_type ='PHISHING')", ['tic_id_client', 'tic_public_id_ticket', 'tic_url'])


