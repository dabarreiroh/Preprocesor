#PREPROCESSOR AM-CTAC
#Script for the preprocesing of URLs from tickets for the Analizer Module
#____________________________________________________________________________
#____________________________________________________________________________
#Configuration:
#
#The unique parameter that have to be configured is the hours, that represents
#the interval of time in that the script is executed.
#
#_____________________________________________________________________________
from clients import Clients
from tickets import Tickets
from ticketspaths import Ticketpaths
from paths import Paths


#                                 unique parameter
#Tables                                  |
#ticketpaths                             V
ticpaths = Ticketpaths(Clients(),Tickets(8))
paths=Paths(ticpaths)

#Client_score and recurrence
paths.paths_recurrence()
paths.paths_client_score()

#insert_data
    #ticketpaths
ticpaths.tp_insert_data()
    #paths
paths.paths_insert_data()