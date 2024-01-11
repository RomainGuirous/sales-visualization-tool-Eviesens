import pandas as pd
from sqlalchemy import create_engine
import visualisation as viz
import database_connection as dbc

df_activite=dbc.get_data()

### CHIFFRE D'AFFAIRE ###
##  CA PAR ATELIER / AN
viz.show_atelier_an(df_activite, 2023)

# ## CA PAR ATELIER / MOIS
viz.show_atelier_mois(df_activite,1,2023)
# for i in range(1, 13):
#     viz.show_atelier_mois(df_activite,i,2023)

# CA / VENDEUR
viz.show_vendeur_an(df_activite, 2023)

## CA / (VENDEUR, ATELIER)
viz.show_vendeur_atelier_an(df_activite, 2023)

### NOMBRE ACHAT ###
## NA ATELIER / AN
viz.show_nbr_atelier_an(df_activite, 2023, dbc.get_type_activite())

### NOMBRE ACHAT ###
## NA ATELIER / MOIS
viz.show_nbr_atelier_mois(df_activite, 2023)

### CA ANNUEL POUR CHAQUE MOIS ###
viz.show_CA_annuel(df_activite, 2023)

### REVENU NET ANNUEL POUR CHAQUE MOIS ###
viz.show_revenu_net_annuel(df_activite, 2023)

### LISTE DES PLUS GROS ACHETEURS
viz.show_CA_par_client(df_activite, 2023)