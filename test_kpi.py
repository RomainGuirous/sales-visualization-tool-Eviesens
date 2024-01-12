import pandas as pd
from sqlalchemy import create_engine
import kpi
import database_connection as dbc
pd.set_option('display.max_rows', 500)

df_activite=dbc.get_data()

# ### CHIFFRE D'AFFAIRE ###
# ##  CA PAR ATELIER / AN
# print(kpi.CA_atelier_an(df_activite,2023))

# # ## CA PAR ATELIER / MOIS
# print(kpi.CA_atelier_mois(df_activite,1,2023)) #mois,annee à adapter
# # for i in range(1,13):
# #     print(kpi.CA_atelier_mois(df_activite,i,2023))

# # ## CA / VENDEUR
# print(kpi.CA_vendeur_an(df_activite,2023))

# # ## CA / (VENDEUR, ATELIER)
# print(kpi.CA_vendeur_atelier_an(df_activite,2023))

# # ## CA / CLIENT / AN
# print(kpi.CA_par_client(df_activite, 2023))

# # ## CA / AN (TABLEAU CHAQUE MOIS)
# print(kpi.CA_annuel(df_activite,2023))

# ## REVENU NET / AN (TABLEAU CHAQUE MOIS)
print(kpi.revenu_net_annuel(df_activite,2023))

# ## CA ANNUEL / ANS
print(kpi.CA_par_ans(df_activite))

# # ## REVENU NET ANNUEL / ANS
# print(kpi.revenu_net_par_ans(df_activite))

# # ## MOYENNE PERSONNE ATELIER/AN
# print(kpi.moy_personne_atelier_an(df_activite,2023))

# ## NBR PERSONNE ATELIER/AN
# print(kpi.nbr_personne_atelier_an(df_activite,2023))