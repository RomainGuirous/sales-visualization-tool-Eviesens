import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
import re
pd.set_option('display.max_rows', 500)
# pour 1 atelier donné :
# -combien de  personnes en moyenne
# -quel chiffre d'affaire en moyenne par an/mois

# pour 1 soin donné : quel chiffre d'affaire moyen par mois (Ventes Eviesens, lebienetre.fr et SARL edito)

# par mois, combien d'interventions extérieures et quel chiffre d'affaire moyen ?

# quel est le total de chiffre d'affaire par mois/an obtenu par lebienetre.fr et SARL edito

conn= create_engine('mysql+mysqlconnector://root:root@localhost:3306/eviesens')

#le dataframe de chaque table, etrait de la base de donnee
df_vendeur= pd.read_sql_query('SELECT * FROM vendeur',conn)
df_type_transaction= pd.read_sql_query('SELECT * FROM type_transaction',conn)
df_type_structure= pd.read_sql_query('SELECT * FROM type_structure',conn)
df_moyen_paiement= pd.read_sql_query('SELECT * FROM moyen_paiement',conn)
df_type_activite= pd.read_sql_query('SELECT * FROM type_activite',conn)
df_commande= pd.read_sql_query('SELECT * FROM commande',conn)
df_commande_activite= pd.read_sql_query('SELECT * FROM commande_activite',conn)
df_activite= pd.read_sql_query('SELECT * FROM activite',conn)
df_client= pd.read_sql_query('SELECT * FROM client',conn)

df_type_activite_activite_commande_activite=df_type_activite.join(df_activite.set_index('type_activite_id'),on=('type_activite_id'), how="inner")
df_type_activite_activite_commande_activite=df_type_activite_activite_commande_activite.join(df_commande_activite.set_index('activite_id'),on=('activite_id'), how="inner")
df_activite_nom_qte_prix=df_type_activite_activite_commande_activite[['activite_nom','activite_prix','commande_quantite']]

df_activite_nom_qte_prix=df_activite_nom_qte_prix.groupby(by=['activite_nom']).sum()
print(df_activite_nom_qte_prix)














# df_atelier_an = pd.read_sql_query('''
#                                   SELECT type_activite_nom, activite_prix, commande_quantite,type_actite_id,activite_id
#                                   FROM ((activite 
#                                   INNER JOIN type_activite
#                                   ON activite.type_actite_id = type_activite.type_actite_id)
#                                   INNER JOIN commande_activite
#                                   ON activite.activite_id =commande_activite.activite_id) ''', conn)
# print(df_atelier_an)