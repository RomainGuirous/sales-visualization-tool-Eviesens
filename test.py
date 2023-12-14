import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
import re
pd.set_option('display.max_rows', 500)


def select_commmande(df) :
    df_commande = df.iloc[:49,:21] # selectionne uniquement les lignes jusque la ligne 50 du csv et 21 premieres colonnes
    df_commande = df_commande[df_commande['Structure'].notna()]
    df_commande = df_commande[df_commande['Type'].notna()]
    df_commande = df_commande[df_commande['Vendeur'].notna()]
    df_commande = df_commande[df_commande['Intitulé'].notna()]
    df_commande = df_commande[df_commande['Transaction'].notna()]
    df_commande = df_commande[df_commande['Moyen de paiement'].notna()]
    df_commande = df_commande[df_commande['Déplacement'].notna()]
    df_commande = df_commande[df_commande['Quantité'].notna()]
    df_commande = df_commande[df_commande['Reduction'].notna()]
    df_commande = df_commande[['Structure','Transaction','Moyen de paiement',"Date d'achat", "Nom","Prénom"]]
    df_commande = df_commande.rename(columns={'Structure': 'structure_nom', 'Transaction': 'type_transaction_nom', 'Moyen de paiement':'moyen_paiement_nom', "Date d'achat":'commande_date_achat',"Nom":'client_nom','Prénom':'client_prenom'}) #on change nom col
    return df_commande

df=pd.read_csv("./donnees/fiches_mensuelles/Janvier-2023.csv")
df_activite=select_commmande(df)







conn= create_engine('mysql+mysqlconnector://root:root@localhost:3306/eviesens')
contacts_bd = pd.read_sql_query('SELECT client_id,client_nom,client_prenom FROM client', conn)
print(contacts_bd)
list_contacts=[]
for i in contacts_bd.index :
    list_contacts.append((contacts_bd["client_nom"][i], contacts_bd["client_prenom"][i]))
print(list_contacts)
# before_dico_type_activite_db=database_to_dict("type_activite",conn)
