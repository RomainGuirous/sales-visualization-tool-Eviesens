import pandas as pd
import numpy as np
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine
import mysql.connector
pd.set_option('display.max_rows', 500)


filepath="./Projet_Stage_Eviesens/donnees/Janvier-2023.csv"
df=pd.read_csv(filepath)
df_activite = df.iloc[:,24:28] # selectionne toutes les lignes des colonnes Y a AB d'excel (colonnes 25 a 28 du csv)
df_activite=df_activite.dropna() #supprime Nan

# list_row = ["Eviesens", "a", "33.6", "a",]
# df_activite.loc[len(df_activite)] = list_row


df_activite["Prix"]=df_activite["Prix"].replace(regex='[^,.0-9]', value=np.nan) # remplace tout ce qui n'est pas un chiffre, un . ou une , par Nan
df_activite["Prix"]=df_activite["Prix"].str.replace(',', '.', regex=True) # remplace les , par des . dans la colonne Prix




df_activite = df_activite.rename(columns={'Vendeur.1': 'vendeur_nom', 'Intitulé.1': 'activite_nom', 'Prix':'activite_prix', 'Type.1':'type_activite_nom'}) #on change nom col

df_type_activite=df_activite['type_activite_nom'].drop_duplicates() #on supprime doublons -> Nan
df_type_activite=df_type_activite.dropna() #on supprime Nan

# print(df_type_activite)

df_vendeur=df_activite['vendeur_nom'].drop_duplicates() #on supprime doublons -> Nan
df_vendeur=df_vendeur.dropna() #on supprime Nan


df_commande = df.iloc[:49,:21] # selectionne uniquement les lignes jusque la ligne 50 du csv et 21 premieres colonnes
df_commande = df_commande[df_commande['Structure'].notna()]
df_commande = df_commande[df_commande['Type'].notna()]
df_commande = df_commande[df_commande['Vendeur'].notna()]
df_commande = df_commande[df_commande['Intitulé'].notna()]
df_commande = df_commande[df_commande['Transaction'].notna()]
df_commande = df_commande[df_commande['Moyen de paiement'].notna()]
df_commande = df_commande[df_commande['Déplacement'].notna()]
df_commande = df_commande[df_commande['Quantité'].notna()]
df_commande = df_commande[df_commande['Reduction'].notna()]  #si un des champs est vide, toute la ligne est supprimée
# print(df_commande)



conn= create_engine('mysql+mysqlconnector://root:root@localhost:3306/test')

# # df_type_activite.to_sql('type_activite', con=conn, index=False, if_exists='append') # ajoute les type d'activite dans la BDD, faire atttention à ne pas dupliquer données
# # df_vendeur.to_sql('vendeur', con=conn, index=False, if_exists='append') #/!\


df_vendeur_db = pd.read_sql_query('SELECT * FROM vendeur', conn) #on récupère DF de la BDD pour avoir le bon id
list_vendeur_db=df_vendeur_db.to_dict(orient='records') # crée une liste de dico en clé  -> valeur 1ere col, en valeur -> valeur 2eme col

dico_vendeur_db={}  #on tranforme la liste de dico en dico valeur(k), id(v)
for i in list_vendeur_db:
    #i ={'vendeur_id':val1, 'vendeur_nom':val2 }
    dico_vendeur_db[i['vendeur_nom']]=i['vendeur_id']
# print(dico_vendeur_db)

df_type_activite_db= pd.read_sql_query('SELECT * FROM type_activite', conn) #on récupère DF de la BDD pour avoir le bon id
list_type_activite_db=df_type_activite_db.to_dict(orient='records') # crée une liste de dico en clé  -> valeur 1ere col, en valeur -> valeur 2eme col

dico_type_activite_db={}  #on tranforme la liste de dico en dico valeur(k), id(v)
for i in list_type_activite_db:
    #i ={'type_activite_id':val1, 'type_activite_nom':val2 }
    dico_type_activite_db[i['type_activite_nom']]=i['type_activite_id'] #i['type_activite_nom']=val2 ; i['type_activite_id']= val1
# print(dico_type_activite_db)

#fusionner les 2 dico:
dico_type_activite_db.update(dico_vendeur_db) # pas réussis à le renommer ?
# print(dico_type_activite_db)

df_activite= df_activite.replace(dico_type_activite_db) #on transforme les noms de vendeur et type_activite en leur id
df_activite = df_activite.rename(columns={'vendeur_nom': 'vendeur_id', 'activite_nom': 'activite_nom','type_activite_nom':'type_activite_id'}) #on change les nom de col pour correspondre à la table  de  la BDD
# print(df_activite)
# # df_activite.to_sql('activite', con=conn, index=False, if_exists='append') #/!\ on injecte dans BDD


