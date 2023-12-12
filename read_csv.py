import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine
pd.set_option('display.max_rows', 500)


filepath="./donnees/Janvier-2023.csv"
df=pd.read_csv(filepath)
df_activite = df.iloc[:,24:28] # selectionne toutes les lignes des colonnes Y a AB d'excel (colonnes 25 a 28 du csv)
df_activite=df_activite.dropna()

# list_row = ["Eviesens", "a", "33.6", "a",]
# df_activite.loc[len(df_activite)] = list_row


df_activite["Prix"]=df_activite["Prix"].replace(regex='[^,.0-9]', value=np.nan) # remplace tout ce qui n'est pas un chiffre, un . ou une , par Nan
df_activite["Prix"]=df_activite["Prix"].str.replace(',', '.', regex=True) # remplace les , par des . dans la colonne Prix
# print(df_activite)






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
# print(df_commande)


df_activite = df_activite.rename(columns={'Vendeur.1': 'vendeur_id', 'Intitulé.1': 'activite_nom', 'Prix':'activite_prix', 'Type.1':'type_activite_id'})
print(df_activite)


conn_string = 'postgresql://postgres:root@host/eviesens'
  
db = create_engine(conn_string) 
conn = db.connect() 
  
  
# Create DataFrame 
df_activite.to_sql('activite', con=conn, if_exists='replace', 
          index=False)
conn = psycopg2.connect(conn_string 
                        ) 
conn.autocommit = True