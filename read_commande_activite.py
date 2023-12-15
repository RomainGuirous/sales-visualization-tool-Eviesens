import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
import re
pd.set_option('display.max_rows', 500)


filepath="./donnees/fiches_mensuelles/Janvier-2023.csv"

#mettre:
    #activite_id
    #commande_id
    #commmande_date_soin
    #commande_quantite
    #commande_deplacement
    #commande_reduction
    #commande_date_encaissement
    #commande_perception
    #commande_date_remboursement
df=pd.read_csv(filepath)
# df=df.iloc[:49,:21]
# print(df)
def select_commmande_activite(df) :
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
    df_commande = df_commande[df_commande["Date d'achat"].notna()]  ######/!\/!\ PROBLEME DATE D'ACHAT  NAN A REPARER PLUS TARD /!\ /!\ #####

    df_commande = df_commande[['Intitulé',"Date d'achat",'Date soin', 'Quantité', 'Déplacement', 'Reduction', 'Date perception', 'Date remboursement', 'Date Encaissement ']] #on selectionne les colonnes utiles pour nous par leur nom dans le DF

    df_commande = df_commande.rename(columns={'Intitulé':'activite_nom',"Date d'achat":'commande_date_achat','Date soin': 'commande_date_soin', 'Quantité': 'commande_quantite', 'Déplacement':'commande_deplacement',
                                              "Reduction":'commande_reduction',"Date perception":'commande_perception',"Date remboursement":'commande_date_remboursement', 'Date Encaissement ':'commande_date_encaissement'})
    
    return df_commande


# df_commande_activite=select_commmande_activite(df)
# print(df_commande_activite)
# envoie un dataframe dans la base de donnee
def df_to_database(df, table_name, connection) :
    df.to_sql(table_name, con=connection, index=False, if_exists='append')

#transforme les dates format excel au format SQL et si Nan retourne Nan
def excel_to_sql_date(date):
    if pd.isnull(date):
        return date
    else:
        date=re.sub(r"/","-",date) #on transforme les "/" en "-"
        date=re.sub(r"(\d\d)-(\d\d)-(\d{4})",r"\3-\2-\1",date) #on inverse les jours et les mois
        return date

#on transforme date avec un jour fixe (le 01)
def date_jour_fixe(date):
    date=re.sub(r"(\d{4}-\d\d)-\d\d",r"\1-01",date)
    return date

# si dans une ligne de la table activite le couple (activite_nom, activite_mois) est egal au couple (activite_nom,commande_date_achat) d'une ligne la table commande_activite
# il rajoute à la ligne de commande_activite son activite_id
def comp_activite_commande_activite(df_acti, df_com_acti) :
    x=df_com_acti
    x['activite_id']=np.nan
    x['commande_date_achat']=x['commande_date_achat'].tranform(lambda x: date_jour_fixe(x))
    for i in df_acti.index :
        for j in x.index :
            if (df_acti["activite_nom"][i]==x["activite_nom"][j]) & (df_acti["activite_mois"][i]==x["commande_date_achat"][j]):
                x.loc[j,['activite_id']]=df_acti.loc[i,['activite_id']]
    return x

#Main
conn=create_engine('mysql+mysqlconnector://root:root@localhost:3306/eviesens')

filepaths=os.listdir("./donnees/fiches_mensuelles/") #récupère liste des noms des fichiers dans le dossier "fiches_mensuelles"

for i in range(len(filepaths)) :
    filepaths[i]="./donnees/fiches_mensuelles/"+filepaths[i] #on récupère liste des filepath de chaque fiche mensuelle

for filepath in filepaths :
    df=pd.read_csv(filepath)
    df_commande_activite=select_commmande_activite(df)


df_commande_activite=df_commande_activite[['commande_date_achat','commande_date_soin','commande_perception', 'commande_date_remboursement', 'commande_date_encaissement']]
df_commande_activite['commande_date_achat'] =df_commande_activite['commande_date_achat'].transform(lambda x: excel_to_sql_date(x))
df_commande_activite['commande_date_soin'] =df_commande_activite['commande_date_soin'].transform(lambda x: excel_to_sql_date(x))
df_commande_activite['commande_perception'] =df_commande_activite['commande_perception'].transform(lambda x: excel_to_sql_date(x))
df_commande_activite['commande_date_remboursement'] =df_commande_activite['commande_date_remboursement'].transform(lambda x: excel_to_sql_date(x))
df_commande_activite['commande_date_encaissement'] =df_commande_activite['commande_date_encaissement'].transform(lambda x: excel_to_sql_date(x))

# df_activite=pd.read_sql_query('SELECT * FROM activite', conn)
# df_inner_join=df_commande_activite.join(df_activite.set_index('activite_nom'), on='activite_nom',how='inner')

df_activite= pd.read_sql_query('SELECT * FROM activite', conn)
df_activite=df_activite[['activite_id','activite_nom','activite_mois']] #on prend nom et mois pour pouvoir comparer, id est ce que nous voulons




# print(df_activite)

# print(df_commande_activite)
