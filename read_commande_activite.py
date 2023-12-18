import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
import re
pd.set_option('display.max_rows', 401)


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
df=df.iloc[:49,:21]
print(df)
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

# compare deux chaines de charactere, si ils sont egaux ou tous les deux nuls renvoie True, sinon renvoie False
def equal_or_both_null(s1, s2) :
    s1_2=str(s1)
    s2_2=str(s2)
    if pd.isnull(s1_2) & pd.isnull(s2_2) :
        return True
    if pd.isnull(s1_2) | pd.isnull(s2_2) :
        return False
    if s1_2.lower()==s2_2.lower() :
        return True
    return False

# si dans une ligne de la table activite le couple (activite_nom, activite_mois) est egal au couple (activite_nom,commande_date_achat) d'une ligne la table commande_activite
# il rajoute à la ligne de commande_activite son activite_id
def add_id_df(df_acti, df_com_acti) :
    x=df_com_acti
    x['activite_id']=np.nan #pour créer colonne, on la remplit de vides
    x['commande_date_achat_test']=x['commande_date_achat'].transform(lambda x: date_jour_fixe(x)) #on crée une nouvelle colonne avec le jour fixe (01) qui va permettre de comparer (comme ça la vraie date reste intact)
    for i in df_acti.index :
        for j in x.index :
            if equal_or_both_null(df_acti["activite_nom"][i],x["activite_nom"][j]) & equal_or_both_null(df_acti["activite_mois"][i],x["commande_date_achat_test"][j]): #on s'assure de ne pas avoir de vide et de tout mettre en minuscule
                x.loc[j,['activite_id']]=df_acti.loc[i,['activite_id']]   
    x=x.drop(['commande_date_achat_test'],axis=1) #on supprime la colonne de commparaison qui ne sert plus
    return x

def add_id_df2(df_com, df_com_acti) :
    x=df_com_acti
    x['commande_id']=np.nan #pour créer colonne, on la remplit de vides
    # x['commande_date_achat_test']=x['commande_date_achat'].transform(lambda x: date_jour_fixe(x)) #on crée une nouvelle colonne avec le jour fixe (01) qui va permettre de comparer (comme ça la vraie date reste intact)
    for i in df_com.index :
        for j in x.index :
            if equal_or_both_null(df_com["activite_nom"][i],x["activite_nom"][j]) & equal_or_both_null(df_com["commande_date_achat"][i],x["commande_date_achat"][j]): #on s'assure de ne pas avoir de vide et de tout mettre en minuscule
                x.loc[j,['activite_id']]=df_com.loc[i,['activite_id']]   
    x=x.drop(['commande_date_achat_test'],axis=1) #on supprime la colonne de commparaison qui ne sert plus
    return x

def get_clients_id(df_to_get, df_from_db) :
    pd.options.mode.chained_assignment = None
    df_res=df_to_get
    df_res["client_id"]=np.nan # initialise tous les id a null
    for i in df_res.index :
        for j in df_from_db.index :
            if equal_or_both_null(df_res.loc[i,"client_nom"], df_from_db.loc[j,"client_nom"]) & equal_or_both_null(df_res.loc[i,"client_prenom"], df_from_db.loc[j,"client_prenom"]):
                df_res.loc[i, "client_id"]=df_from_db.loc[j, "client_id"] # quand un couple nom/prenom est trouve dans la bdd, son id lui est associe
    pd.options.mode.chained_assignment = "warn"
    return df_res

#Main
conn=create_engine('mysql+mysqlconnector://root:root@localhost:3306/eviesens')

filepaths=os.listdir("./donnees/fiches_mensuelles/") #récupère liste des noms des fichiers dans le dossier "fiches_mensuelles"

for i in range(len(filepaths)) :
    filepaths[i]="./donnees/fiches_mensuelles/"+filepaths[i] #on récupère liste des filepath de chaque fiche mensuelle

for filepath in filepaths :
    print(filepath)
    df=pd.read_csv(filepath)
    df_commande_activite=select_commmande_activite(df)

    #on met les dates au format yyyy-mm-dd
    df_commande_activite['commande_date_achat'] =df_commande_activite['commande_date_achat'].transform(lambda x: excel_to_sql_date(x))
    df_commande_activite['commande_date_soin'] =df_commande_activite['commande_date_soin'].transform(lambda x: excel_to_sql_date(x))
    df_commande_activite['commande_perception'] =df_commande_activite['commande_perception'].transform(lambda x: excel_to_sql_date(x))
    df_commande_activite['commande_date_remboursement'] =df_commande_activite['commande_date_remboursement'].transform(lambda x: excel_to_sql_date(x))
    df_commande_activite['commande_date_encaissement'] =df_commande_activite['commande_date_encaissement'].transform(lambda x: excel_to_sql_date(x))

    df_activite= pd.read_sql_query('SELECT * FROM activite', conn) #on appelle df_activite pour l'injecter dans la fonction add_id_df()

    df_commande_activite=add_id_df(df_activite,df_commande_activite) #on ajoute la colonne activite_id à df_commande_activite
    df_commande_activite=df_commande_activite.drop(['activite_nom'],axis=1) #on supprime activite_nom qui n'est pas dans la table commande_activite
    
    df_client=pd.read_sql_query('SELECT * FROM client', conn) #on appelle df_client pour l'injecter dans la fonction add_id_df()

    # print(df_activite)

    # print(df_commande_activite)
