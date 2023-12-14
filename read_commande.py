import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
import re
pd.set_option('display.max_rows', 500)


filepath="./donnees/fiches_mensuelles/Janvier-2023.csv"

# commande
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
    df_commande = df_commande[df_commande["Date d'achat"].notna()]  ######/!\/!\ PROBLEME DATE D'ACHAT  NAN A REPARER PLUS TARD /!\ /!\ #####
    df_commande = df_commande[['Structure','Transaction','Moyen de paiement','Nom',"Date d'achat" ]] #on selectionne les colonnes utiles pour nous par leur nom dans le DF
    df_commande = df_commande.rename(columns={'Structure': 'type_structure_nom', 'Transaction': 'type_transaction_nom', 'Moyen de paiement':'moyen_paiement_nom', "Nom":'client_id',"Date d'achat":'commande_date_achat'})
    #on change nom col pour correspondre au nom dans les tables type_structure, type_transaction, moyen_paiement et commande (pour commande_date_achat et client_id)
    return df_commande


# envoie un dataframe dans la base de donnee
def df_to_database(df, table_name, connection) :
    df.to_sql(table_name, con=connection, index=False, if_exists='append')

# a partir d'un nom de table de la base de donne, recupere un dictionnaire avec comme cle le nom du champ et comme valeur son id
def database_to_dict(table_name, connection) :
    df_res = pd.read_sql_query('SELECT * FROM '+table_name, connection) #on récupère DF de la BDD pour avoir le bon id
    list_res=df_res.to_dict(orient='records') # crée une liste de dico en clé  -> valeur 1ere col, en valeur -> valeur 2eme col

    dict_res={}  #on tranforme la liste de dico en dico valeur(k), id(v)
    for i in list_res:
        dict_res[i[table_name+'_nom']]=i[table_name+'_id']
    return dict_res


#a partir d'un dictionnaire de noms et d'une serie, renvoie cette serie sans les noms existants deja dans le dictionnaire
def drop_existing_name(dico, df) :
    res=df
    for i in res.index:
        if res[i] in dico :
            res=res.drop(index=i)
    return res

#transforme les dates format excel au format SQL
def excel_to_sql_date(date):
    date=re.sub(r"/","-",date) #on transforme les "/" en "-"
    date=re.sub(r"(\d\d)-(\d\d)-(\d{4})",r"\3-\2-\1",date) #on inverse les jours et les mois
    return date


#Main
conn= create_engine('mysql+mysqlconnector://root:root@localhost:3306/eviesens')

filepaths=os.listdir("./donnees/fiches_mensuelles/") #récupère liste des noms des fichiers dans le dossier "fiches_mensuelles"

for i in range(len(filepaths)) :
    filepaths[i]="./donnees/fiches_mensuelles/"+filepaths[i] #on récupère liste des filepath de chaque fiche mensuelle

for filepath in filepaths :
    df=pd.read_csv(filepath)
    df_commande=select_commmande(df) #on récupère un dataframe par mois avec les colonnes et les lignes qui nous intéressent
    
    #dictionnaires des structures / transactions / moyens de paiement avant l'insertion
    before_dico_type_structure_db=database_to_dict("type_structure",conn)
    before_dico_type_transaction_db=database_to_dict("type_transaction",conn)
    before_dico_moyen_paiement_db=database_to_dict("moyen_paiement",conn)


    # recupere la liste des structures et envoie les nouvelles structures en bdd
    df_type_structure=df_commande['type_structure_nom'].drop_duplicates() #on supprime doublons -> Nan
    df_type_structure=df_type_structure.dropna() #on supprime Nan
    df_type_structure=drop_existing_name(before_dico_type_structure_db, df_type_structure) #supprime les noms deja existants en bdd
    df_to_database(df_type_structure,"type_structure",conn)

    # recupere la liste des transactions et envoie les nouvelles transactions en bdd
    df_type_transaction=df_commande['type_transaction_nom'].drop_duplicates() #on supprime doublons -> Nan
    df_type_transaction=df_type_transaction.dropna() #on supprime Nan
    df_type_transaction=drop_existing_name(before_dico_type_transaction_db, df_type_transaction) #supprime les noms deja existants en bdd
    df_to_database(df_type_transaction,"type_transaction",conn)

    # recupere la liste des moyens de paiement et envoie les nouveaux moyens de paiement en bdd
    df_moyen_paiement=df_commande['moyen_paiement_nom'].drop_duplicates() #on supprime doublons -> Nan
    df_moyen_paiement=df_moyen_paiement.dropna() #on supprime Nan
    df_moyen_paiement=drop_existing_name(before_dico_moyen_paiement_db, df_moyen_paiement) #supprime les noms deja existants en bdd
    df_to_database(df_moyen_paiement,"moyen_paiement",conn)



    #dictionnaires des structures / transactions / moyens de paiement apres l'insertion
    after_dico_type_structure_db=database_to_dict("type_structure",conn)
    after_dico_type_transaction_db=database_to_dict("type_transaction",conn)
    after_dico_moyen_paiement_db=database_to_dict("moyen_paiement",conn)


    #transforme les noms de structures / transactions / moyens de paiement en leur id associe
    df_commande= df_commande.replace(after_dico_type_structure_db) #on transforme noms de structures en leur id
    df_commande= df_commande.replace(after_dico_type_transaction_db) #on transforme les noms de transactions en leur id
    df_commande= df_commande.replace(after_dico_moyen_paiement_db) #on transforme les noms de moyen de paiement en leur id

    #on change les nom de col pour correspondre à la table de la BDD
    df_commande = df_commande.rename(columns={'type_structure_nom': 'type_structure_id', 'type_transaction_nom': 'type_transaction_id','moyen_paiement_nom':'moyen_paiement_id'})

    #on normalise les dates
    df_commande['commande_date_achat']=df_commande['commande_date_achat'].transform(lambda x: excel_to_sql_date(x)) #on change "/" en "-" et on inverse jours et ans


    df_commande["client_id"]=1 #il faut un champs non nul, on en place un arbitraire

    # insert le tableau activite dans la bdd
    df_to_database(df_commande,"commande",conn)