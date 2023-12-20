import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
import re
pd.set_option('display.max_rows', 500)

# commande
def select_commande(df) :
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
    df_commande = df_commande[['Structure', 'Transaction', 'Moyen de paiement', 'Nom', "Prénom", "Date d'achat"]] #on selectionne les colonnes utiles pour nous par leur nom dans le DF
    df_commande = df_commande.rename(columns={'Structure': 'type_structure_nom', 'Transaction': 'type_transaction_nom', 'Moyen de paiement':'moyen_paiement_nom', "Nom":'client_nom', "Prénom":'client_prenom',"Date d'achat":'commande_date_achat'})
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
    if pd.isnull(date) :
        return np.nan
    date=re.sub(r"/","-",date) #on transforme les "/" en "-"
    date=re.sub(r"(\d\d)-(\d\d)-(\d{4})",r"\3-\2-\1",date) #on inverse les jours et les mois
    return date

# compare deux chaines de charactere, si ils sont egaux ou tous les deux nuls renvoie True, sinon renvoie False
def equal_or_both_null(s1, s2) :
    if pd.isnull(s1) & pd.isnull(s2) :
        return True
    if pd.isnull(s1) | pd.isnull(s2) :
        return False
    s1_2=str(s1)
    s2_2=str(s2)
    if s1_2.lower() == s2_2.lower() :
        return True
    return False

def add_new_clients(df_to_add, connection) :
    df_res=df_to_add
    df_from_db = pd.read_sql_query('SELECT client_id,client_nom,client_prenom FROM client', connection)
    df_res=df_res[["client_nom","client_prenom"]]
    add_client=True #un client est par defaut inconnu et doit etre ajoute a la bdd
    for i in df_res.index :
        for j in df_from_db.index :
            # si le nom et prenom en fiche = le nom et prenom en bdd :
            if equal_or_both_null(df_res.loc[i,"client_nom"], df_from_db.loc[j,"client_nom"]) & equal_or_both_null(df_res.loc[i,"client_prenom"], df_from_db.loc[j,"client_prenom"]):
                add_client=False # si le client est deja connu, on ne l'ajoute pas
        if add_client :
            client_to_add=df_res.loc[[i]] #on recupere la ligne de la fiche a rajouter
            client_to_add["client_mail"]=np.nan # on ajoute les colonnes client_mail et client_telephone pour correspondre au schema de la bdd
            client_to_add["client_telephone"]=np.nan
            client_to_add.to_sql("client", con=connection, index=False, if_exists='append') # on ajoute le client a la bdd
            df_from_db = pd.concat([df_from_db if not df_from_db.empty else None, client_to_add if not client_to_add.empty else None], ignore_index=True) # ajoute le nouveau client au df local
        add_client=True # on reinitialise la variable pour le prochain client

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


def add_new_commands(df_to_add, connection) :
    df_res = df_to_add # liste des commandes a rajouter en cours
    df_from_db = pd.read_sql_query('SELECT commande_date_achat, client_id, moyen_paiement_id, type_transaction_id, type_structure_id FROM commande', connection)
    add_command=True # une commande est ajoutee par defaut

    # compare chacun des champs des deux dataframe entre eux (commande_date_achat, client_id, moyen_paiement_id, type_transaction_id et type_structure_id) en renvoie True
    # si les 5 champs correspondent entre eux
    def same_line(i, j, dfcom, dfdb) :
        c = str(dfcom.loc[i,"commande_date_achat"])
        c_db = str(dfdb.loc[j,"commande_date_achat"])
        cli = float(dfcom.loc[i,"client_id"])
        cli_db = float(dfdb.loc[j,"client_id"])
        mp = str(dfcom.loc[i,"moyen_paiement_id"])
        mp_db = str(dfdb.loc[j,"moyen_paiement_id"])
        tt = str(dfcom.loc[i,"type_transaction_id"])
        tt_db = str(dfdb.loc[j,"type_transaction_id"])
        ts = str(dfcom.loc[i,"type_structure_id"])
        ts_db = str(dfdb.loc[j,"type_structure_id"])
        if ( (c==c_db) & (cli==cli_db) & (mp==mp_db) & (tt==tt_db) & (ts==ts_db) ) :
            return True
        return False
    for i in df_res.index :
        for j in df_from_db.index :
            if same_line(i, j, df_res, df_from_db) :
                add_command=False # si la commande existe deja, on ne l'ajoute pas
        if add_command :
            command_to_add=df_res.loc[[i]] #on recupere la ligne de la commande a rajouter
            command_to_add.to_sql("commande", con=connection, index=False, if_exists='append') # on ajoute la commande a la bdd
            df_from_db = pd.concat([df_from_db if not df_from_db.empty else None, command_to_add if not command_to_add.empty else None], ignore_index=True) # ajoute la nouvelle commande au df local
        add_command=True # on reinitialise la variable pour la prochaine commande
    return df_res


#Main
conn=create_engine('mysql+mysqlconnector://root:root@localhost:3306/eviesens')

filepaths=os.listdir("./donnees/fiches_mensuelles/") #récupère liste des noms des fichiers dans le dossier "fiches_mensuelles"

for i in range(len(filepaths)) :
    filepaths[i]="./donnees/fiches_mensuelles/"+filepaths[i] #on récupère liste des filepath de chaque fiche mensuelle

for filepath in filepaths :
    print(filepath)
    df=pd.read_csv(filepath)
    df_commande=select_commande(df) #on récupère un dataframe par mois avec les colonnes et les lignes qui nous intéressent
    
    # table type_structure
    before_dico_type_structure_db=database_to_dict("type_structure",conn) # recupere la liste des structures existantes en bdd
    df_type_structure=df_commande['type_structure_nom'].drop_duplicates() # suppression des doublons du fichier en cours
    df_type_structure=df_type_structure.dropna() #suppression des Nan
    df_type_structure=drop_existing_name(before_dico_type_structure_db, df_type_structure) # suppression des noms deja existants en bdd
    df_to_database(df_type_structure,"type_structure",conn) # insertion des nouveaux noms de structure en bdd
    after_dico_type_structure_db=database_to_dict("type_structure",conn) # dictionnaires des structures apres l'insertion
    df_commande["type_structure_nom"] = df_commande["type_structure_nom"].replace(after_dico_type_structure_db) # remplacement des noms de structure locals par leur id


    # meme chose avec la table type_transaction
    before_dico_type_transaction_db=database_to_dict("type_transaction",conn)
    df_type_transaction=df_commande['type_transaction_nom'].drop_duplicates() 
    df_type_transaction=df_type_transaction.dropna()
    df_type_transaction=drop_existing_name(before_dico_type_transaction_db, df_type_transaction)
    df_to_database(df_type_transaction,"type_transaction",conn)
    after_dico_type_transaction_db=database_to_dict("type_transaction",conn)
    df_commande["type_transaction_nom"] = df_commande["type_transaction_nom"].replace(after_dico_type_transaction_db)


    # meme chose avec la table moyen_paiement
    before_dico_moyen_paiement_db=database_to_dict("moyen_paiement",conn)
    df_moyen_paiement=df_commande['moyen_paiement_nom'].drop_duplicates()
    df_moyen_paiement=df_moyen_paiement.dropna()
    df_moyen_paiement=drop_existing_name(before_dico_moyen_paiement_db, df_moyen_paiement)
    df_to_database(df_moyen_paiement,"moyen_paiement",conn)
    after_dico_moyen_paiement_db=database_to_dict("moyen_paiement",conn)
    df_commande["moyen_paiement_nom"] = df_commande["moyen_paiement_nom"].replace(after_dico_moyen_paiement_db)


    # table clients
    add_new_clients(df_commande, conn) # ajoute les client du fichier qui n'existent pas dans la bdd
    df_from_db = pd.read_sql_query('SELECT client_id, client_nom, client_prenom FROM client', conn) # recupere la liste des clients de la bdd
    df_commande=get_clients_id(df_commande, df_from_db) # compare chaque client du fichier avec ceux de la bdd et leur associe leur id en rajoutant client_id au dataframe
    df_commande=df_commande.drop(["client_nom", "client_prenom"], axis=1) # retire les colonnes client_nom et client_prenom qui n'apparaissent pas en bdd


    # change les nom de col pour correspondre à la table de la BDD
    df_commande = df_commande.rename(columns={'type_structure_nom': 'type_structure_id', 'type_transaction_nom': 'type_transaction_id','moyen_paiement_nom':'moyen_paiement_id'})

    # change le format de dates pour correspondre aux normes sql : de JJ/MM/AAAA -> AAAA-MM-JJ
    df_commande['commande_date_achat']=df_commande['commande_date_achat'].transform(lambda x: excel_to_sql_date(x)) #on change "/" en "-" et on inverse jours et ans

    add_new_commands(df_commande, conn) # ajoute les commandes une par une dans la bdd