import pandas as pd
import numpy as np
import os
import sys
from sqlalchemy import create_engine
import re
pd.set_option('display.max_rows', 500)

# commande
def select_commande(df) :
    df_commande=df.copy()
    df_commande = df_commande.iloc[:49,:21] # selectionne uniquement les lignes jusque la ligne 50 du csv et 21 premieres colonnes
    df_commande = df_commande[df_commande['Structure'].notna()]
    df_commande = df_commande[df_commande['Type'].notna()]
    df_commande = df_commande[df_commande['Vendeur'].notna()]
    df_commande = df_commande[df_commande['Intitulé'].notna()]
    df_commande = df_commande[df_commande['Transaction'].notna()]
    df_commande = df_commande[df_commande['Moyen de paiement'].notna()]
    df_commande = df_commande[df_commande['Déplacement'].notna()]
    df_commande = df_commande[df_commande['Quantité'].notna()]
    df_commande = df_commande[df_commande['Reduction'].notna()]
    df_commande = df_commande[df_commande['Commission'].notna()]
    df_commande = df_commande[df_commande['RSI'].notna()]
    df_commande = df_commande[df_commande["Date d'achat"].notna()]

    df_commande=df_commande.astype({'Déplacement': 'string', 'Tarif': 'string', 'Reduction': 'string', 'Commission': 'string', 'RSI': 'string'})
    df_commande["Déplacement"]=df_commande["Déplacement"].replace(regex='[^,.0-9]', value=np.nan)
    df_commande["Déplacement"]=df_commande["Déplacement"].str.replace(',', '.', regex=True) # remplace tout ce qui n'est pas un chiffre, un . ou une , par Nan

    df_commande["Tarif"]=df_commande["Tarif"].replace(regex='[^,.0-9]', value=np.nan)
    df_commande["Tarif"]=df_commande["Tarif"].str.replace(',', '.', regex=True)

    df_commande["Reduction"]=df_commande["Reduction"].replace(regex='[^,.0-9]', value=np.nan)
    df_commande["Reduction"]=df_commande["Reduction"].str.replace(',', '.', regex=True)
    
    df_commande["Commission"]=df_commande["Commission"].replace(regex='[^,.0-9]', value=np.nan)
    df_commande["Commission"]=df_commande["Commission"].str.replace(',', '.', regex=True)

    df_commande["RSI"]=df_commande["RSI"].replace(regex='[^,.0-9]', value=np.nan)
    df_commande["RSI"]=df_commande["RSI"].str.replace(',', '.', regex=True)
    #on selectionne les colonnes utiles pour nous par leur nom dans le DF    df_commande = df_commande.rename(columns={'Structure': 'type_structure_nom', 'Transaction': 'type_transaction_nom', 'Moyen de paiement':'moyen_paiement_nom', "Nom":'client_nom', "Prénom":'client_prenom',"Date d'achat":'commande_date_achat'})
    df_commande = df_commande[['Date soin', 'Nom' ,'Prénom', 'Type', 'Vendeur', 'Intitulé','Déplacement', 'Quantité', 'Tarif', 
                               'Reduction', 'Commission', 'RSI', "Date d'achat", 'Date Encaissement ', 'Date perception', 'Date remboursement']]
    
    #on change nom col pour correspondre au nom dans les tables type_structure, type_transaction, moyen_paiement et commande (pour commande_date_achat et client_id)
    df_commande = df_commande.rename(columns={'Date soin': 'commande_date_soin', 'Nom': 'client_nom',
                                              'Prénom': 'client_prenom', 'Type': 'type_activite_nom', 'Vendeur': 'vendeur_nom',
                                              'Intitulé': 'activite_nom', 'Déplacement': 'commande_deplacement',
                                              'Quantité': 'commande_quantite', 'Reduction': 'commande_reduction', 'Commission' : 'commande_commission',
                                              'RSI' : 'commande_rsi', 'Tarif' : "activite_prix",
                                              "Date d'achat": 'commande_date_achat', 'Date Encaissement ': 'commande_date_encaissement',
                                              'Date perception': 'commande_date_perception', 'Date remboursement': 'commande_date_remboursement'})
    return df_commande

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

#transforme les dates format excel au format SQL et si Nan retourne Nan
def excel_to_sql_date(date):
    if pd.isnull(date):
        return date
    else:
        date=re.sub(r"/","-",date) #on transforme les "/" en "-"
        date=re.sub(r"(\d\d)-(\d\d)-(\d{4})",r"\3-\2-\1",date) #on inverse les jours et les mois
        return date
    
def get_type_intervention_id(connection) :
    df = pd.read_sql_query('SELECT type_activite_id, type_activite_nom, activite_nom FROM type_activite', connection)
    for i in df.index :
        if ((df.loc[i, "activite_nom"])=="Intervention Extérieure sur devis") & ((df.loc[i, "type_activite_nom"])=="Prestation") :
            return(df.loc[i, "type_activite_id"])
        
def get_vendeur_intervention_id(connection) :
    df = pd.read_sql_query('SELECT vendeur_id, vendeur_nom FROM vendeur', connection)
    for i in df.index :
        if (df.loc[i, "vendeur_nom"])=="Eviesens" :
            return(df.loc[i, "vendeur_id"])

# remplace les colonnes client_nom et client_prenom par client_id
def get_client_id(df_commande, connection) :
    pd.options.mode.chained_assignment = None
    df_res=df_commande.copy()
    df_from_db = pd.read_sql_query('SELECT client_id, client_nom, client_prenom FROM client', connection)
    df_res["client_id"]=np.nan
    def same_line(i, j, dfcom, dfdb) :
        nom = dfcom.loc[i,"client_nom"]
        nom_db = dfdb.loc[j,"client_nom"]
        prenom = dfcom.loc[i,"client_prenom"]
        prenom_db = dfdb.loc[j,"client_prenom"]
        if ( (equal_or_both_null(nom,nom_db)) & (equal_or_both_null(prenom,prenom_db)) ) :
            return True
        return False
    for i in df_res.index :
        for j in df_from_db.index :
            if same_line(i, j, df_res, df_from_db) :
                df_res.loc[i, "client_id"]=df_from_db.loc[j, "client_id"] # quand un couple nom/prenom est trouve dans la bdd, son id lui est associe
    pd.options.mode.chained_assignment = "warn"
    df_res=df_res.drop(["client_nom", "client_prenom"], axis=1)
    return df_res

# ajoute l'id de la commande a partir de l'id client et de la date d'achat de la commande
def get_commande_id(df_commande, connection) :
    df_res=df_commande.copy()
    df_from_db = pd.read_sql_query('SELECT commande_id, commande_date_achat, client_id, moyen_paiement_id, type_transaction_id, type_structure_id FROM commande', connection)
    df_res["commande_id"]=np.nan
    def same_line(i, j, dfcom, dfdb) :
        c = str(dfcom.loc[i,"commande_date_achat"])
        c_db = str(dfdb.loc[j,"commande_date_achat"])
        cli = float(dfcom.loc[i,"client_id"])
        cli_db = float(dfdb.loc[j,"client_id"])
        if ( (c==c_db) & (cli==cli_db) ) :
            return True
        return False
    for i in df_res.index :
        for j in df_from_db.index :
            if same_line(i, j, df_res, df_from_db) :
                df_res.loc[i, "commande_id"] = df_from_db.loc[j, "commande_id"]
    df_res=df_res.drop(["client_id"], axis=1)
    return df_res

# remplace le nom du vendeur par son id associe
def get_vendeur_id(df_commande, connection) :
    df_res=df_commande.copy()
    df_from_db = pd.read_sql_query('SELECT vendeur_id, vendeur_nom FROM vendeur', connection)
    df_res["vendeur_id"]=np.nan
    for i in df_res.index :
        for j in df_from_db.index :
            if(equal_or_both_null(df_res.loc[i, "vendeur_nom"], df_from_db.loc[j, "vendeur_nom"])) :
                df_res.loc[i, "vendeur_id"]=df_from_db.loc[j, "vendeur_id"] # quand un couple nom/prenom est trouve dans la bdd, son id lui est associe
    pd.options.mode.chained_assignment = "warn"
    df_res=df_res.drop(["vendeur_nom"], axis=1)
    return df_res

# remplace les colonnes type_activite_nom et activite_nom par l'id type_activite associe
def get_type_activite_id(df_commande, connection) :
    pd.options.mode.chained_assignment = None
    df_res=df_commande.copy()
    df_from_db = pd.read_sql_query('SELECT type_activite_id, type_activite_nom, activite_nom FROM type_activite', connection)
    df_res["type_activite_id"]=np.nan
    def same_line(i, j, dfcom, dfdb) :
        type_a_nom = str(dfcom.loc[i,"type_activite_nom"]).lower()
        type_a_nom_db = str(dfdb.loc[j,"type_activite_nom"]).lower()
        a_nom = str(dfcom.loc[i,"activite_nom"]).lower()
        a_nom_db = str(dfdb.loc[j,"activite_nom"]).lower()
        if (type_a_nom==type_a_nom_db) & (a_nom==a_nom_db) :
            return True
        return False
    for i in df_res.index :
        for j in df_from_db.index :
            if same_line(i, j, df_res, df_from_db) :
                df_res.loc[i, "type_activite_id"] = df_from_db.loc[j, "type_activite_id"]
    pd.options.mode.chained_assignment = "warn"
    df_res=df_res.drop(["type_activite_nom", "activite_nom"], axis=1)
    return df_res

# cree une intervention a partir d'une commande et de l'id general du vendeur (Eviesens) et de l'id du type correspondant (intervention exterieure, Prestation)
def create_intervention(i, df_commande, id_vendeur_intervention, id_type_intervention, conn) :
    line_to_add=df_commande.copy()
    line_to_add=line_to_add.loc[i, ["activite_prix", "commande_date_achat"]]
    mois=line_to_add["commande_date_achat"][5:7]
    annee=line_to_add["commande_date_achat"][0:4]
    line_to_add["activite_mois"]=annee+"-"+mois+"-"+"01"
    line_to_add["activite_prix"]=re.sub(r",", ".", str(line_to_add["activite_prix"]))
    d = {
            'activite_prix': [line_to_add["activite_prix"]],
            'activite_mois': [line_to_add["activite_mois"]],
            'vendeur_id': [id_vendeur_intervention],
            'type_activite_id': [id_type_intervention]
    }
    line_to_add=pd.DataFrame(d)
    line_to_add.to_sql("activite", con=conn, index=False, if_exists='append') # on ajoute la commande a la bdd

# recupere l'intervention correspondante au prix et mois de la commande, de l'id general du vendeur (Eviesens) et de l'id du type correspondant (intervention exterieure, Prestation)
def get_intervention_id (i, df_commande, id_vendeur_intervention, id_type_intervention, connection) :
    activites = pd.read_sql_query('SELECT activite_id, activite_prix, activite_mois, type_activite_id, vendeur_id FROM activite', connection)
    def same_line(i, j, dfcom, dfdb, id_vendeur_intervention, id_type_intervention) :
        type=dfcom.loc[i,"type_activite_id"]
        vendeur=dfcom.loc[i,"vendeur_id"]
        prix=float(dfcom.loc[i,"activite_prix"])
        prix_db=float(dfdb.loc[j, "activite_prix"])
        mois_annee = str(dfcom.loc[i,"commande_date_achat"])
        mois=mois_annee[5:7]
        annee=mois_annee[0:4]
        mois_annee_db = str(dfdb.loc[j, "activite_mois"])
        mois_db=mois_annee_db[5:7]
        annee_db=mois_annee_db[0:4]
        if (type==id_type_intervention) & (vendeur==id_vendeur_intervention) & (prix==prix_db) & (mois==mois_db) & (annee==annee_db):
            return True
        return False
    for j in activites.index :
        if(same_line(i, j, df_commande, activites, id_vendeur_intervention ,id_type_intervention)) :
            return activites.loc[j, "activite_id"]

# recupere l'id de l'activite a partir de l'id du type d'activite, de l'id du vendeur et de la date d'achat (mois et annee)
# cree une nouvelle intervention exterieure si il s'agit d'un devis
def get_activite_id(df_commande, id_type_intervention, id_vendeur_intervention, connection) :
    pd.options.mode.chained_assignment = None
    df_res=df_commande.copy()
    df_from_db = pd.read_sql_query('SELECT activite_id, activite_mois, type_activite_id, vendeur_id FROM activite', connection)
    df_res["activite_id"]=np.nan

    def same_line(i, j, dfcom, dfdb) :
        type_a_id = float(dfcom.loc[i,"type_activite_id"])
        type_a_id_db = float(dfdb.loc[j,"type_activite_id"])
        vendeur_id = float(dfcom.loc[i,"vendeur_id"])
        vendeur_id_db = float(dfdb.loc[j,"vendeur_id"])
        mois_annee = str(dfcom.loc[i,"commande_date_achat"])
        mois=mois_annee[5:7]
        annee=mois_annee[0:4]
        mois_annee_db = str(dfdb.loc[j, "activite_mois"])
        mois_db=mois_annee_db[5:7]
        annee_db=mois_annee_db[0:4]
        if (type_a_id==type_a_id_db) & (vendeur_id==vendeur_id_db) & (mois==mois_db) & (annee==annee_db):
            return True
        return False
    for i in df_res.index :
        # si la ligne lue correspond a une intervention (prix a fixer) :
        if ((df_res.loc[i,"type_activite_id"]==float(id_type_intervention))
        & (df_res.loc[i,"vendeur_id"]==float(id_vendeur_intervention)) ) :
            create_intervention(i, df_res, id_vendeur_intervention, id_type_intervention, connection) # on cree une intervention
            intervention_id=get_intervention_id(i, df_res, id_vendeur_intervention, id_type_intervention, connection) # on recupere l'id de cette intervention
            df_res.loc[i, "activite_id"] = intervention_id # on associe a la commande l'id de l'activite de l'intervention
            continue # on passe a la ligne suivante
        for j in df_from_db.index :
            if same_line(i, j, df_res, df_from_db) :
                df_res.loc[i, "activite_id"] = df_from_db.loc[j, "activite_id"]
    pd.options.mode.chained_assignment = "warn"
    df_res=df_res.drop(["type_activite_id", "vendeur_id", "commande_date_achat", "activite_prix"], axis=1)
    return df_res

def add_new_command_activite (df_to_add, connection) :
    df_res = df_to_add.copy() # liste des commandes a rajouter en cours
    for i in df_res.index :
        command_activite_to_add=df_res.loc[[i]] #on recupere la ligne de la commande a rajouter
        command_activite_to_add.to_sql("commande_activite", con=connection, index=False, if_exists='append') # on ajoute la commande a la bdd
    return df_res

#Main
conn = create_engine('sqlite:///eviesens.db')

id_type_intervention=get_type_intervention_id(conn)
id_vendeur_intervention=get_vendeur_intervention_id(conn)

folder_filepath=sys.argv[1]
filepaths_list=[]
filepaths=[]
if os.path.isdir(folder_filepath) :
    filepaths_list=os.listdir(folder_filepath)
    for i in range(len(filepaths_list)) :
        if os.path.isfile(folder_filepath+"/"+filepaths_list[i]) :
            filepaths.append(folder_filepath+"/"+filepaths_list[i])
elif os.path.isfile(folder_filepath) :
    filepaths=[folder_filepath]

for filepath in filepaths :
    print(filepath)
    df=pd.read_csv(filepath)
    df_commande=select_commande(df)
    df_commande['commande_date_soin'] = df_commande['commande_date_soin'].transform(lambda x: excel_to_sql_date(x))
    df_commande['commande_date_achat'] = df_commande['commande_date_achat'].transform(lambda x: excel_to_sql_date(x))
    df_commande['commande_date_encaissement'] = df_commande['commande_date_encaissement'].transform(lambda x: excel_to_sql_date(x))
    df_commande['commande_date_perception'] = df_commande['commande_date_perception'].transform(lambda x: excel_to_sql_date(x))
    df_commande['commande_date_remboursement'] = df_commande['commande_date_remboursement'].transform(lambda x: excel_to_sql_date(x))

    df_commande=get_client_id(df_commande, conn) # recupere l'id des clients 
    df_commande=get_commande_id(df_commande, conn) # recupere l'id des commandes

    df_commande=get_vendeur_id(df_commande, conn) # recupere l'id des vendeurs
    df_commande=get_type_activite_id(df_commande, conn) # recupere l'id du type d'activite
    df_commande=get_activite_id(df_commande, id_type_intervention, id_vendeur_intervention, conn) # recupere l'id de l'activite, cree une intervention exterieure si besoin

    add_new_command_activite(df_commande,conn) # ajoute les lignes dans la bdd
