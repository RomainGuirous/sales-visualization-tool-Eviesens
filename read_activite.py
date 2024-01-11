import pandas as pd
import numpy as np
import os
import sys
from sqlalchemy import create_engine
import re
import sqlite3
pd.set_option('display.max_rows', 500)


# activite
#a partir du fichier csv, selectionne les colonnes et les lignes correspondant au tableau activite
def select_activite(df) :
    df_activite=df.copy()
    df_activite = df_activite.iloc[:,24:28] # selectionne toutes les lignes des colonnes Y a AB d'excel (colonnes 25 a 28 du csv)
    df_activite=df_activite.dropna() #supprime Nan
    df_activite=df_activite.astype({'Prix': 'string'})
    df_activite["Prix"]=df_activite["Prix"].replace(regex='[^,.0-9]', value=np.nan) # remplace tout ce qui n'est pas un chiffre, un . ou une , par Nan
    df_activite["Prix"]=df_activite["Prix"].str.replace(',', '.', regex=True) # remplace les , par des . dans la colonne Prix
    df_activite = df_activite.rename(columns={'Vendeur.1': 'vendeur_nom', 'Intitulé.1': 'activite_nom', 'Prix':'activite_prix', 'Type.1':'type_activite_nom'}) #on change nom col
    return df_activite

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


def str_to_month_year(s) :
    l_mois={
    "janvier" : "01","fevrier" : "02", "février" : "02", "mars" : "03","avril" : "04","mai" : "05","juin" : "06",
    "juillet" : "07","aout" : "08", "août" : "08", "septembre" : "09", "octobre" : "10", "novembre" : "11", "decembre" : "12", "décembre" : "12"
    }
    low_s=s.lower()
    mois=re.search(r"(janvier|f(e|é)vrier|mars|avril|mai|juin|juillet|ao(u|û)t|septembre|octobre|novembre|d(e|é)cembre)", low_s).group()
    n_mois=l_mois[mois]
    year=re.search(r"[0-9]{4}", low_s).group()
    return n_mois,year

#a partir d'un dictionnaire de noms et d'une serie, renvoie cette serie sans les noms existants deja dans le dictionnaire
def drop_existing_name(dico, df) :
    res=df.copy()
    for i in res.index:
        if res[i] in dico :
            res=res.drop(index=i)
    return res

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

def is_valid_filename(filename) :
    contains_month=bool(re.search(r"(janvier|f(e|é)vrier|mars|avril|mai|juin|juillet|ao(u|û)t|septembre|octobre|novembre|d(e|é)cembre)", filename.lower()))
    contains_year=bool(re.search(r"[0-9]{4}", filename))
    if contains_month & contains_year :
        return True
    return False

def add_type_act(df_to_add, connection) :
    df_res=df_to_add.copy()
    df_from_db = pd.read_sql_query('SELECT type_activite_id, type_activite_nom, activite_nom FROM type_activite', connection)
    df_res=df_res[["type_activite_nom","activite_nom"]]
    add_type_act=True #un client est par defaut inconnu et doit etre ajoute a la bdd
    for i in df_res.index :
        for j in df_from_db.index :
            # si le nom et type_nom_activite en fiche = les memes bdd :
            if equal_or_both_null(df_res.loc[i,"type_activite_nom"], df_from_db.loc[j,"type_activite_nom"]) & equal_or_both_null(df_res.loc[i,"activite_nom"], df_from_db.loc[j,"activite_nom"]):
                add_type_act=False # si le le type_act est deja connu, on ne l'ajoute pas
        if add_type_act :
            type_act_to_add=df_res.loc[[i]] #on recupere la ligne de la fiche a rajouter
            type_act_to_add.to_sql("type_activite", con=connection, index=False, if_exists='append') # on ajoute le type_act a la bdd
            df_from_db = pd.concat([df_from_db if not df_from_db.empty else None, type_act_to_add if not type_act_to_add.empty else None], ignore_index=True) # ajoute le nouveau type_act au df local
        add_type_act=True # on reinitialise la variable pour le prochain type_act

def get_type_act_id(df_to_get, connection) :
    pd.options.mode.chained_assignment = None
    df_res=df_to_get.copy()
    df_from_db = pd.read_sql_query('SELECT type_activite_id, type_activite_nom, activite_nom FROM type_activite', connection) # recupere la liste des clients de la bdd
    df_res["type_activite_id"]=np.nan # initialise tous les id a null
    for i in df_res.index :
        for j in df_from_db.index :
            if equal_or_both_null(df_res.loc[i,"type_activite_nom"], df_from_db.loc[j,"type_activite_nom"]) & equal_or_both_null(df_res.loc[i,"activite_nom"], df_from_db.loc[j,"activite_nom"]):
                df_res.loc[i, "type_activite_id"]=df_from_db.loc[j, "type_activite_id"] # quand un couple nom/prenom est trouve dans la bdd, son id lui est associe
    pd.options.mode.chained_assignment = "warn"
    return df_res

def add_new_activite(df_to_add, connection) :
    df_res = df_to_add.copy() # liste des activites a rajouter en cours
    df_from_db = pd.read_sql_query('SELECT activite_mois, type_activite_id, vendeur_id FROM activite', connection)
    add_activite=True # une activite est ajoutee par defaut

    # compare chacun des champs des deux dataframe entre eux (activite_mois, type_activite_id, vendeur_id) et renvoie True
    # si les 5 champs correspondent entre eux
    def same_line(i, j, dfcom, dfdb) :
        type_a_id = float(dfcom.loc[i,"type_activite_id"])
        type_a_id_db = float(dfdb.loc[j,"type_activite_id"])
        vendeur_id = float(dfcom.loc[i,"vendeur_id"])
        vendeur_id_db = float(dfdb.loc[j,"vendeur_id"])
        mois_annee = str(dfcom.loc[i,"activite_mois"])
        mois=mois_annee[5:7]
        annee=mois_annee[0:4]
        mois_annee_db = str(dfdb.loc[j, "activite_mois"])
        mois_db=mois_annee_db[5:7]
        annee_db=mois_annee_db[0:4]
        if (type_a_id==type_a_id_db) & (vendeur_id==vendeur_id_db) & (mois==mois_db) & (annee==annee_db):
            return True
        return False
    add_activite=True
    for i in df_res.index :
        for j in df_from_db.index :
            if same_line(i, j, df_res, df_from_db) :
                add_activite=False
        if add_activite :
            activite_to_add=df_res.loc[[i]] #on recupere la ligne de l'activite a rajouter
            activite_to_add.to_sql("activite", con=connection, index=False, if_exists='append') # on ajoute l'activite a la bdd
            df_from_db = pd.concat([df_from_db if not df_from_db.empty else None, activite_to_add if not activite_to_add.empty else None], ignore_index=True) # ajoute la nouvelle activite au df local
        add_activite=True # on reinitialise la variable pour la prochaine activite
    return df_res

#Main
conn = create_engine('sqlite:///eviesens.db')

folder_filepath=sys.argv[1]
filepaths_list=[]
filepaths=[]
if os.path.isdir(folder_filepath) :
    filepaths_list=os.listdir(folder_filepath)
    for i in range(len(filepaths_list)) :
        if os.path.isfile(folder_filepath+"/"+filepaths_list[i]) & is_valid_filename(filepaths_list[i]) : # ne lit que les fichiers contenant un mois et une annee
            filepaths.append(folder_filepath+"/"+filepaths_list[i])
elif os.path.isfile(folder_filepath) :
    filepaths=[folder_filepath]

for filepath in filepaths :
    print(filepath)
    df=pd.read_csv(filepath)
    df_activite=select_activite(df)
    
    # table vendeur
    before_dico_vendeur_db=database_to_dict("vendeur",conn)#dictionnaires des vendeurs avant l'insertion
    df_vendeur=df_activite['vendeur_nom'].drop_duplicates() #on supprime doublons -> Nan
    df_vendeur=df_vendeur.dropna() #on supprime Nan
    df_vendeur=drop_existing_name(before_dico_vendeur_db, df_vendeur) #supprime les noms deja existants en bdd
    df_to_database(df_vendeur,"vendeur",conn)
    after_dico_vendeur_db=database_to_dict("vendeur",conn) #dictionnaires des vendeurs / types activites apres l'insertion
    df_activite["vendeur_nom"]= df_activite["vendeur_nom"].replace(after_dico_vendeur_db) #on transforme noms de vendeur en leur id
    
    #table type_activite
    add_type_act(df_activite, conn)
    df_activite=get_type_act_id(df_activite, conn)
    df_activite=df_activite.drop(["type_activite_nom", "activite_nom"], axis=1)



    #on change les nom de col pour correspondre à la table de la BDD
    df_activite = df_activite.rename(columns={'vendeur_nom': 'vendeur_id','type_activite_nom':'type_activite_id'})

    #ajoute la colonne mois correspondante au mois trouve dans le nom de fichier
    filename=os.path.basename(filepath)
    mois, annee = str_to_month_year(filename)
    df_activite["activite_mois"]=annee+"-"+mois+"-01" #YYYY/MM/dd

    # insert le tableau activite dans la bdd
    add_new_activite(df_activite, conn)