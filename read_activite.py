import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
import re
pd.set_option('display.max_rows', 500)


# activite
#a partir du fichier csv, selectionne les colonnes et les lignes correspondant au tableau activite
def select_activite(df) :
    df_activite = df.iloc[:,24:28] # selectionne toutes les lignes des colonnes Y a AB d'excel (colonnes 25 a 28 du csv)
    df_activite=df_activite.dropna() #supprime Nan
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
    res=df
    for i in res.index:
        if res[i] in dico :
            res=res.drop(index=i)
    return res

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

def add_type_act(df_to_add, connection) :
    df_res=df_to_add
    df_from_db = pd.read_sql_query('SELECT type_activite_nom,activite_nom FROM type_activite', connection)
    df_res=df_res[["type_activite_nom","activite_nom"]]
    df_res = df_res.astype({"type_activite_nom" : str, "activite_nom" : str})
    add_type_act=True #un client est par defaut inconnu et doit etre ajoute a la bdd
    for i in df_res.index :
        for j in df_from_db.index :
            # si le nom et prenom en fiche = le nom et prenom en bdd :
            if equal_or_both_null(df_res.loc[i,"type_activite_nom"], df_from_db.loc[j,"type_activite_nom"]) & equal_or_both_null(df_res.loc[i,"activite_nom"], df_from_db.loc[j,"activite_nom"]):
                add_type_act=False # si le client est deja connu, on ne l'ajoute pas
        if add_type_act :
            type_act_to_add=df_res.loc[[i]] #on recupere la ligne de la fiche a rajouter
            type_act_to_add.to_sql("type_activite", con=connection, index=False, if_exists='append') # on ajoute le client a la bdd
            df_from_db = pd.concat([df_from_db, type_act_to_add], ignore_index=True) # ajoute le nouveau client au df local
        add_type_act=True # on reinitialise la variable pour le prochain client



#Main
conn= create_engine('mysql+mysqlconnector://root:root@localhost:3306/eviesens')

filepaths=os.listdir("./donnees/fiches_mensuelles/")

for i in range(len(filepaths)) :
    filepaths[i]="./donnees/fiches_mensuelles/"+filepaths[i]

for filepath in filepaths :
    df=pd.read_csv(filepath)
    df_activite=select_activite(df)
    
    # table vendeur
    before_dico_vendeur_db=database_to_dict("vendeur",conn)#dictionnaires des vendeurs avant l'insertion
    df_vendeur=df_activite['vendeur_nom'].drop_duplicates() #on supprime doublons -> Nan
    df_vendeur=df_vendeur.dropna() #on supprime Nan
    df_vendeur=drop_existing_name(before_dico_vendeur_db, df_vendeur) #supprime les noms deja existants en bdd
    df_to_database(df_vendeur,"vendeur",conn)
    after_dico_vendeur_db=database_to_dict("vendeur",conn) #dictionnaires des vendeurs / types activites apres l'insertion
    df_activite= df_activite.replace(after_dico_vendeur_db) #on transforme noms de vendeur en leur id
    
    #table type_activite
    add_type_act(df_activite,conn)
    after_dico_type_act_db=database_to_dict("type_activite",conn) #dictionnaires des vendeurs / types activites apres l'insertion
    df_activite= df_activite.replace(after_dico_type_act_db)



    #on change les nom de col pour correspondre à la table de la BDD
    df_activite = df_activite.rename(columns={'vendeur_nom': 'vendeur_id','type_activite_nom':'type_activite_id'})
    df_activite = df_activite.drop('activite_nom',axis=1)

    #ajoute la colonne mois correspondante au mois trouve dans le nom de fichier
    filename=os.path.basename(filepath)
    mois, annee = str_to_month_year(filename)
    df_activite["activite_mois"]=annee+"-"+mois+"-01" #YYYY/MM/dd

    # insert le tableau activite dans la bdd
    df_to_database(df_activite,"activite",conn)