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

#a partir d'un filepath, renvoie le numero de mois et l'annee correspondante
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





#Main
conn= create_engine('mysql+mysqlconnector://root:root@localhost:3306/eviesens')

filepaths=os.listdir("./donnees/fiches_mensuelles/")

for i in range(len(filepaths)) :
    filepaths[i]="./donnees/fiches_mensuelles/"+filepaths[i]

for filepath in filepaths :
    df=pd.read_csv(filepath)
    df_activite=select_activite(df)
    
    #dictionnaires des vendeurs / types activites avant l'insertion
    before_dico_type_activite_db=database_to_dict("type_activite",conn)
    before_dico_vendeur_db=database_to_dict("vendeur",conn)


    # recupere la liste des activites et envoie les nouvelles activites en bdd
    df_type_activite=df_activite['type_activite_nom'].drop_duplicates() #on supprime doublons -> Nan
    df_type_activite=df_type_activite.dropna() #on supprime Nan
    df_type_activite=drop_existing_name(before_dico_type_activite_db, df_type_activite) #supprime les noms deja existants en bdd
    df_to_database(df_type_activite,"type_activite",conn)

    # recupere la liste des vendeurs et envoie les nouveaux vendeurs en bdd
    df_vendeur=df_activite['vendeur_nom'].drop_duplicates() #on supprime doublons -> Nan
    df_vendeur=df_vendeur.dropna() #on supprime Nan
    df_vendeur=drop_existing_name(before_dico_vendeur_db, df_vendeur) #supprime les noms deja existants en bdd
    df_to_database(df_vendeur,"vendeur",conn)



    #dictionnaires des vendeurs / types activites apres l'insertion
    after_dico_vendeur_db=database_to_dict("vendeur",conn)
    after_dico_type_activite_db=database_to_dict("type_activite",conn)


    #transforme les noms de vendeur et type_activite en leur id associe
    df_activite= df_activite.replace(after_dico_vendeur_db) #on transforme noms de vendeur en leur id
    df_activite= df_activite.replace(after_dico_type_activite_db) #on transforme les noms de type_activite en leur id

    #on change les nom de col pour correspondre à la table de la BDD
    df_activite = df_activite.rename(columns={'vendeur_nom': 'vendeur_id', 'activite_nom': 'activite_nom','type_activite_nom':'type_activite_id'}) 

    #ajoute la colonne mois correspondante au mois trouve dans le nom de fichier
    filename=os.path.basename(filepath)
    mois, annee = str_to_month_year(filename)
    df_activite["activite_mois"]=annee+"-"+mois+"-01" #YYYY/MM/dd

    #insert le tableau activite dans la bdd
    df_to_database(df_activite,"activite",conn)




