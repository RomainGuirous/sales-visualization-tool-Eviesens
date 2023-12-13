import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
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



#Main
mois={
    1 : "janvier", 2 : "fevrier", 3 : "mars", 4 : "avril", 5 : "mai", 6 : "juin",
    7 : "juillet", 8 : "aout", 9 : "septembre", 10 : "octobre", 11 : "novembre", 12 : "decembre",
}

conn= create_engine('mysql+mysqlconnector://root:root@localhost:3306/eviesens')

filepath="./donnees/Janvier-2023.csv"

filepaths=os.listdir("./donnees")
for i in range(len(filepaths)) :
    filepaths[i]="./donnees/"+filepaths[i]
print(filepaths)

for filepath in filepaths :

# activite
    df=pd.read_csv(filepath)
    df_activite=select_activite(df)


    #recupere
    dico_vendeur_db=database_to_dict("vendeur",conn)
    dico_type_activite_db=database_to_dict("type_activite",conn)

    # recupere la liste des activites et l'envoie en bdd
    df_type_activite=df_activite['type_activite_nom'].drop_duplicates() #on supprime doublons -> Nan
    df_type_activite=df_type_activite.dropna() #on supprime Nan
    df_to_database(df_type_activite,"type_activite",conn)

    # recupere la liste des vendeurs
    df_vendeur=df_activite['vendeur_nom'].drop_duplicates() #on supprime doublons -> Nan
    df_vendeur=df_vendeur.dropna() #on supprime Nan
    df_to_database(df_vendeur,"vendeur",conn)


    df_activite= df_activite.replace(dico_vendeur_db) #on transforme noms de vendeur en leur id
    df_activite= df_activite.replace(dico_type_activite_db) #on transforme les noms de type_activite en leur id
    df_activite = df_activite.rename(columns={'vendeur_nom': 'vendeur_id', 'activite_nom': 'activite_nom','type_activite_nom':'type_activite_id'}) #on change les nom de col pour correspondre à la table  de  la BDD
    df_activite["activite_mois"]="2023-01-01" #YYYY/MM/dd
    print(df_activite)

    df_to_database(df_activite,"activite",conn)