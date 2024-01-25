import pandas as pd
from sqlalchemy import create_engine
import sqlite3




def get_data() :
    #le dataframe de chaque table, extrait de la base de donnee
    conn = create_engine('sqlite:///eviesens.db')
    df_table_vendeur= pd.read_sql_query('SELECT * FROM vendeur',conn)
    df_table_type_transaction= pd.read_sql_query('SELECT * FROM type_transaction',conn)
    df_table_type_structure= pd.read_sql_query('SELECT * FROM type_structure',conn)
    df_table_moyen_paiement= pd.read_sql_query('SELECT * FROM moyen_paiement',conn)
    df_table_type_activite= pd.read_sql_query('SELECT * FROM type_activite',conn)
    df_table_commande= pd.read_sql_query('SELECT * FROM commande',conn)
    df_table_commande_activite= pd.read_sql_query('SELECT * FROM commande_activite',conn)
    df_table_activite= pd.read_sql_query('SELECT * FROM activite',conn)
    df_table_client= pd.read_sql_query('SELECT * FROM client',conn)


    # jointure des 4 tables type_activite ,commande, activite, commande_activite et vendeur(par la gauche pour garder affiché tous les noms d'activite)
    df_activite=df_table_type_activite.join(df_table_activite.set_index('type_activite_id'),on=('type_activite_id'), how="inner")
    df_activite=df_activite.join(df_table_commande_activite.set_index('activite_id'),on=('activite_id'), how="inner")
    df_activite=df_activite.join(df_table_commande.set_index('commande_id'),on=('commande_id'), how="inner")
    df_activite=df_activite.join(df_table_vendeur.set_index('vendeur_id'),on=('vendeur_id'),how='inner') # on transforme df_activite pour incorporer vendeur_nom
    df_activite=df_activite.join(df_table_type_transaction.set_index('type_transaction_id'),on=('type_transaction_id'), how="inner")
    df_activite=df_activite.join(df_table_client.set_index('client_id'),on=('client_id'), how="inner")

    #on transforme les prix des lignes Remboursement en négatif
    df_activite.loc[df_activite['type_transaction_nom'] == "Remboursement",'activite_prix']=df_activite[df_activite['type_transaction_nom'] == "Remboursement"]['activite_prix'].map( lambda x : -x)
    #loc => 1er argument: ligne (ici filtre) 2eme argument:colonne
    
    return df_activite

def get_type_activite() :
    conn = create_engine('sqlite:///eviesens.db')
    return pd.read_sql_query('SELECT * FROM type_activite',conn)

def create_database() :
    cursor = sqlite3.connect('eviesens.db')
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS client(
    client_id INTEGER,
    client_nom TEXT,
    client_prenom TEXT,
    client_mail TEXT,
    client_telephone TEXT,
    PRIMARY KEY(client_id)
    );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS vendeur(
        vendeur_id INTEGER,
        vendeur_nom TEXT NOT NULL,
        PRIMARY KEY(vendeur_id)
    );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS type_activite(
        type_activite_id INTEGER,
        type_activite_nom TEXT NOT NULL,
        activite_nom TEXT NOT NULL,
        PRIMARY KEY(type_activite_id)
    );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS activite(
        activite_id INTEGER,
        activite_prix NUMERIC(15,2)  ,
        activite_mois NUMERIC,
        type_activite_id INTEGER NOT NULL,
        vendeur_id INTEGER NOT NULL,
        PRIMARY KEY(activite_id),
        FOREIGN KEY(type_activite_id) REFERENCES type_activite(type_activite_id),
        FOREIGN KEY(vendeur_id) REFERENCES vendeur(vendeur_id)
    );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS type_structure(
        type_structure_id INTEGER,
        type_structure_nom TEXT NOT NULL,
        PRIMARY KEY(type_structure_id)
    );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS type_transaction(
        type_transaction_id INTEGER,
        type_transaction_nom TEXT NOT NULL,
        PRIMARY KEY(type_transaction_id)
    );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS moyen_paiement(
        moyen_paiement_id INTEGER,
        moyen_paiement_nom TEXT NOT NULL,
        PRIMARY KEY(moyen_paiement_id)
    );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS commande(
        commande_id INTEGER,
        commande_date_achat NUMERIC NOT NULL,
        client_id INTEGER NOT NULL,
        moyen_paiement_id INTEGER NOT NULL,
        type_transaction_id INTEGER NOT NULL,
        type_structure_id INTEGER NOT NULL,
        PRIMARY KEY(commande_id),
        FOREIGN KEY(client_id) REFERENCES client(client_id),
        FOREIGN KEY(moyen_paiement_id) REFERENCES moyen_paiement(moyen_paiement_id),
        FOREIGN KEY(type_transaction_id) REFERENCES type_transaction(type_transaction_id),
        FOREIGN KEY(type_structure_id) REFERENCES type_structure(type_structure_id)
    );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS commande_activite(
        commande_activite_id INTEGER,
        commande_date_soin NUMERIC,
        commande_quantite INTEGER NOT NULL,
        commande_deplacement NUMERIC(15,2)   NOT NULL,
        commande_reduction NUMERIC(15,2)   NOT NULL,
        commande_commission NUMERIC(15,2)  ,
        commande_rsi NUMERIC(15,2)  ,
        commande_date_encaissement NUMERIC,
        commande_date_perception NUMERIC,
        commande_date_remboursement NUMERIC,
        commande_id INTEGER NOT NULL,
        activite_id INTEGER NOT NULL,
        PRIMARY KEY(commande_activite_id),
        FOREIGN KEY(commande_id) REFERENCES commande(commande_id),
        FOREIGN KEY(activite_id) REFERENCES activite(activite_id)
    );
    """)

def delete_database() :
    cursor = sqlite3.connect('eviesens.db')
    cursor.execute("""
    DELETE FROM client
    """)

    cursor.execute("""
    DELETE FROM vendeur
    """)

    cursor.execute("""
    DELETE FROM type_activite
    """)

    cursor.execute("""
    DELETE FROM activite
    """)

    cursor.execute("""
    DELETE FROM type_structure
    """)

    cursor.execute("""
    DELETE FROM type_transaction
    """)

    cursor.execute("""
    DELETE FROM moyen_paiement
    """)

    cursor.execute("""
    DELETE FROM commande
    """)

    cursor.execute("""
    DELETE FROM commande_activite
    """)

    cursor.commit()

def load_annee() :
    conn = create_engine('sqlite:///eviesens.db')
    res=pd.read_sql_query('SELECT * FROM commande',conn)
    res['commande_date_achat']=pd.to_datetime(res['commande_date_achat'])
    res["annee"]=res['commande_date_achat'].dt.year
    res=res["annee"].drop_duplicates().to_list()
    return res



