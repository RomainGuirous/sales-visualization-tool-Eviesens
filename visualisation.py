import pandas as pd
import os
import numpy as np
from sqlalchemy import create_engine
import re
pd.set_option('display.max_rows', 500)
import matplotlib.pyplot as plt
import matplotlib as mpl
from textwrap import wrap
pd.options.mode.chained_assignment = None
# pour 1 atelier donné :
# -combien de  personnes en moyenne
# -quel chiffre d'affaire en moyenne par an/mois

# pour 1 soin donné : quel chiffre d'affaire moyen par mois (Ventes Eviesens, lebienetre.fr et SARL edito)

# par mois, combien d'interventions extérieures et quel chiffre d'affaire moyen ?

# quel est le total de chiffre d'affaire par mois/an obtenu par lebienetre.fr et SARL edito

#reformulation kpi:
''' -> activite_nom:                *combien de personnes en moyenne (combien d'achat?)
                                    *CA par mois, par an
                                    *quelle part de Eviens et SARL Edito
    ->vendeur_nom:                  *CA par mois,par an
    ->interventions extérieures:    *nombre
                                    *CA moyen
'''
#utilitaire
l_mois={
    1 : "janvier" , 2 : "fevrier" , 3 : "mars" , 4 : "avril" , 5 : "mai" , 6 : "juin",
    7 : "juillet" , 8 : "aout" , 9 : "septembre" , 10 : "octobre" , 11 : "novembre" , 12 : "decembre"
}

#permet d'obtenir les achats dans un mois,année donnés (rentrer mois et an en int)
def achat_mois(df_entree,mois,an):
    df=df_entree.copy()
    df['commande_date_achat']=pd.to_datetime(df['commande_date_achat']) #on convertit la colonne en datetime pour pouvoir travailler dessus
    df=df[df['commande_date_achat'].dt.month == mois]
    df=df[df['commande_date_achat'].dt.year == an]
    return df

#permet d'obtenir les achats dans une année donnée (rentrer annne en int)
def achat_an(df_entree, an):
    df=df_entree.copy()
    df['commande_date_achat']=pd.to_datetime(df['commande_date_achat']) #on convertit la colonne en datetime pour pouvoir travailler dessus
    df=df[df['commande_date_achat'].dt.year == an]
    return df

def achat_an_soin(df_entree, an):
    df=df_entree.copy()
    df['commande_date_soin']=pd.to_datetime(df['commande_date_soin']) #on convertit la colonne en datetime pour pouvoir travailler dessus
    df=df[df['commande_date_soin'].dt.year == an]
    return df

# modèle de comment est calculé le CA
def CA(df_entree):
    df=df_entree.copy()
    df['chiffre_affaire']=df['activite_prix'] * df['commande_quantite'] + df['commande_deplacement'] - df['commande_reduction'] - df['commande_commission']
    return df['chiffre_affaire']

# modèle de comment est calculé le revenu net
def revenu_net(df_entree):
    df=df_entree.copy()
    df['revenu_net']=df['activite_prix'] * df['commande_quantite'] + df['commande_deplacement'] - df['commande_reduction'] - df['commande_commission'] - df['commande_rsi']
    return df['revenu_net']


def CA_atelier_an(df_entree,an): #on donne l'annee en int
    df=df_entree.copy()
    df=achat_an(df,an) # on trie pour obtenir les dates d'achat d'une seule année
    df['chiffre_affaire']= CA(df)# on crée une colonne qui multiplie le prix par la qte pour avoir le CA brut
    df=df[['activite_nom','chiffre_affaire']] # on affiche juste nom et CA pour clarté
    df=df.groupby(by=['activite_nom']).sum().sort_values(by=['chiffre_affaire'], ascending=False) # donne le chiffre d'affaire total par activite
    df=df.reset_index()
    return df

def CA_atelier_mois(df_entree,mois,an):
    df=df_entree.copy()
    df=achat_mois(df,mois,an)# on trie pour obtenir les dates d'achat d'un seul mois (avec l'année correspondante)
    df['chiffre_affaire']=CA(df)
    df=df[['activite_nom','chiffre_affaire']] #on affiche juste nom et CA pour clarté
    df=df.groupby(by=['activite_nom']).sum().sort_values(by=['chiffre_affaire'], ascending=False) # donne le chiffre d'affaire total par activite)
    df=df.reset_index()
    return df

def CA_vendeur_an(df_entree,an):
    df=df_entree.copy()
    df=achat_an(df,an)
    df['chiffre_affaire']=CA(df)
    df=df[['vendeur_nom','chiffre_affaire']] #on affiche juste vendeur, nom et CA pour clarté
    df=df.groupby(by=['vendeur_nom']).sum().sort_values(by=['chiffre_affaire'], ascending=False) # donne le chiffre d'affaire total par activite
    df=df.reset_index()
    return df

def CA_vendeur_atelier_an(df_entree,an):
    df=df_entree.copy()
    df=achat_an(df,an)
    df['chiffre_affaire']= CA(df)
    df=df[['vendeur_nom','activite_nom','chiffre_affaire']] #on affiche juste vendeur, nom et CA pour clarté
    df=df.groupby(by=['vendeur_nom','activite_nom']).sum().sort_values(by=['vendeur_nom','activite_nom']) # donne le chiffre d'affaire total par activite
    df=df.reset_index()
    return df

def nbr_atelier_an(df_entree,an):
    df=df_entree.copy()
    df_nbr_atelier_an=achat_an(df,an)# on trie pour obtenir les dates d'achat d'une seule année
    df_nbr_atelier_an=df_nbr_atelier_an[['type_activite_id','activite_nom','commande_quantite']] #on affiche juste nom et CA pour clarté
    df_nbr_atelier_an=df_nbr_atelier_an.groupby(by=['activite_nom','type_activite_id']).sum().sort_values(by=['type_activite_id']) # donne le chiffre d'affaire total par activite
    df2=df_table_type_activite.merge(df_nbr_atelier_an,on=('type_activite_id'), how="left")
    df2=df2[['activite_nom','commande_quantite']].sort_values(by=['commande_quantite'],ascending=False)
    df2=df2.rename(columns={"commande_quantite":"nbr_ateliers"})
    df2=df2.fillna(0)
    df2['nbr_ateliers']=df2['nbr_ateliers'].astype('Int32')
    return df2

def moy_personne_atelier_an(df_entree,an):
    df=df_entree.copy()
    df=achat_an_soin(df,an)# on trie pour obtenir les dates d'achat d'un seul mois (avec l'année correspondante)
    df=df[['commande_date_soin','activite_nom','commande_quantite']]
    df=df.groupby(by=['commande_date_soin','activite_nom']).sum().sort_values(by=['activite_nom'], ascending=False).reset_index()
    df=df[['activite_nom','commande_quantite']]
    df=df.groupby(by=['activite_nom']).mean().sort_values(by=['commande_quantite'], ascending=False).reset_index()
    df=df.round(2)
    df=df.rename(columns={"commande_quantite":"nbr_gens"})
    return df

# renvoie un tableau avec le CA par mois pour toute l'année
def CA_annuel(df_entree,an):
    df=df_entree.copy()
    df=achat_an(df,an)
    df['mois']=df['commande_date_achat'].dt.month
    df['chiffre_affaire']=CA(df)
    df=df[['mois','chiffre_affaire']]
    df=df.groupby(by=['mois']).sum().sort_values(by=['mois']).reset_index()
    df['mois']=df['mois'].replace(l_mois)
    return df

def revenu_net_annuel(df_entree,an):
    df=df_entree.copy()
    df=achat_an(df,an)
    df['mois']=df['commande_date_achat'].dt.month
    df['revenu_net']=revenu_net(df)
    df=df[['mois','revenu_net']]
    df=df.groupby(by=['mois']).sum().sort_values(by=['mois']).reset_index()
    df['mois']=df['mois'].replace(l_mois)
    return df

def CA_par_client(df_entree, an):
    df=df_entree
    df=achat_an(df, an)
    df["chiffre_affaire"]=CA(df)
    df=df[['client_id','client_prenom','client_nom', 'chiffre_affaire']]
    df=df.groupby(["client_id", "client_prenom", "client_nom"],as_index=False).sum().sort_values(by=['chiffre_affaire'], ascending=False)
    df=df[["client_prenom", "client_nom",'chiffre_affaire']]
    return df




#Main
conn= create_engine('mysql+mysqlconnector://root:root@localhost:3306/eviesens')

#le dataframe de chaque table, extrait de la base de donnee
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
df_activite=df_table_type_activite.join(df_table_activite.set_index('type_activite_id'),on=('type_activite_id'), how="left")
df_activite=df_activite.join(df_table_commande_activite.set_index('activite_id'),on=('activite_id'), how="inner")
df_activite=df_activite.join(df_table_commande.set_index('commande_id'),on=('commande_id'), how="inner")
df_activite=df_activite.join(df_table_vendeur.set_index('vendeur_id'),on=('vendeur_id'),how='inner') # on transforme df_activite pour incorporer vendeur_nom
df_activite=df_activite.join(df_table_type_transaction.set_index('type_transaction_id'),on=('type_transaction_id'), how="inner")
df_activite=df_activite.join(df_table_client.set_index('client_id'),on=('client_id'), how="inner")

# jointure de table avec comme table principale commande_activite
# df_commande=df_table_commande_activite.join(df_table_activite.set_index('activite_id'),on=('activite_id'), how="inner")
# df_commande=df_commande.join(df_table_commande.set_index('commande_id'),on=('commande_id'), how="inner")
# df_commande=df_commande.join(df_table_type_activite.set_index('type_activite_id'),on=('type_activite_id'), how="inner")
# df_commande=df_commande.join(df_table_client.set_index('client_id'),on=('client_id'), how="inner")
# df_commande=df_commande.join(df_table_vendeur.set_index('vendeur_id'),on=('vendeur_id'),how='inner') # on transforme df_activite pour incorporer vendeur_nom
# df_commande=df_commande.join(df_table_type_transaction.set_index('type_transaction_id'),on=('type_transaction_id'), how="inner")
# df_commande.loc[df_commande['type_transaction_nom'] == "Remboursement",'activite_prix']=df_commande[df_commande['type_transaction_nom'] == "Remboursement"]['activite_prix'].map( lambda x : -x)

#on transforme les prix des lignes Remboursement en négatif
df_activite.loc[df_activite['type_transaction_nom'] == "Remboursement",'activite_prix']=df_activite[df_activite['type_transaction_nom'] == "Remboursement"]['activite_prix'].map( lambda x : -x)
#loc => 1er argument: ligne (ici filtre) 2eme argument:colonne


### CHIFFRE D'AFFAIRE ###
##  CA PAR ATELIER / AN
def show_atelier_an(df, annee) :
    df_atelier_an=CA_atelier_an(df, annee)
    fig, ax = plt.subplots()
    y=df_atelier_an["activite_nom"]
    x=df_atelier_an["chiffre_affaire"]
    bars=ax.barh(y, x)

    ax.bar_label(bars)
    plt.gcf().subplots_adjust(left=.25)
    ax.set_title(f"Chiffre d'affaire annuel ({annee}) par atelier")

# show_atelier_an(df_activite, 2023)


# ## CA PAR ATELIER / MOIS
def show_atelier_mois(df, mois, annee) :
    df_atelier_mois=CA_atelier_mois(df,mois,annee) #mois,annee à adapter
    fig, ax = plt.subplots()
    y=df_atelier_mois["activite_nom"]
    x=df_atelier_mois["chiffre_affaire"]
    bars=ax.barh(y, x)

    ax.bar_label(bars)
    plt.gcf().subplots_adjust(left=.25)
    de_ou_d = "d'" if mois in (4, 8, 10) else "de "
    ax.set_title(f"Chiffre d'affaire du mois {de_ou_d}{ l_mois[mois] } {annee} par atelier")

# show_atelier_mois(df_activite,1,2023)
# for i in range(1, 13):
#     show_atelier_mois(df_commande,i,2023)


# CA / VENDEUR
def show_vendeur_an(df, annee) :
    df_vendeur_an=CA_vendeur_an(df, annee)
    fig, ax = plt.subplots()
    y=df_vendeur_an["vendeur_nom"]
    x=df_vendeur_an["chiffre_affaire"]
    bars=ax.bar(y, x)

    ax.bar_label(bars)
    plt.gcf().subplots_adjust(left=.25)
    ax.set_title(f"Chiffre d'affaire annuel ({annee}) par vendeur")

# show_vendeur_an(df_activite, 2023)


## CA / (VENDEUR, ATELIER)
def show_vendeur_atelier_an(df, annee) :
    df_vendeur_atelier_an=CA_vendeur_atelier_an(df, annee)
    fig, ax = plt.subplots()
    l_vendeurs=df_vendeur_atelier_an["vendeur_nom"].to_list() #recupere les noms des vendeurs
    l_vendeurs=list(dict.fromkeys(l_vendeurs)) # retire les doublons

    for v in l_vendeurs :
        df_temp = df_vendeur_atelier_an[df_vendeur_atelier_an["vendeur_nom"]==v] # le dataframe correspondant au vendeur
        y=df_temp["activite_nom"].to_list()
        x=df_temp["chiffre_affaire"].values.tolist()
        bars=ax.barh(y, x, label=v) # cree les barres correspondantes et rajoute la legende associee
        ax.bar_label(bars)

    ax.legend()
    plt.gcf().subplots_adjust(left=.25)
    ax.set_title(f"Chiffre d'affaire annuel ({annee}) par atelier et par vendeur")

# show_vendeur_atelier_an(df_activite, 2023)


### NOMBRE ACHAT ###
## NA ATELIER / AN
def show_nbr_atelier_an(df, annee) :
    df_nbr_atelier_an = nbr_atelier_an(df, annee)
    fig, ax = plt.subplots()
    y=df_nbr_atelier_an["activite_nom"]
    x=df_nbr_atelier_an["nbr_ateliers"]
    bars=ax.barh(y, x)

    ax.bar_label(bars)
    plt.gcf().subplots_adjust(left=.27)
    ax.set_title(f"nombre d'ateliers commandes ({annee})")

# show_nbr_atelier_an(df_activite, 2023)


### NOMBRE ACHAT ###
## NA ATELIER / MOIS
def show_nbr_atelier_mois(df, annee) :
    df_nbr_atelier_an = moy_personne_atelier_an(df, annee)
    fig, ax = plt.subplots()
    y=df_nbr_atelier_an["activite_nom"]
    x=df_nbr_atelier_an["nbr_gens"]
    bars=ax.barh(y, x)

    ax.bar_label(bars)
    plt.gcf().subplots_adjust(left=.27)
    ax.set_title(f"nombre de personnes en moyenne ayant participe aux ateliers ({annee})")

# show_nbr_atelier_mois(df_activite, 2023)


### CA ANNUEL POUR CHAQUE MOIS ###
def show_CA_annuel(df, annee) :
    df_CA_annuel = CA_annuel(df, annee)
    fig, ax = plt.subplots()
    x=df_CA_annuel["mois"]
    y=df_CA_annuel["chiffre_affaire"]
    bars=ax.bar(x, y)

    ax.bar_label(bars)
    ax.set_title(f"chiffre d'affaire annuel ({annee})")

# show_CA_annuel(df_activite, 2023)


### REVENU NET ANNUEL POUR CHAQUE MOIS ###
def show_revenu_net_annuel(df, annee) :
    df_revenu_annuel = revenu_net_annuel(df, annee)
    fig, ax = plt.subplots()
    x=df_revenu_annuel["mois"]
    y=df_revenu_annuel["revenu_net"]
    bars=ax.bar(x, y)

    ax.bar_label(bars)
    ax.set_title(f"chiffre d'affaire annuel ({annee})")

# show_revenu_net_annuel(df_activite, 2023)


def show_CA_par_client(df, annee) :
    df_CA_par_client=CA_par_client(df, annee)
    df_CA_par_client=df_CA_par_client.head(30)

    fig, ax = plt.subplots()

    fig.patch.set_visible(False)
    ax.axis('off')

    ax.set_title(f"top 30 des clients ayant achete des produits ({annee})")
    table = ax.table(cellText=df_CA_par_client.values, colLabels=["nom", "prenom", "argent depensé"], bbox=[0, 0, 1, 1])


# show_CA_par_client(df_activite, 2023)
plt.show()