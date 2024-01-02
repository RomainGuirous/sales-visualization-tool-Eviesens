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
def n_mois_to_mois(n) :
    l_mois={
        1 : "janvier" , 2 : "fevrier" , 3 : "mars" , 4 : "avril" , 5 : "mai" , 6 : "juin",
        7 : "juillet" , 8 : "aout" , 9 : "septembre" , 10 : "octobre" , 11 : "novembre" , 12 : "decembre"
        }
    return(l_mois[n])

#permet d'obtenir les achats dans un mois,année donnés (rentrer mois et an en int)
def achat_mois(df_entree,mois,an):
    df=df_entree
    df['commande_date_achat']=pd.to_datetime(df['commande_date_achat']) #on convertit la colonne en datetime pour pouvoir travailler dessus
    df=df[df['commande_date_achat'].dt.month == mois]
    df=df[df['commande_date_achat'].dt.year == an]
    return df

#permet d'obtenir les achats dans une année donnée (rentrer annne en int)
def achat_an(df_entree, an):
    df=df_entree
    df['commande_date_achat']=pd.to_datetime(df['commande_date_achat']) #on convertit la colonne en datetime pour pouvoir travailler dessus
    df=df[df['commande_date_achat'].dt.year == an]
    return df


def CA_atelier_an(df_entree,an): #on donne l'annee en int
    df=df_entree
    df_CA_atelier_an=achat_an(df,an) # on trie pour obtenir les dates d'achat d'une seule année
    df_CA_atelier_an['prix_x_qte']=df_CA_atelier_an['activite_prix'] * df_CA_atelier_an['commande_quantite'] # on crée une colonne qui multiplie le prix par la qte pour avoir le CA brut
    df_CA_atelier_an=df_CA_atelier_an[['activite_nom','prix_x_qte']] # on affiche juste nom et CA pour clarté
    df_CA_atelier_an=df_CA_atelier_an.groupby(by=['activite_nom']).sum().sort_values(by=['prix_x_qte'], ascending=False) # donne le chiffre d'affaire total par activite
    return df_CA_atelier_an

def CA_atelier_mois(df_entree,mois,an):
    df=df_entree
    df_CA_atelier_mois=achat_mois(df,mois,an)# on trie pour obtenir les dates d'achat d'un seul mois (avec l'année correspondante)
    df_CA_atelier_mois['prix_x_qte']=df_CA_atelier_mois['activite_prix'] * df_CA_atelier_mois['commande_quantite']# on crée une colonne qui multiplie le prix par la qte pour avoir le CA brut
    df_CA_atelier_mois=df_CA_atelier_mois[['activite_nom','prix_x_qte']] #on affiche juste nom et CA pour clarté
    df_CA_atelier_mois=df_CA_atelier_mois.groupby(by=['activite_nom']).sum().sort_values(by=['prix_x_qte'], ascending=False) # donne le chiffre d'affaire total par activite)
    return df_CA_atelier_mois

# df_CA_vendeur_an=df_commande.join(df_table_vendeur.set_index('vendeur_id'),on=('vendeur_id'),how='inner') #j'ai décidé d'extraire la transformation du df de la ft
def CA_vendeur_an(df_entree,an):
    df=df_entree
    df_CA_vendeur_an=achat_an(df,an)
    df_CA_vendeur_an['prix_x_qte']=df_CA_vendeur_an['activite_prix'] * df_CA_vendeur_an['commande_quantite']# on crée une colonne qui multiplie le prix par la qte pour avoir le CA brut
    df_CA_vendeur_an=df_CA_vendeur_an[['vendeur_nom','prix_x_qte']] #on affiche juste vendeur, nom et CA pour clarté
    df_CA_vendeur_an=df_CA_vendeur_an.groupby(by=['vendeur_nom']).sum().sort_values(by=['prix_x_qte'], ascending=False) # donne le chiffre d'affaire total par activite
    return df_CA_vendeur_an

# df_commande=df_commande.join(df_table_vendeur.set_index('vendeur_id'),on=('vendeur_id'),how='inner') #j'ai décidé d'extraire la transformation du df de la ft
def CA_vendeur_atelier_an(df_entree,an):
    df=df_entree
    df_CA_vendeur_atelier_an=achat_an(df,an)
    df_CA_vendeur_atelier_an['prix_x_qte']=df_CA_vendeur_atelier_an['activite_prix'] * df_CA_vendeur_atelier_an['commande_quantite']# on crée une colonne qui multiplie le prix par la qte pour avoir le CA brut
    df_CA_vendeur_atelier_an=df_CA_vendeur_atelier_an[['vendeur_nom','activite_nom','prix_x_qte']] #on affiche juste vendeur, nom et CA pour clarté
    df_CA_vendeur_atelier_an=df_CA_vendeur_atelier_an.groupby(by=['vendeur_nom','activite_nom']).sum().sort_values(by=['vendeur_nom','activite_nom']) # donne le chiffre d'affaire total par activite
    ### /!\/!\/!\ CHOISIR COMMENT ORDONNER GROUP BY ET SORT VALUES /!\/!\/!\ ###
    return df_CA_vendeur_atelier_an

def nbr_atelier_an(df_entree,an):
    df=df_entree
    df_nbr_atelier_an=achat_an(df,an)# on trie pour obtenir les dates d'achat d'une seule année
    df_nbr_atelier_an=df_nbr_atelier_an[['type_activite_id','activite_nom','commande_quantite']] #on affiche juste nom et CA pour clarté
    df_nbr_atelier_an=df_nbr_atelier_an.groupby(by=['activite_nom','type_activite_id']).sum().sort_values(by=['type_activite_id']) # donne le chiffre d'affaire total par activite
    df2=df_table_type_activite.merge(df_nbr_atelier_an,on=('type_activite_id'), how="left")
    df2=df2[['activite_nom','commande_quantite']].sort_values(by=['commande_quantite'],ascending=False)
    df2=df2.fillna(0)
    return df2
    # # print(df_nbr[df_nbr['activite_nom']=='Intervention Extérieure sur devis']) #permet de selectionner une activité en particulier, est-ce que je le mets dans une autre ft?



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



# jointure des 5 tables type_activite ,commande, activite, commande_activite et vendeur (par la gauche pour garder affiché tous les noms d'activite)
df_commande=df_table_type_activite.join(df_table_activite.set_index('type_activite_id'),on=('type_activite_id'), how="left")
df_commande=df_commande.join(df_table_commande_activite.set_index('activite_id'),on=('activite_id'), how="left")
df_commande=df_commande.join(df_table_commande.set_index('commande_id'),on=('commande_id'), how="left")
df_commande=df_commande.join(df_table_vendeur.set_index('vendeur_id'),on=('vendeur_id'),how='inner')# ontransforme df_commande pour incorporer vendeur_nom 



### CHIFFRE D'AFFAIRE ###
##  CA PAR ATELIER / AN
def show_atelier_an(df, annee) :
    df_atelier_an=CA_atelier_an(df, annee)
    fig, ax = plt.subplots()
    y=df_atelier_an.index
    x=df_atelier_an["prix_x_qte"]
    bars=ax.barh(y, x)

    ax.bar_label(bars)
    plt.gcf().subplots_adjust(left=.25)
    ax.set_title(f"Chiffre d'affaire annuel ({annee}) par atelier")

show_atelier_an(df_commande, 2023)


# ## CA PAR ATELIER / MOIS
def show_atelier_mois(df, mois, annee) :
    df_atelier_mois=CA_atelier_mois(df,mois,annee) #mois,annee à adapter
    fig, ax = plt.subplots()
    y=df_atelier_mois.index
    x=df_atelier_mois["prix_x_qte"]
    bars=ax.barh(y, x)

    ax.bar_label(bars)
    plt.gcf().subplots_adjust(left=.25)
    de_ou_d = "d'" if mois in (4, 8, 10) else "de "
    ax.set_title(f"Chiffre d'affaire du mois {de_ou_d}{ n_mois_to_mois(mois) } {annee} par atelier")

show_atelier_mois(df_commande,2,2023)
# for i in range(1, 13):
#     show_atelier_mois(df_commande,i,2023)


# CA / VENDEUR
def show_vendeur_an(df, annee) :
    df_vendeur_an=CA_vendeur_an(df, annee)
    fig, ax = plt.subplots()
    y=df_vendeur_an.index
    x=df_vendeur_an["prix_x_qte"]
    bars=ax.bar(y, x)

    ax.bar_label(bars)
    plt.gcf().subplots_adjust(left=.25)
    ax.set_title(f"Chiffre d'affaire annuel ({annee}) par vendeur")

show_vendeur_an(df_commande, 2023)


## CA / (VENDEUR, ATELIER)
def show_vendeur_atelier_an(df, annee) :
    df_vendeur_atelier_an=CA_vendeur_atelier_an(df, annee)
    fig, ax = plt.subplots()
    l_vendeurs=df_vendeur_atelier_an.index.get_level_values("vendeur_nom").to_list() #recupere les noms des vendeurs
    l_vendeurs=list(dict.fromkeys(l_vendeurs)) # retire les doublons

    for v in l_vendeurs :
        df_temp = df_vendeur_atelier_an.filter(like=v, axis=0) # le dataframe correspondant au vendeur
        y=df_temp.index.get_level_values("activite_nom").to_list()
        x=df_temp["prix_x_qte"].values.tolist()
        bars=ax.barh(y, x, label=v) # cree les barres correspondantes et rajoute la legende associee
        ax.bar_label(bars)

    ax.legend()
    plt.gcf().subplots_adjust(left=.25)
    ax.set_title(f"Chiffre d'affaire annuel ({annee}) par atelier et par vendeur")

show_vendeur_atelier_an(df_commande, 2023)


### NOMBRE ACHAT ###
## NA ATELIER / AN
def show_nbr_atelier_an(df, annee) :
    df_nbr_atelier_an = nbr_atelier_an(df, annee)
    fig, ax = plt.subplots()
    y=df_nbr_atelier_an["activite_nom"]
    x=df_nbr_atelier_an["commande_quantite"]
    bars=ax.barh(y, x)

    ax.bar_label(bars)
    plt.gcf().subplots_adjust(left=.27)
    ax.set_title(f"quantite commandee annuelle ({annee}) par atelier")

show_nbr_atelier_an(df_commande, 2023)
plt.show()