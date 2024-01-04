import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
import re
pd.set_option('display.max_rows', 500)
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

#histogramme
#-> 1 activite => tous les mois

#histogramme
#-> CA tot par mois

#histogramme
#->CA activite / an

#courbe (CA/mois)
#-> activites

#histogramme (abs:mois, ord:CA)
#vendeur

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

#similaire à la ft achat_mois, mais prendra comme référence pour trier la colonne commande_date_soin
def achat_mois_soin(df_entree,mois,an):
    df=df_entree
    df['commande_date_soin']=pd.to_datetime(df['commande_date_soin']) #on convertit la colonne en datetime pour pouvoir travailler dessus
    df=df[df['commande_date_soin'].dt.month == mois]
    df=df[df['commande_date_soin'].dt.year == an]
    return df

def achat_an_soin(df_entree, an):
    df=df_entree
    df['commande_date_soin']=pd.to_datetime(df['commande_date_soin']) #on convertit la colonne en datetime pour pouvoir travailler dessus
    df=df[df['commande_date_soin'].dt.year == an]
    return df

#similaire à la ft achat_mois, mais prendra comme référence pour trier la colonne commande_date_perception
def achat_mois_perception(df_entree,mois,an):
    df=df_entree
    df['commande_date_perception']=pd.to_datetime(df['commande_date_perception']) #on convertit la colonne en datetime pour pouvoir travailler dessus
    df=df[df['commande_date_perception'].dt.month == mois]
    df=df[df['commande_date_perception'].dt.year == an]
    return df

def achat_an_perception(df_entree, an):
    df=df_entree
    df['commande_date_perception']=pd.to_datetime(df['commande_date_perception']) #on convertit la colonne en datetime pour pouvoir travailler dessus
    df=df[df['commande_date_perception'].dt.year == an]
    return df


#donne le chiffre d'affaire par an de chaque intitulé
def CA_atelier_an(df_entree,an): #on donne l'annee en int
    df=df_entree
    df=achat_an(df,an) # on trie pour obtenir les dates d'achat d'une seule année
    df['prix_x_qte']=df['activite_prix'] * df['commande_quantite'] # on crée une colonne qui multiplie le prix par la qte pour avoir le CA brut
    df=df[['activite_nom','prix_x_qte']] # on affiche juste nom et CA pour clarté
    df=df.groupby(by=['activite_nom']).sum().sort_values(by=['prix_x_qte'], ascending=False) # donne le chiffre d'affaire total par activite
    return df

#donne le chiffre d'affaire par mois de chaque intitulé
def CA_atelier_mois(df_entree,mois,an):
    df=df_entree
    df=achat_mois(df,mois,an)# on trie pour obtenir les dates d'achat d'un seul mois (avec l'année correspondante)
    df['prix_x_qte']=df['activite_prix'] * df['commande_quantite']# on crée une colonne qui multiplie le prix par la qte pour avoir le CA brut
    df=df[['activite_nom','prix_x_qte']] #on affiche juste nom et CA pour clarté
    df=df.groupby(by=['activite_nom']).sum().sort_values(by=['prix_x_qte'], ascending=False) # donne le chiffre d'affaire total par activite)
    return df

#donne le chiffre d'affaire par an de chaque vendeur
def CA_vendeur_an(df_entree,an):
    df=df_entree
    df=achat_an(df,an)
    df['prix_x_qte']=df['activite_prix'] * df['commande_quantite']# on crée une colonne qui multiplie le prix par la qte pour avoir le CA brut
    df=df[['vendeur_nom','prix_x_qte']] #on affiche juste vendeur, nom et CA pour clarté
    df=df.groupby(by=['vendeur_nom']).sum().sort_values(by=['prix_x_qte'], ascending=False) # donne le chiffre d'affaire total par activite
    return df

#donne le chiffre d'affaire par an de chaque intitulé en fonction du vendeur
def CA_vendeur_atelier_an(df_entree,an):
    df=df_entree
    df=achat_an(df,an)
    df['prix_x_qte']=df['activite_prix'] * df['commande_quantite']# on crée une colonne qui multiplie le prix par la qte pour avoir le CA brut
    df=df[['vendeur_nom','activite_nom','prix_x_qte']] #on affiche juste vendeur, nom et CA pour clarté
    df=df.groupby(by=['vendeur_nom','activite_nom']).sum().sort_values(by=['vendeur_nom','activite_nom']) # donne le chiffre d'affaire total par activite
    ### /!\/!\/!\ CHOISIR COMMENT ORDONNER GROUP BY ET SORT VALUES /!\/!\/!\ ###
    return df

#donne le nombre d'atelier vendus par an
def nbr_atelier_an(df_entree,an):
    df=df_entree
    df_nbr_atelier_an=achat_an(df,an)# on trie pour obtenir les dates d'achat d'une seule année
    df_nbr_atelier_an=df_nbr_atelier_an[['type_activite_id','activite_nom','commande_quantite']] #on affiche juste nom et CA pour clarté
    df_nbr_atelier_an=df_nbr_atelier_an.groupby(by=['activite_nom','type_activite_id']).sum().sort_values(by=['type_activite_id']) # donne le chiffre d'affaire total par activite
    df2=df_table_type_activite.merge(df_nbr_atelier_an,on=('type_activite_id'), how="left")
    df2=df2[['activite_nom','commande_quantite']].sort_values(by=['commande_quantite'],ascending=False)
    df2=df2.rename(columns={"commande_quantite":"nbr_ateliers"})
    df2=df2.fillna(0)
    df2['nbr_ateliers']=df2['nbr_ateliers'].astype('Int32')
    return df2
    # # print(df_nbr[df_nbr['activite_nom']=='Intervention Extérieure sur devis']) #permet de selectionner une activité en particulier, est-ce que je le mets dans une autre ft?

#donne le nombre de personnes présentes dans chaque atelier par mois
def nbr_personne_atelier_mois(df_entree,mois,an):
    df=df_entree
    df=achat_mois_soin(df,mois,an)# on trie pour obtenir les dates d'achat d'un seul mois (avec l'année correspondante)
    df=df[['commande_date_soin','activite_nom','commande_quantite']]
    df=df.groupby(by=['commande_date_soin','activite_nom']).sum().sort_values(by=['commande_quantite'], ascending=False).reset_index()
    df=df[['activite_nom','commande_quantite']]
    df=df.rename(columns={"commande_quantite":"nbr_gens"})
    return df

#est-ce utile étant donné que c'est identique à nbr_atelier_an?
#donne le nombre de personnes présentes dans chaque atelier par an
# def nbr_personne_atelier_an(df_entree,an):
#     df=df_entree
#     df=achat_an_soin(df,an)# on trie pour obtenir les dates d'achat d'un seul mois (avec l'année correspondante)
#     df=df[['activite_nom','commande_quantite']]
#     df=df.groupby(by=['activite_nom']).sum().sort_values(by=['commande_quantite'], ascending=False).reset_index()
#     df=df.rename(columns={"commande_quantite":"nbr_gens"})
#     return df


def CA_brut_an(df_entree,an):
    df=df_entree
    df=achat_an(an)
    df['mois']=df['commande_date_achat'].dt.month
    df['chiffre_affaire']=df['activite_prix'] * df['commande_quantite'] - df['commande_deplacement'] - df['reduction']
    df=df[['mois','chiffre_affaire']]
    df=df.groupby(by=['mois']).sum().sort_values(by=['mois']).reset_index()



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



# jointure des 4 tables type_activite ,commande, activite et commande_activite (par la gauche pour garder affiché tous les noms d'activite)
df_activite=df_table_type_activite.join(df_table_activite.set_index('type_activite_id'),on=('type_activite_id'), how="left")
df_activite=df_activite.join(df_table_commande_activite.set_index('activite_id'),on=('activite_id'), how="left")
df_activite=df_activite.join(df_table_commande.set_index('commande_id'),on=('commande_id'), how="left")

#jointure avec table vendeur
df_activite_vendeur=df_activite.join(df_table_vendeur.set_index('vendeur_id'),on=('vendeur_id'),how='inner')# on transforme df_activite pour incorporer vendeur_nom

# jointure de table avec comme table principale commande_activite
df_commande=df_table_commande_activite.join(df_table_activite.set_index('activite_id'),on=('activite_id'), how="left")
df_commande=df_commande.join(df_table_commande.set_index('commande_id'),on=('commande_id'), how="left")
df_commande=df_commande.join(df_table_type_activite.set_index('type_activite_id'),on=('type_activite_id'), how="left")

### CHIFFRE D'AFFAIRE ###
##  CA PAR ATELIER / AN
CA_atelier_an(df_activite,2023) #annee à adapter
print(CA_atelier_an(df_activite,2023))

## CA PAR ATELIER / MOIS
CA_atelier_mois(df_activite,1,2023) #mois,annee à adapter
for i in range(1,13):
    print(CA_atelier_mois(df_activite,i,2023))


## CA / VENDEUR
CA_vendeur_an(df_activite_vendeur,2023) #annee à adapter
print(CA_vendeur_an(df_activite_vendeur,2023))

## CA / (VENDEUR, ATELIER)
CA_vendeur_atelier_an(df_activite_vendeur,2023) #annee à adapter
print(CA_vendeur_atelier_an(df_activite_vendeur,2023))


### NOMBRE ACHAT ###
## NA ATELIER / AN
# nbr_atelier_an(df_activite,2023) #annee à adapter
print(nbr_personne_atelier_an(df_activite,2023))


### NOMBRE PERSONNES ####
## NBR PERSONNE / ATELIER / MOIS
nbr_personne_atelier_mois(df_commande,1,2023)
for i in range(1,13):
    print(nbr_personne_atelier_mois(df_commande,i,2023))

# ## PERSONNE / ATELIER / AN
nbr_personne_atelier_an(df_commande,2023)
print(nbr_personne_atelier_an(df_commande,2023))