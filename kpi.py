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

#transforme les numéro de mois en lettres
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


#similaire à la ft achat_mois, mais prendra comme référence pour trier la colonne commande_date_soin
def achat_mois_soin(df_entree,mois,an):
    df=df_entree.copy()
    df['commande_date_soin']=pd.to_datetime(df['commande_date_soin']) #on convertit la colonne en datetime pour pouvoir travailler dessus
    df=df[df['commande_date_soin'].dt.month == mois]
    df=df[df['commande_date_soin'].dt.year == an]
    return df

def achat_an_soin(df_entree, an):
    df=df_entree.copy()
    df['commande_date_soin']=pd.to_datetime(df['commande_date_soin']) #on convertit la colonne en datetime pour pouvoir travailler dessus
    df=df[df['commande_date_soin'].dt.year == an]
    return df


#similaire à la ft achat_mois, mais prendra comme référence pour trier la colonne commande_date_perception
def achat_mois_perception(df_entree,mois,an):
    df=df_entree.copy()
    df['commande_date_perception']=pd.to_datetime(df['commande_date_perception']) #on convertit la colonne en datetime pour pouvoir travailler dessus
    df=df[df['commande_date_perception'].dt.month == mois]
    df=df[df['commande_date_perception'].dt.year == an]
    return df

def achat_an_perception(df_entree, an):
    df=df_entree.copy()
    df['commande_date_perception']=pd.to_datetime(df['commande_date_perception']) #on convertit la colonne en datetime pour pouvoir travailler dessus
    df=df[df['commande_date_perception'].dt.year == an]
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



#donne le chiffre d'affaire par an de chaque intitulé
def CA_atelier_an(df_entree,an): #on donne l'annee en int
    df=df_entree.copy()
    df=achat_an(df,an) # on trie pour obtenir les dates d'achat d'une seule année
    df['chiffre_affaire']= CA(df)# on crée une colonne qui multiplie le prix par la qte pour avoir le CA brut
    df=df[['activite_nom','chiffre_affaire']] # on affiche juste nom et CA pour clarté
    df=df.groupby(by=['activite_nom']).sum().sort_values(by=['chiffre_affaire'], ascending=False) # donne le chiffre d'affaire total par activite
    df=df.reset_index()
    return df

#donne le chiffre d'affaire par mois de chaque intitulé
def CA_atelier_mois(df_entree,mois,an):
    df=df_entree.copy()
    df=achat_mois(df,mois,an)# on trie pour obtenir les dates d'achat d'un seul mois (avec l'année correspondante)
    df['chiffre_affaire']=CA(df)
    df=df[['activite_nom','chiffre_affaire']] #on affiche juste nom et CA pour clarté
    df=df.groupby(by=['activite_nom']).sum().sort_values(by=['chiffre_affaire'], ascending=False) # donne le chiffre d'affaire total par activite)
    df=df.reset_index()
    return df


#donne le chiffre d'affaire par an de chaque vendeur
def CA_vendeur_an(df_entree,an):
    df=df_entree.copy()
    df=achat_an(df,an)
    df['chiffre_affaire']=CA(df)
    df=df[['vendeur_nom','chiffre_affaire']] #on affiche juste vendeur, nom et CA pour clarté
    df=df.groupby(by=['vendeur_nom']).sum().sort_values(by=['chiffre_affaire'], ascending=False) # donne le chiffre d'affaire total par activite
    df=df.reset_index()
    return df

#donne le chiffre d'affaire par an de chaque intitulé en fonction du vendeur
def CA_vendeur_atelier_an(df_entree,an):
    df=df_entree.copy()
    df=achat_an(df,an)
    df['chiffre_affaire']= CA(df)
    df=df[['vendeur_nom','activite_nom','chiffre_affaire']] #on affiche juste vendeur, nom et CA pour clarté
    df=df.groupby(by=['vendeur_nom','activite_nom']).sum().sort_values(by=['vendeur_nom','activite_nom']) # donne le chiffre d'affaire total par activite
    ### /!\/!\/!\ CHOISIR COMMENT ORDONNER GROUP BY ET SORT VALUES /!\/!\/!\ ###
    df=df.reset_index()
    return df


#donne le nombre d'atelier vendus par an
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
    # # print(df_nbr[df_nbr['activite_nom']=='Intervention Extérieure sur devis']) #permet de selectionner une activité en particulier, est-ce que je le mets dans une autre ft?


#donne le nombre de personnes présentes dans chaque atelier par mois
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

#est-ce utile étant donné que c'est identique à nbr_atelier_an?
#donne le nombre de personnes présentes dans chaque atelier par an
# def nbr_personne_atelier_an(df_entree,an):
#     df=df_entree.copy()
#     df=achat_an_soin(df,an)# on trie pour obtenir les dates d'achat d'un seul mois (avec l'année correspondante)
#     df=df[['activite_nom','commande_quantite']]
#     df=df.groupby(by=['activite_nom']).sum().sort_values(by=['commande_quantite'], ascending=False).reset_index()
#     df=df.rename(columns={"commande_quantite":"nbr_gens"})
#     return df


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

# renvoie un tableau avec le revenu net par mois pour toute l'année
def revenu_net_annuel(df_entree,an):
    df=df_entree.copy()
    df=achat_an(df,an)
    df['mois']=df['commande_date_achat'].dt.month
    df['revenu_net']=revenu_net(df)
    df=df[['mois','revenu_net']]
    df=df.groupby(by=['mois']).sum().sort_values(by=['mois']).reset_index()
    df['mois']=df['mois'].replace(l_mois)
    return df


#renvoie un tableau avec le CA annuel pour chaque année
def CA_par_ans(df_entree):
    df=df_entree.copy()
    df['commande_date_achat']=pd.to_datetime(df['commande_date_achat'])
    df['annee']=df['commande_date_achat'].dt.year
    df['chiffre_affaire']=CA(df)
    df=df[['annee','chiffre_affaire']]
    df=df.groupby(by=['annee']).sum().sort_values(by=['annee']).reset_index()
    return df

#renvoie un tableau avec le revenu net annuel pour chaque année
def revenu_net_par_ans(df_entree):
    df=df_entree.copy()
    df['commande_date_achat']=pd.to_datetime(df['commande_date_achat'])
    df['annee']=df['commande_date_achat'].dt.year
    df['revenu_net']=revenu_net(df)
    df=df[['annee','revenu_net']]
    df=df.groupby(by=['annee']).sum().sort_values(by=['annee']).reset_index()
    return df

#renvoie le CA annuel par client
def CA_par_client(df_entree, an):
    df=df_entree
    df=achat_an(df, an)
    df["chiffre_affaire"]=CA(df)
    df=df[['client_id','client_prenom','client_nom', 'chiffre_affaire']]
    df=df.groupby(["client_id", "client_prenom", "client_nom"],as_index=False).sum().sort_values(by=['chiffre_affaire'], ascending=False)
    df=df[["client_prenom", "client_nom",'chiffre_affaire']]
    return df
    # df = df.reset_index(level="client_id", drop=True) # le 3-tuple ["client_id", "client_prenom", "client_nom"] forme l'index, cette ligne permet d'en supprimer une partie






### MAIN ###

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
df_activite=df_activite.join(df_table_commande_activite.set_index('activite_id'),on=('activite_id'), how="left")
df_activite=df_activite.join(df_table_commande.set_index('commande_id'),on=('commande_id'), how="left")
df_activite=df_activite.join(df_table_vendeur.set_index('vendeur_id'),on=('vendeur_id'),how='inner')# on transforme df_activite pour incorporer vendeur_nom

# jointure de table avec comme table principale commande_activite
df_commande=df_table_commande_activite.join(df_table_activite.set_index('activite_id'),on=('activite_id'), how="left")
df_commande=df_commande.join(df_table_commande.set_index('commande_id'),on=('commande_id'), how="left")
df_commande=df_commande.join(df_table_type_activite.set_index('type_activite_id'),on=('type_activite_id'), how="left")
df_commande=df_commande.join(df_table_client.set_index('client_id'),on=('client_id'), how="inner")
df_commande=df_commande.join(df_table_type_transaction.set_index('type_transaction_id'),on=('type_transaction_id'), how="inner")


#on transforme les prix des lignes Remboursement en négatif
df_commande.loc[df_commande['type_transaction_nom'] == "Remboursement",'activite_prix']=df_commande[df_commande['type_transaction_nom'] == "Remboursement"]['activite_prix'].map( lambda x : -x)
#loc => 1er argument: ligne (ici filtre) 2eme argument:colonne




### CHIFFRE D'AFFAIRE ###
##  CA PAR ATELIER / AN
# CA_atelier_an(df_activite,2023) #annee à adapter
# print(CA_atelier_an(df_activite,2023))

# ## CA PAR ATELIER / MOIS
# CA_atelier_mois(df_activite,1,2023) #mois,annee à adapter
# for i in range(1,13):
#     print(CA_atelier_mois(df_activite,i,2023))


# ## CA / VENDEUR
# CA_vendeur_an(df_activite,2023) #annee à adapter
# print(CA_vendeur_an(df_activite,2023))

# ## CA / (VENDEUR, ATELIER)
# CA_vendeur_atelier_an(df_activite,2023) #annee à adapter
# print(CA_vendeur_atelier_an(df_activite,2023))

# ## CA / CLIENT / AN
# CA_par_client(df_commande, 2023)
# print(CA_par_client(df_commande, 2023))

# ## CA / AN (TABLEAU CHAQUE MOIS)
# print(CA_annuel(df_commande,2023))

# ## REVENU NET / AN (TABLEAU CHAQUE MOIS)
# print(revenu_net_annuel(df_commande,2023))

# ## CA ANNUEL / ANS
# print(CA_par_ans(df_commande))

# ## REVENU NET ANNUEL / ANS
# print(revenu_net_par_ans(df_commande))




# ### NOMBRE ACHAT ###
# ## NA ATELIER / AN
# nbr_atelier_an(df_activite,2023) #annee à adapter
# print(nbr_atelier_an(df_activite,2023))


### NOMBRE PERSONNES ####
## NBR PERSONNE / ATELIER / MOIS
# print(nbr_personne_atelier_mois(df_commande,2023))
# for i in range(1,13):
#     print(nbr_personne_atelier_mois(df_commande,i,2023))


