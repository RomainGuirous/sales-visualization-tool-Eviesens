import pandas as pd
from sqlalchemy import create_engine

#transforme les numéro de mois en nom de mois
l_mois={
    1 : "janvier" , 2 : "fevrier" , 3 : "mars" , 4 : "avril" , 5 : "mai" , 6 : "juin",
    7 : "juillet" , 8 : "aout" , 9 : "septembre" , 10 : "octobre" , 11 : "novembre" , 12 : "decembre"
}



# modèle de comment est calculé le CA
def CA(df_entree):
    df=df_entree.copy()
    df['chiffre_affaire']=(df['activite_prix'] - df['commande_reduction'] - df['commande_commission']) * df['commande_quantite'] + df['commande_deplacement']
    return df['chiffre_affaire']

# modèle de comment est calculé le revenu net
def revenu_net(df_entree):
    df=df_entree.copy()
    df['revenu_net']=(df['activite_prix'] - df['commande_reduction'] - df['commande_commission']) * df['commande_quantite'] + df['commande_deplacement'] - df['commande_rsi']
    return df['revenu_net']


# fonctions de selection
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




#kpi
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
    df=df.reset_index()
    return df

#donne le nombre d'atelier vendus par an
def nbr_atelier_an(df_entree,an, df_all_activite):
    df=df_entree.copy()
    df_nbr_atelier_an=achat_an(df,an)# on trie pour obtenir les dates d'achat d'une seule année
    df_nbr_atelier_an=df_nbr_atelier_an[['type_activite_id','activite_nom','commande_quantite']] #on affiche juste nom et CA pour clarté
    df_nbr_atelier_an=df_nbr_atelier_an.groupby(by=['activite_nom','type_activite_id']).sum().sort_values(by=['type_activite_id']) # donne le chiffre d'affaire total par activite
    df2=df_all_activite.merge(df_nbr_atelier_an,on=('type_activite_id'), how="left")
    df2=df2[['activite_nom','commande_quantite']].sort_values(by=['commande_quantite'],ascending=False)
    df2=df2.rename(columns={"commande_quantite":"nbr_ateliers"})
    df2=df2.fillna(0)
    df2['nbr_ateliers']=df2['nbr_ateliers'].astype('Int32')
    return df2

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

#donne le nombre de personnes présentes dans chaque atelier par an
def nbr_personne_atelier_an(df_entree,an):
    df=df_entree.copy()
    df=achat_an_soin(df,an)# on trie pour obtenir les dates d'achat d'un seul mois (avec l'année correspondante)
    #on multiplie la quantité pour les achats multi-séances
    df.loc[df['activite_nom'] == "Formule 10 Yin Yoga R",'commande_quantite']=df[df['activite_nom'] == "Formule 10 Yin Yoga R"]['commande_quantite'].map( lambda x : 10*x)
    df.loc[df['activite_nom'] == "Formule 10 YinYoga R en 3x",'commande_quantite']=df[df['activite_nom'] == "Formule 10 YinYoga R en 3x"]['commande_quantite'].map( lambda x : 10*x)
    df.loc[df['activite_nom'] == "Forfait Massage pour nourrissons 4 séances (creche)",'commande_quantite']=df[df['activite_nom'] == "Forfait Massage pour nourrissons 4 séances (creche)"]['commande_quantite'].map( lambda x : 4*x)
    df.loc[df['activite_nom'] == "Forfait Mini-Misp 3 séances (creche)",'commande_quantite']=df[df['activite_nom'] == "Forfait Mini-Misp 3 séances (creche)"]['commande_quantite'].map( lambda x : 3*x)
    df.loc[df['activite_nom'] == "Éveil sonore et musical 3 séances (creche)",'commande_quantite']=df[df['activite_nom'] == "Éveil sonore et musical 3 séances (creche)"]['commande_quantite'].map( lambda x : 3*x)
    df.loc[df['activite_nom'] == "Programme 10 séances (creche)",'commande_quantite']=df[df['activite_nom'] == "Programme 10 séances (creche)"]['commande_quantite'].map( lambda x : 10*x)
    df.loc[df['activite_nom'] == "Massage du corps entier de bébé : 4 séances ludiques et progressives",'commande_quantite']=df[df['activite_nom'] == "Massage du corps entier de bébé : 4 séances ludiques et progressives"]['commande_quantite'].map( lambda x : 4*x)
    df.loc[df['activite_nom'] == "Reliance 10 séances",'commande_quantite']=df[df['activite_nom'] == "Reliance 10 séances"]['commande_quantite'].map( lambda x : 10*x)
    df.loc[df['activite_nom'] == "Magic Sound Wave 5 séances",'commande_quantite']=df[df['activite_nom'] == "Magic Sound Wave 5 séances"]['commande_quantite'].map( lambda x : 5*x)
    #si remboursement, la quantité devient négative
    df.loc[df['type_transaction_nom'] == "Remboursement",'commande_quantite']=df[df['type_transaction_nom'] == "Remboursement"]['commande_quantite'].map( lambda x : -x)
    df=df[['activite_nom','commande_quantite']]
    print(df)
    df=df.groupby(by=['activite_nom']).sum().sort_values(by=['activite_nom'], ascending=False).reset_index()
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