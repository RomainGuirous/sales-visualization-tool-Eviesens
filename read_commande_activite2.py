import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
import re
pd.set_option('display.max_rows', 500)

# commande
def select_commande(df) :
    df_commande = df.iloc[:49,:21] # selectionne uniquement les lignes jusque la ligne 50 du csv et 21 premieres colonnes
    df_commande = df_commande[df_commande['Structure'].notna()]
    df_commande = df_commande[df_commande['Type'].notna()]
    df_commande = df_commande[df_commande['Vendeur'].notna()]
    df_commande = df_commande[df_commande['Intitulé'].notna()]
    df_commande = df_commande[df_commande['Transaction'].notna()]
    df_commande = df_commande[df_commande['Moyen de paiement'].notna()]
    df_commande = df_commande[df_commande['Déplacement'].notna()]
    df_commande = df_commande[df_commande['Quantité'].notna()]
    df_commande = df_commande[df_commande['Reduction'].notna()]
    df_commande = df_commande[df_commande["Date d'achat"].notna()]  ######/!\/!\ PROBLEME DATE D'ACHAT  NAN A REPARER PLUS TARD /!\ /!\ #####

    #on selectionne les colonnes utiles pour nous par leur nom dans le DF    df_commande = df_commande.rename(columns={'Structure': 'type_structure_nom', 'Transaction': 'type_transaction_nom', 'Moyen de paiement':'moyen_paiement_nom', "Nom":'client_nom', "Prénom":'client_prenom',"Date d'achat":'commande_date_achat'})
    df_commande = df_commande[['Date soin', 'Nom' ,'Prénom', 'Structure', 'Type', 'Vendeur', 'Intitulé', 'Transaction',
                               'Moyen de paiement', 'Déplacement', 'Quantité', 'Reduction', "Date d'achat", 'Date Encaissement ', 'Date perception', 'Date remboursement']]
    
    #on change nom col pour correspondre au nom dans les tables type_structure, type_transaction, moyen_paiement et commande (pour commande_date_achat et client_id)
    df_commande = df_commande.rename(columns={'Date soin': 'commande_date_soin', 'Nom': 'client_nom',
                                              'Prénom': 'client_prenom', 'Structure': 'type_structure_nom',
                                              'Type': 'type_activite_nom', 'Vendeur': 'vendeur_nom',
                                              'Intitulé': 'activite_nom', 'Transaction': 'type_transaction_nom',
                                              'Moyen de paiement': 'moyen_paiement_nom', 'Déplacement': 'commande_deplacement',
                                              'Quantité': 'commande_quantité', 'Reduction': 'commande_reduction',
                                              "Date d'achat": 'commande_date_achat', 'Date Encaissement ': 'commande_date_encaissement',
                                              'Date perception': 'commande_date_perception', 'Date remboursement': 'commande_date_remboursement'})
    return df_commande





#Main
conn=create_engine('mysql+mysqlconnector://root:root@localhost:3306/eviesens')

filepaths=os.listdir("./donnees/fiches_mensuelles/") #récupère liste des noms des fichiers dans le dossier "fiches_mensuelles"

for i in range(len(filepaths)) :
    filepaths[i]="./donnees/fiches_mensuelles/"+filepaths[i] #on récupère liste des filepath de chaque fiche mensuelle

for filepath in filepaths :
    print(filepath)
    df=pd.read_csv(filepath)
    df_commande=select_commande(df)
    print(df_commande)