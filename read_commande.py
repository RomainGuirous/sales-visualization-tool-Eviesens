import pandas as pd
import numpy as np
from sqlalchemy import create_engine
pd.set_option('display.max_rows', 500)


filepath="./donnees/Janvier-2023.csv"

# commande
df=pd.read_csv(filepath)
df_commande = df.iloc[:49,:21] # selectionne uniquement les lignes jusque la ligne 50 du csv et 21 premieres colonnes
df_commande = df_commande[df_commande['Structure'].notna()]
df_commande = df_commande[df_commande['Type'].notna()]
df_commande = df_commande[df_commande['Vendeur'].notna()]
df_commande = df_commande[df_commande['Intitulé'].notna()]
df_commande = df_commande[df_commande['Transaction'].notna()]
df_commande = df_commande[df_commande['Moyen de paiement'].notna()]
df_commande = df_commande[df_commande['Déplacement'].notna()]
df_commande = df_commande[df_commande['Quantité'].notna()]
df_commande = df_commande[df_commande['Reduction'].notna()]  #si un des champs est vide, toute la ligne est supprimée
print(df_commande)