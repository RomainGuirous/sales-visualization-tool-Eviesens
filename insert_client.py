import pandas as pd
from sqlalchemy import create_engine
pd.set_option('display.max_rows', 500)

filepath="./donnees/contacts.csv"
df=pd.read_csv(filepath)

df_contact = df.iloc[:,:4] # selectionne toutes les lignes des colonnes A a D d'excel (colonnes 1 a 4 du csv)
df_contact = df_contact.dropna(how = 'all') #retire les lignes ou toutes les donnees sont vides
df_contact = df_contact.rename(columns={'Prénom': 'client_prenom', 'Nom de famille': 'client_nom', 'E-mail 1':'client_mail', 'Téléphone 1':'client_telephone'}) #rename pour correspondre aux noms en bdd

conn= create_engine('mysql+mysqlconnector://root:root@localhost:3306/eviesens')

df_contact.to_sql("client", con=conn, index=False, if_exists='append')