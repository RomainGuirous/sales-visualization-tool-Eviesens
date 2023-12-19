#octobre, mars, juin, avril,septembre, novembre,juillet
import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
import re
pd.set_option('display.max_rows', 500)

missing_mois= ['Octobre', 'Mars', 'Juin','Avril', 'Septembre', 'Novembre', 'Juillet']


def missing_mois_annee_ligne(mois_annee,ligne):
    filepath="./donnees/fiches_mensuelles/"+mois_annee+".csv"
    df=pd.read_csv(filepath)
    df=df.iloc[ligne,:]
    return df

def add_missing_data(col,data,mois_annee,table,connection):
    filepath="./donnees/fiches_mensuelles/"+mois_annee+".csv"
    d = {col:data}
    df = pd.DataFrame(data=d)
    df.to_sql(table, con=connection, index=False, if_exists='append')

#nov-oct-juill-dec
#vendeur - intitule - prix - type =>
    #type_activite_nom  +  activite_nom =>
        #activite_prix + activite_mois (yyyy-mm-01) + type_activite_id + vendeur_id
 
 
conn=create_engine('mysql+mysqlconnector://root:root@localhost:3306/eviesens')
### AJOUT DES ACTIVITES ###
# #oct-2023 => 1er ligne manquante (Eviesens,Maternisens,,185,Soin) =>type
# d={"activite_prix": 185,"activite_mois": "2023-10-01","type_activite_id": 1,"vendeur_id": 1}
# df= {k:[v] for k,v in d.items()}
# df=pd.DataFrame(df)
# print(df)
# df.to_sql("activite", con=conn, index=False, if_exists='append')

# #juillet-2023 => 14eme ligne manquante (Eviesens, Envol Critallin, 16, Atelier)
# d={"activite_prix": 16,"activite_mois": "2023-07-01","type_activite_id": 14,"vendeur_id": 1}
# df= {k:[v] for k,v in d.items()}
# df=pd.DataFrame(df)
# df.to_sql("activite", con=conn, index=False, if_exists='append')

# #sept-2023 => 7eme ligne manquante (Eviesens, Bilan Chakras, 45, Soin)
# d={"activite_prix": 45,"activite_mois": "2023-09-01","type_activite_id": 7,"vendeur_id": 1}
# df= {k:[v] for k,v in d.items()}
# df=pd.DataFrame(df)
# df.to_sql("activite", con=conn, index=False, if_exists='append')

# #nov-2023 => 16eme ligne manquante (Eviesens, Forfait Massage pour nourrissons 4 séances (creche), 200, Prestation)
# d={"activite_prix": 200,"activite_mois": "2023-11-01","type_activite_id": 16,"vendeur_id": 1}
# df= {k:[v] for k,v in d.items()}
# df=pd.DataFrame(df)
# df.to_sql("activite", con=conn, index=False, if_exists='append')

### AJOUT DES COMMANDES ###
#En premier ajout dans client:

#oct-2023 => ligne 9
d={"client_id": 1000,"client_nom": "Bouvet","client_prenom": "Anne","client_mail":,"client_telephone":}
df= {k:[v] for k,v in d.items()}
df=pd.DataFrame(df)
df.to_sql("client", con=conn, index=False, if_exists='append')

#juin-2023 => ligne 1
d={"client_id": 1001,"client_nom": "Boulhol","client_prenom": "Géraldine","client_mail":,"client_telephone":}
df= {k:[v] for k,v in d.items()}
df=pd.DataFrame(df)
df.to_sql("client", con=conn, index=False, if_exists='append')

#avr-2023 => ligne 5
d={"client_id": 1002,"client_nom": "Vi","client_prenom": "Franck","client_mail":,"client_telephone":}
df= {k:[v] for k,v in d.items()}
df=pd.DataFrame(df)
df.to_sql("client", con=conn, index=False, if_exists='append')

#mars-2023 => ligne 16
d={"client_id": 1003,"client_nom": "Villa Yoga","client_prenom": ,"client_mail":,"client_telephone":}
df= {k:[v] for k,v in d.items()}
df=pd.DataFrame(df)
df.to_sql("client", con=conn, index=False, if_exists='append')

#mars-2023 => ligne 15
d={"client_id": 1004,"client_nom": "Ma bonne Etoile","client_prenom": ,"client_mail":,"client_telephone":}
df= {k:[v] for k,v in d.items()}
df=pd.DataFrame(df)
df.to_sql("client", con=conn, index=False, if_exists='append')




##ajout dans commande

#octobre-2023 => manque Date d'achat ligne 9(excel) (remplacé par 11/10/2023 = date de soin)
d={"commande_id": 1000,"commande_date_achat": "2023-10-11","client_id": 1000,"moyen_paiement_id": 2,"type_transaction_id": 1,"type_structure_id":3}
df= {k:[v] for k,v in d.items()}
df=pd.DataFrame(df)
df.to_sql("commande", con=conn, index=False, if_exists='append')

#juin-2023 => manque Date d'achat ligne 2(excel) (remplacé par 03/06/2023 = date de soin)
d={"commande_id": 1001,"commande_date_achat": "2023-06-03","client_id": 1001,"moyen_paiement_id": 2,"type_transaction_id": 1,"type_structure_id":3}
df= {k:[v] for k,v in d.items()}
df=pd.DataFrame(df)
df.to_sql("commande", con=conn, index=False, if_exists='append')

#avril-2023 => manque Date d'achat ligne 5(excel) (remplacé par 06/04/2023 = date de soin)
d={"commande_id": 1002,"commande_date_achat": "2023-04-06","client_id": 1002,"moyen_paiement_id": 4,"type_transaction_id": 1,"type_structure_id":1}
df= {k:[v] for k,v in d.items()}
df=pd.DataFrame(df)
df.to_sql("commande", con=conn, index=False, if_exists='append')

#mars-2023 => manque Date d'achat ligne 16(excel) (remplacé par 12/03/2023 = date de soin)
d={"commande_id": 1003,"commande_date_achat": "2023-03-12","client_id": 1003,"moyen_paiement_id": 2,"type_transaction_id": 1,"type_structure_id":4}
df= {k:[v] for k,v in d.items()}
df=pd.DataFrame(df)
df.to_sql("commande", con=conn, index=False, if_exists='append')

#mars-2023 => manque Structure ligne 15(excel) (remplacé par Entreprise )
d={"commande_id": 1004,"commande_date_achat": "2023-03-09","client_id": 1004,"moyen_paiement_id": 3,"type_transaction_id": 1,"type_structure_id":2}
df= {k:[v] for k,v in d.items()}
df=pd.DataFrame(df)
df.to_sql("commande", con=conn, index=False, if_exists='append')



## ajout dans commande_activite

#oct-2023 => ligne 9
d={"activite_id": 1000,"commande_id": 1000,"commande_date_soin": "Anne","commande_deplacement": ,"commande_reduction": ,"commande_date_encaissement": ,"commande_date_perception": ,"commande_date_remboursement": }
df= {k:[v] for k,v in d.items()}
df=pd.DataFrame(df)
df.to_sql("commande_activite", con=conn, index=False, if_exists='append')

#juin-2023 => ligne 1
d={"activite_id": 1000,"commande_id": 1001,"commande_date_soin": "Anne","commande_deplacement": ,"commande_reduction": ,"commande_date_encaissement": ,"commande_date_perception": ,"commande_date_remboursement": }
df= {k:[v] for k,v in d.items()}
df=pd.DataFrame(df)
df.to_sql("commande_activite", con=conn, index=False, if_exists='append')

#avr-2023 => ligne 5
d={"activite_id": 1000,"commande_id": 1002,"commande_date_soin": "Anne","commande_deplacement": ,"commande_reduction": ,"commande_date_encaissement": ,"commande_date_perception": ,"commande_date_remboursement": }
df= {k:[v] for k,v in d.items()}
df=pd.DataFrame(df)
df.to_sql("commande_activite", con=conn, index=False, if_exists='append')

#mars-2023 => ligne 16
d={"activite_id": 1000,"commande_id": 1003,"commande_date_soin": "Anne","commande_deplacement": ,"commande_reduction": ,"commande_date_encaissement": ,"commande_date_perception": ,"commande_date_remboursement": }
df= {k:[v] for k,v in d.items()}
df=pd.DataFrame(df)
df.to_sql("commande_activite", con=conn, index=False, if_exists='append')

#mars-2023 => ligne 15
d={"activite_id": 1000,"commande_id": 1003,"commande_date_soin": "Anne","commande_deplacement": ,"commande_reduction": ,"commande_date_encaissement": ,"commande_date_perception": ,"commande_date_remboursement": }
df= {k:[v] for k,v in d.items()}
df=pd.DataFrame(df)
df.to_sql("commande_activite", con=conn, index=False, if_exists='append')


