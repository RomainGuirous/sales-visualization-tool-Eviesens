import pandas as pd
from sqlalchemy import create_engine
pd.set_option('display.max_rows', 500)

conn=create_engine('mysql+mysqlconnector://root:root@localhost:3306/eviesens')

## AJOUT DES COMMANDES ###
# En premier ajout dans client:
#oct-2023 => ligne 9
d={"client_id": 1000, "client_nom": "Bouvet", "client_prenom": "Anne"}
df= {k:[v] for k,v in d.items()}
df=pd.DataFrame(df)
df.to_sql("client", con=conn, index=False, if_exists='append')

#juin-2023 => ligne 1
d={"client_id": 1001, "client_nom": "Boulhol", "client_prenom": "Géraldine"}
df= {k:[v] for k,v in d.items()}
df=pd.DataFrame(df)
df.to_sql("client", con=conn, index=False, if_exists='append')

#mars-2023 => ligne 15
d={"client_id": 1004, "client_nom": "Ma bonne Etoile"}
df= {k:[v] for k,v in d.items()}
df=pd.DataFrame(df)
df.to_sql("client", con=conn, index=False, if_exists='append')




##ajout dans commande
#octobre-2023 => manque Date d'achat ligne 9(excel) (remplacé par 11/10/2023 = date de soin)
d={"commande_id": 1000, "commande_date_achat": "2023-10-11", "client_id": 1000, "moyen_paiement_id": 2, "type_transaction_id": 1, "type_structure_id":3}
df= {k:[v] for k,v in d.items()}
df=pd.DataFrame(df)
df.to_sql("commande", con=conn, index=False, if_exists='append')

#juin-2023 => manque Date d'achat ligne 2(excel) (remplacé par 03/06/2023 = date de soin)
d={"commande_id": 1001, "commande_date_achat": "2023-06-03", "client_id": 1001, "moyen_paiement_id": 2, "type_transaction_id": 1, "type_structure_id":3}
df= {k:[v] for k,v in d.items()}
df=pd.DataFrame(df)
df.to_sql("commande", con=conn, index=False, if_exists='append')

#avril-2023 => manque Date d'achat ligne 5(excel) (remplacé par 06/04/2023 = date de soin), client existe deja en bdd id : 624
d={"commande_id": 1002, "commande_date_achat": "2023-04-06", "client_id": 624, "moyen_paiement_id": 4, "type_transaction_id": 1, "type_structure_id":1}
df= {k:[v] for k,v in d.items()}
df=pd.DataFrame(df)
df.to_sql("commande", con=conn, index=False, if_exists='append')

#mars-2023 => manque Date d'achat ligne 16(excel) (remplacé par 12/03/2023 = date de soin), client existe deja en bdd id : 654
d={"commande_id": 1003, "commande_date_achat": "2023-03-12", "client_id": 654, "moyen_paiement_id": 2, "type_transaction_id": 1, "type_structure_id":4}
df= {k:[v] for k,v in d.items()}
df=pd.DataFrame(df)
df.to_sql("commande", con=conn, index=False, if_exists='append')

#mars-2023 => manque Structure ligne 15(excel) (remplacé par Entreprise )
d={"commande_id": 1004, "commande_date_achat": "2023-03-09", "client_id": 1004, "moyen_paiement_id": 3, "type_transaction_id": 1, "type_structure_id":2}
df= {k:[v] for k,v in d.items()}
df=pd.DataFrame(df)
df.to_sql("commande", con=conn, index=False, if_exists='append')



## ajout dans commande_activite
#oct-2023 => ligne 9
d={"activite_id": 341, "commande_id": 1000, "commande_date_soin": "2023-10-11", "commande_deplacement": 0, "commande_reduction": 5, "commande_quantite" : 1}
df= {k:[v] for k,v in d.items()}
df=pd.DataFrame(df)
df.to_sql("commande_activite", con=conn, index=False, if_exists='append')

#juin-2023 => ligne 1
d={"activite_id": 227, "commande_id": 1001, "commande_date_soin": "2023-06-03", "commande_deplacement": 0, "commande_reduction": 5, "commande_date_encaissement": "2023-06-04" , "commande_quantite" : 1}
df= {k:[v] for k,v in d.items()}
df=pd.DataFrame(df)
df.to_sql("commande_activite", con=conn, index=False, if_exists='append')

#avr-2023 => ligne 5
d={"activite_id": 39, "commande_id": 1002, "commande_date_soin": "2023-04-06", "commande_deplacement": 0, "commande_reduction": 10 , "commande_quantite" : 1}
df= {k:[v] for k,v in d.items()}
df=pd.DataFrame(df)
df.to_sql("commande_activite", con=conn, index=False, if_exists='append')

#mars-2023 => ligne 16
d={"activite_id": 287, "commande_id": 1003, "commande_date_soin": "2023-03-12", "commande_deplacement": 0, "commande_reduction": 0, "commande_date_perception": "2023-03-15", "commande_quantite" : 1}
df= {k:[v] for k,v in d.items()}
df=pd.DataFrame(df)
df.to_sql("commande_activite", con=conn, index=False, if_exists='append')

#mars-2023 => ligne 15
d={"activite_id": 287, "commande_id": 1004, "commande_date_soin": "2023-03-09", "commande_deplacement": 0, "commande_reduction": 0, "commande_date_encaissement": "2023-03-29", "commande_date_perception": "2023-03-31", "commande_quantite" : 1}
df= {k:[v] for k,v in d.items()}
df=pd.DataFrame(df)
df.to_sql("commande_activite", con=conn, index=False, if_exists='append')