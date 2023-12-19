import pandas as pd
from sqlalchemy import create_engine
pd.set_option('display.max_rows', 500)



conn=create_engine('mysql+mysqlconnector://root:root@localhost:3306/eviesens')
### AJOUT DES ACTIVITES ###
#oct-2023 => 1er ligne manquante (Eviesens,Maternisens,,185,Soin) =>type
d={"activite_prix": 185, "activite_mois": "2023-10-01", "type_activite_id": 1, "vendeur_id": 1}
df= {k:[v] for k,v in d.items()}
df=pd.DataFrame(df)
df.to_sql("activite", con=conn, index=False, if_exists='append')

#juillet-2023 => 14eme ligne manquante (Eviesens, Envol Critallin, 16, Atelier)
d={"activite_prix": 16, "activite_mois": "2023-07-01", "type_activite_id": 14, "vendeur_id": 1}
df= {k:[v] for k,v in d.items()}
df=pd.DataFrame(df)
df.to_sql("activite", con=conn, index=False, if_exists='append')

#sept-2023 => 7eme ligne manquante (Eviesens, Bilan Chakras, 45, Soin)
d={"activite_prix": 45, "activite_mois": "2023-09-01", "type_activite_id": 7, "vendeur_id": 1}
df= {k:[v] for k,v in d.items()}
df=pd.DataFrame(df)
df.to_sql("activite", con=conn, index=False, if_exists='append')

#nov-2023 => 16eme ligne manquante (Eviesens, Forfait Massage pour nourrissons 4 séances (creche), 200, Prestation)
d={"activite_prix": 200, "activite_mois": "2023-11-01", "type_activite_id": 16, "vendeur_id": 1}
df= {k:[v] for k,v in d.items()}
df=pd.DataFrame(df)
df.to_sql("activite", con=conn, index=False, if_exists='append')