import sqlite3
from sqlalchemy import create_engine

# conn = create_engine('sqlite:///eviesens.db')
conn = sqlite3.connect('eviesens.db')

conn.execute("""
    DELETE FROM client
""")

conn.execute("""
   DELETE FROM vendeur
""")

conn.execute("""
   DELETE FROM type_activite
""")

conn.execute("""
   DELETE FROM activite
""")

conn.execute("""
   DELETE FROM type_structure
""")

conn.execute("""
   DELETE FROM type_transaction
""")

conn.execute("""
   DELETE FROM moyen_paiement
""")

conn.execute("""
   DELETE FROM commande
""")

conn.execute("""
   DELETE FROM commande_activite
""")

conn.commit()