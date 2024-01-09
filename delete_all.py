import mysql.connector

connection = mysql.connector.connect(
    host='localhost',
    database='eviesens',
    user='root',
    password='root'
)

cursor = connection.cursor()

cursor.execute("DELETE FROM commande_activite")

cursor.execute("DELETE FROM commande")
cursor.execute("DELETE FROM client")
cursor.execute("DELETE FROM moyen_paiement")
cursor.execute("DELETE FROM type_structure")
cursor.execute("DELETE FROM type_transaction")

cursor.execute("DELETE FROM activite")
cursor.execute("DELETE FROM type_activite")
cursor.execute("DELETE FROM vendeur")

connection.commit()