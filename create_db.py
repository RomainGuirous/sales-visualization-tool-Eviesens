import sqlite3
from sqlalchemy import create_engine

# conn = create_engine('sqlite:///eviesens.db')
conn = sqlite3.connect('eviesens.db')

conn.execute("""
    CREATE TABLE IF NOT EXISTS client(
    client_id INTEGER,
    client_nom TEXT,
    client_prenom TEXT,
    client_mail TEXT,
    client_telephone TEXT,
    PRIMARY KEY(client_id)
);
""")

conn.execute("""
    CREATE TABLE IF NOT EXISTS vendeur(
    vendeur_id INTEGER,
    vendeur_nom TEXT NOT NULL,
    PRIMARY KEY(vendeur_id)
);
""")

conn.execute("""
    CREATE TABLE IF NOT EXISTS type_activite(
    type_activite_id INTEGER,
    type_activite_nom TEXT NOT NULL,
    activite_nom TEXT NOT NULL,
    PRIMARY KEY(type_activite_id)
);
""")

conn.execute("""
    CREATE TABLE IF NOT EXISTS activite(
    activite_id INTEGER,
    activite_prix NUMERIC(15,2)  ,
    activite_mois NUMERIC,
    type_activite_id INTEGER NOT NULL,
    vendeur_id INTEGER NOT NULL,
    PRIMARY KEY(activite_id),
    FOREIGN KEY(type_activite_id) REFERENCES type_activite(type_activite_id),
    FOREIGN KEY(vendeur_id) REFERENCES vendeur(vendeur_id)
);
""")

conn.execute("""
    CREATE TABLE IF NOT EXISTS type_structure(
    type_structure_id INTEGER,
    type_structure_nom TEXT NOT NULL,
    PRIMARY KEY(type_structure_id)
);
""")

conn.execute("""
    CREATE TABLE IF NOT EXISTS type_transaction(
    type_transaction_id INTEGER,
    type_transaction_nom TEXT NOT NULL,
    PRIMARY KEY(type_transaction_id)
);
""")

conn.execute("""
    CREATE TABLE IF NOT EXISTS moyen_paiement(
    moyen_paiement_id INTEGER,
    moyen_paiement_nom TEXT NOT NULL,
    PRIMARY KEY(moyen_paiement_id)
);
""")

conn.execute("""
    CREATE TABLE IF NOT EXISTS commande(
    commande_id INTEGER,
    commande_date_achat NUMERIC NOT NULL,
    client_id INTEGER NOT NULL,
    moyen_paiement_id INTEGER NOT NULL,
    type_transaction_id INTEGER NOT NULL,
    type_structure_id INTEGER NOT NULL,
    PRIMARY KEY(commande_id),
    FOREIGN KEY(client_id) REFERENCES client(client_id),
    FOREIGN KEY(moyen_paiement_id) REFERENCES moyen_paiement(moyen_paiement_id),
    FOREIGN KEY(type_transaction_id) REFERENCES type_transaction(type_transaction_id),
    FOREIGN KEY(type_structure_id) REFERENCES type_structure(type_structure_id)
);
""")

conn.execute("""
    CREATE TABLE IF NOT EXISTS commande_activite(
    commande_activite_id INTEGER,
    commande_date_soin NUMERIC,
    commande_quantite INTEGER NOT NULL,
    commande_deplacement NUMERIC(15,2)   NOT NULL,
    commande_reduction NUMERIC(15,2)   NOT NULL,
    commande_commission NUMERIC(15,2)  ,
    commande_rsi NUMERIC(15,2)  ,
    commande_date_encaissement NUMERIC,
    commande_date_perception NUMERIC,
    commande_date_remboursement NUMERIC,
    commande_id INTEGER NOT NULL,
    activite_id INTEGER NOT NULL,
    PRIMARY KEY(commande_activite_id),
    FOREIGN KEY(commande_id) REFERENCES commande(commande_id),
    FOREIGN KEY(activite_id) REFERENCES activite(activite_id)
);
""")