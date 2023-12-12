DROP DATABASE IF EXISTS eviesens;
CREATE DATABASE eviesens;

USE eviesens;

CREATE TABLE client(
   client_id INT AUTO_INCREMENT,
   client_nom VARCHAR(50),
   client_prenom VARCHAR(50),
   mail VARCHAR(50),
   telephone VARCHAR(20),
   PRIMARY KEY(client_id)
);

CREATE TABLE vendeur(
   vendeur_id INT AUTO_INCREMENT,
   vendeur_nom VARCHAR(50) NOT NULL,
   PRIMARY KEY(vendeur_id)
);

CREATE TABLE type_activite(
   type_activite_id INT AUTO_INCREMENT,
   type_activite_nom VARCHAR(50) NOT NULL,
   PRIMARY KEY(type_activite_id)
);

CREATE TABLE activite(
   activite_id INT AUTO_INCREMENT,
   activite_prix DECIMAL(15,2),
   activite_nom VARCHAR(50) NOT NULL,
   type_activite_id INT NOT NULL,
   vendeur_id INT NOT NULL,
   PRIMARY KEY(activite_id),
   FOREIGN KEY(type_activite_id) REFERENCES type_activite(type_activite_id ),
   FOREIGN KEY(vendeur_id) REFERENCES vendeur(vendeur_id)
);

CREATE TABLE type_structure(
   structure_id INT AUTO_INCREMENT,
   structure_nom VARCHAR(50) NOT NULL,
   PRIMARY KEY(structure_id)
);

CREATE TABLE type_transaction(
   type_transaction_id INT AUTO_INCREMENT,
   type_transaction_nom VARCHAR(50) NOT NULL,
   PRIMARY KEY(type_transaction_id)
);

CREATE TABLE moyen_paiement(
   moyen_paiement_id INT AUTO_INCREMENT,
   moyen_paiement_nom VARCHAR(50) NOT NULL,
   PRIMARY KEY(moyen_paiement_id)
);

CREATE TABLE commande(
   commmande_id INT AUTO_INCREMENT,
   commande_quantité INT NOT NULL,
   commande_deplacement DECIMAL(15,2) NOT NULL,
   commande_reduction DECIMAL(15,2) NOT NULL,
   commande_date_achat DATE NOT NULL,
   commande_date_encaissement DATE,
   commande_date_perception DATE,
   commande_date_remboursement DATE,
   client_id INT NOT NULL,
   moyen_paiement_id INT NOT NULL,
   type_transaction_id INT NOT NULL,
   structure_id INT NOT NULL,
   PRIMARY KEY(commmande_id),
   FOREIGN KEY(client_id) REFERENCES client(client_id),
   FOREIGN KEY(moyen_paiement_id) REFERENCES moyen_paiement(moyen_paiement_id),
   FOREIGN KEY(type_transaction_id) REFERENCES type_transaction(type_transaction_id),
   FOREIGN KEY(structure_id) REFERENCES type_structure(structure_id)
);

CREATE TABLE commande_activite(
   activite_id INT AUTO_INCREMENT,
   commmande_id INT,
   commande_date_soin DATE,
   PRIMARY KEY(activite_id, commmande_id),
   FOREIGN KEY(activite_id) REFERENCES activite(activite_id),
   FOREIGN KEY(commmande_id) REFERENCES commande(commmande_id)
);
