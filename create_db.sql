DROP DATABASE IF EXISTS eviesens;
CREATE DATABASE eviesens;

USE eviesens;

CREATE TABLE client(
   client_id INT AUTO_INCREMENT,
   client_nom VARCHAR(155) ,
   client_prenom VARCHAR(155) ,
   client_mail VARCHAR(155) ,
   client_telephone VARCHAR(155) ,
   PRIMARY KEY(client_id)
);

CREATE TABLE vendeur(
   vendeur_id INT AUTO_INCREMENT,
   vendeur_nom VARCHAR(155)  NOT NULL,
   PRIMARY KEY(vendeur_id)
);

CREATE TABLE type_activite(
   type_activite_id INT AUTO_INCREMENT,
   type_activite_nom VARCHAR(155)  NOT NULL,
   activite_nom VARCHAR(155)  NOT NULL,
   PRIMARY KEY(type_activite_id)
);

CREATE TABLE activite(
   activite_id INT AUTO_INCREMENT,
   activite_prix DECIMAL(15,2)  ,
   activite_mois DATE,
   type_activite_id INT NOT NULL,
   vendeur_id INT NOT NULL,
   PRIMARY KEY(activite_id),
   FOREIGN KEY(type_activite_id) REFERENCES type_activite(type_activite_id),
   FOREIGN KEY(vendeur_id) REFERENCES vendeur(vendeur_id)
);

CREATE TABLE type_structure(
   type_structure_id INT AUTO_INCREMENT,
   type_structure_nom VARCHAR(155)  NOT NULL,
   PRIMARY KEY(type_structure_id)
);

CREATE TABLE type_transaction(
   type_transaction_id INT AUTO_INCREMENT,
   type_transaction_nom VARCHAR(155)  NOT NULL,
   PRIMARY KEY(type_transaction_id)
);

CREATE TABLE moyen_paiement(
   moyen_paiement_id INT AUTO_INCREMENT,
   moyen_paiement_nom VARCHAR(155)  NOT NULL,
   PRIMARY KEY(moyen_paiement_id)
);

CREATE TABLE commande(
   commande_id INT AUTO_INCREMENT,
   commande_date_achat DATE NOT NULL,
   client_id INT NOT NULL,
   moyen_paiement_id INT NOT NULL,
   type_transaction_id INT NOT NULL,
   type_structure_id INT NOT NULL,
   PRIMARY KEY(commande_id),
   FOREIGN KEY(client_id) REFERENCES client(client_id),
   FOREIGN KEY(moyen_paiement_id) REFERENCES moyen_paiement(moyen_paiement_id),
   FOREIGN KEY(type_transaction_id) REFERENCES type_transaction(type_transaction_id),
   FOREIGN KEY(type_structure_id) REFERENCES type_structure(type_structure_id)
);

CREATE TABLE commande_activite(
   commande_activite_id INT AUTO_INCREMENT,
   commande_date_soin DATE,
   commande_quantite INT NOT NULL,
   commande_deplacement DECIMAL(15,2)   NOT NULL,
   commande_reduction DECIMAL(15,2)   NOT NULL,
   commande_date_encaissement DATE,
   commande_date_perception DATE,
   commande_date_remboursement DATE,
   commande_id INT NOT NULL,
   activite_id INT NOT NULL,
   PRIMARY KEY(commande_activite_id),
   FOREIGN KEY(commande_id) REFERENCES commande(commande_id),
   FOREIGN KEY(activite_id) REFERENCES activite(activite_id)
);