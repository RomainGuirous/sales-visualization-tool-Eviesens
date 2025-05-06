# Projet Stage Eviesens

## Description
Ce projet est une application Python permettant de gérer et d'analyser des données liées aux activités, commandes, et clients d'une entreprise. Elle offre des fonctionnalités d'importation de données, de visualisation de KPI (indicateurs clés de performance), et de gestion de base de données.

## Prérequis
- **Python** : Version 3.12.1
- **Bibliothèques Python** :
  - `pandas`
  - `sqlalchemy`
  - `matplotlib`
  - `tkinter`
  - `numpy`
  - `shutil`
  - `sqlite3`

## Installation
1. Placez l'application (par exemple, le fichier `.exe` ou le projet Python) dans un dossier dédié.
2. Assurez-vous que les fichiers nécessaires (comme les fichiers CSV ou Excel) sont accessibles.

## Utilisation
### Importation des données
L'application permet d'importer des données dans la base de données SQLite via plusieurs options :
- **Ajouter un fichier CSV** : Ajoute le contenu d'un fichier CSV à la base de données.
- **Ajouter un dossier de CSV** : Ajoute tous les fichiers CSV contenus dans un dossier à la base de données.
- **Ajouter un fichier Excel (XLSX)** : Lit un fichier Excel et ajoute son contenu à la base de données. Chaque feuille de calcul doit correspondre à un mois et une année.

⚠️ **Important** : Après l'importation d'un fichier, redémarrez l'application pour garantir une prise en compte correcte des données.

### Options
- **Supprimer la base de données** : Supprime tout le contenu de la base de données.  
  ⚠️ Si un fichier est importé plusieurs fois, les données seront dupliquées, ce qui faussera les résultats. En cas d'erreur d'importation, supprimez la base de données et réimportez les fichiers.

### Visualisation des KPI
L'application propose plusieurs visualisations des données, telles que :
- Chiffre d'affaires par atelier, vendeur, ou client.
- Nombre d'ateliers commandés.
- Revenu net par mois.
- Moyenne de personnes par atelier.

### Personnalisation
- **Couleurs de l'interface** : Vous pouvez personnaliser les couleurs de l'application via le menu "Options".

## ⚠️ Informations importantes ⚠️
- **Nom des feuilles Excel** : Les noms des feuilles doivent contenir le mois et l'année (par exemple, `janvier_2023`).
- **Colonnes obligatoires** :
  - Colonnes E à Q (incluses) : Si des données sont absentes, la ligne sera ignorée.
  - Colonnes Y à AB (incluses) : Ne jamais supprimer de lignes, sauf si l'intitulé n'est pas proposé à la vente.
- **Remboursements** : Lorsqu'un remboursement est effectué, ajoutez une "Date soin" fictive pour que la donnée soit prise en compte.

## Structure du Projet

Projet_Stage_Eviesens/ 
├── application.py # Interface utilisateur principale (Tkinter) 
├── main.py # Point d'entrée pour exécuter l'application 
├── kpi.py # Calcul des indicateurs clés de performance (KPI) 
├── visualisation.py # Génération de graphiques pour les KPI 
├── database_connection.py # Gestion de la base de données SQLite 
├── read_activite.py # Lecture et traitement des fichiers d'activités 
├── read_commande.py # Lecture et traitement des fichiers de commandes 
├── read_commande_activite.py # Lecture des commandes liées aux activités 
├── insert_client.py # Ajout des données clients à la base de données 
├── create_temp_folder.py # Création de dossiers temporaires pour les fichiers Excel 
├── eviesens.db # Base de données SQLite 
├── README.md # Documentation du projet 
├── requirements.txt # Liste des dépendances Python 
├── donnees/ # Dossier contenant les fichiers d'entrée (CSV, Excel) 
└── output/ # Dossier pour les fichiers générés par l'application

### Description des fichiers principaux
- **`application.py`** : Interface utilisateur principale utilisant `tkinter`.
- **`main.py`** : Point d'entrée pour exécuter les scripts d'importation et de traitement.
- **`kpi.py`** : Calcul des indicateurs clés de performance (KPI).
- **`visualisation.py`** : Génération de graphiques pour les KPI.
- **`database_connection.py`** : Gestion de la base de données SQLite.
- **`read_activite.py`, `read_commande.py`, `read_commande_activite.py`** : Scripts pour lire et traiter les fichiers d'activités, commandes, et commandes-activités.
- **`insert_client.py`** : Ajout des données clients à la base de données.
- **`create_temp_folder.py`** : Création de dossiers temporaires pour traiter les fichiers Excel.
- **`eviesens.db`** : Fichier de base de données SQLite contenant les données importées.

### Dossiers
- **`donnees/`** : Contient les fichiers d'entrée (CSV, Excel) nécessaires à l'importation.
- **`output/`** : Contient les fichiers générés par l'application, comme les graphiques ou rapports.

---
