# sales-visualization-tool-Eviesens

Ce projet a pour but d'utiliser des feuilles excel (ou CSV) préformatées (format préexistant) et en tirer des fraphiques selon certains KPI

# instructions
Version: python 3.12.1

Mise en place:
*) Mettre l'application (.exe) dans un dossier (car il va créer d'autre fichiers)

Utilisation
*) importer :
ajouter un fichier csv : ajoute le contenu du csv a la base de donnée
ajouter un dossier de csv : ajoute les csv contenu dans dossier a la base de donnée
ajouter un fichier xlsx : lit le fichier excel et ajoute le contenu a la base de donnée. chaque feuille de calcul correspond a un mois et une année

*) option :
supprimer la base de donnée : supprime le contenu de la base de donnée
=> si un fichier est importer plusieurs fois, il sera dupliqué, ce qui faussera les résultats, donc il faut supprimer la base et tout réimporter
(dès qu'il y a erreur d'importation, supprimer base de donnée et réimporter)

Informations
*) Colonnes E-Q (E et Q comprises) => si donnée absentes, ligne non lue

*) Lorsqu'un Remboursement est effectué, mettre une "Date soin", fictive, pour que la donnée soit lue

*) Tableau colonnes Y,Z,AA,AB => ne jamais effacer de ligne (sauf si l'intitulé n'est pas proposé à la vente)
