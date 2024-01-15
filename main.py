import read_activite
import read_commande
import read_commande_activite

def execute(folder_filepath) :
    print("read_activite.py")
    read_activite.read_activite(folder_filepath)

    print("read_commande.py")
    read_commande.read_commande(folder_filepath)

    print("read_commande_activite.py")
    read_commande_activite.read_commande_activite(folder_filepath)