import sys
import subprocess

folder_filepath=sys.argv[1] #le premier element est le nom du fichier, le deuxieme element est le premier argument

print("read_activite.py")
subprocess.call([sys.executable, "read_activite.py", folder_filepath])

print("read_commande.py")
subprocess.call([sys.executable, "read_commande.py", folder_filepath])

print("read_commande_activite.py")
subprocess.call([sys.executable, "read_commande_activite.py", folder_filepath])