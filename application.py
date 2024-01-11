import pandas as pd
pd.set_option('display.max_rows', 500)
import os
import sys
import subprocess
from sqlalchemy import create_engine
import database_connection as dbc
import matplotlib.pyplot as plt
import tkinter as tk
from tkinter import ttk
from tkinter import filedialog as fd
from tkinter.messagebox import askyesno
from tkinter.colorchooser import askcolor
import visualisation as viz

if not os.path.exists("eviesens.db") :
    dbc.create_database()

df_activite=dbc.get_data()

fenetre = tk.Tk()
fenetre.title("Calculs de chiffre d'affaire")
fenetre.geometry("1800x300")
fenetre.minsize(1800, 300)
fenetre.maxsize(1800, 300)

#police ecriture
font_titre_lf = tkFont.Font(family="Calibri", size=16, weight="bold", slant="italic",underline=True)
font_liste_lf = tkFont.Font(family="Mongolian Baiti", size=12, weight="bold", slant="roman")
font_button = tkFont.Font(family="Arial", size=16, weight="bold", slant="italic",underline=False)

#curseur
curseur="heart"

# couleurs
window_color="#dff9fb"
frame_color="#dff9fb"
label_frame_color="#badc58"
button_color="#f3a683"
window_color="#6c5ce7"
frame_color="#6c5ce7"
label_frame_color="#d63031"
button_color="#a29bfe"
button_color_text='#2d3436'
button_color_push='#74b9ff'
text_label_frame_color='#2d3436'
titre_label_frame_color='#2d3436'


fenetre['background']=window_color

frame=tk.Frame(fenetre, bg=frame_color, cursor=curseur)
frame.place(relx=0.5, rely=0.5, anchor='center')



lf1 = tk.LabelFrame(frame, text="Mois", padx=20, pady=20, bg=label_frame_color,font=font_titre_lf, cursor=curseur,foreground=titre_label_frame_color)
lf1.grid(row=0, column=0)
# lf1.configure(font=font1)
lf2 = tk.LabelFrame(frame, text="Annee", padx=20, pady=20, bg=label_frame_color,font=font_titre_lf, cursor=curseur,foreground=titre_label_frame_color)
lf2.grid(row=0, column=1)
lf3 = tk.LabelFrame(frame, text="Fonction", padx=20, pady=20, bg=label_frame_color,font=font_titre_lf, cursor=curseur,foreground=titre_label_frame_color)
lf3.grid(row=0, column=2)

lframe_max_width=50

# liste
lmois=["janvier", "fevrier", "mars", "avril", "mai", "juin", "juillet", "aout", "septembre", "octobre", "novembre", "decembre"]
mois = ttk.Combobox(lf1, exportselection=0, width=lframe_max_width, values=lmois, state='readonly')
mois.current(0)
mois.pack()

annee = ttk.Combobox(lf2, exportselection=0, width=lframe_max_width, values=dbc.load_annee(), state='readonly')
annee.pack()

# fonctions
dfonctions={
    "chiffre d'affaire annuel par atelier" : viz.show_atelier_an,
    "chiffre d'affaire du mois par atelier" : viz.show_atelier_mois,
    "chiffre d'affaire annuel par vendeur" : viz.show_vendeur_an,
    "chiffre d'affaire annuel par atelier et par vendeur" : viz.show_vendeur_atelier_an,
    "nombre d'ateliers commandes" : viz.show_nbr_atelier_an,
    "nombre de personne en moyenne par atelier" : viz.show_nbr_atelier_mois,
    "chiffre d'affaire annuel par mois" : viz.show_CA_annuel,
    "revenu net par mois" : viz.show_revenu_net_annuel,
    "chiffre d'affaire max par client" : viz.show_CA_par_client
}
fonction = ttk.Combobox(lf3, exportselection=0, width=lframe_max_width, values=list(dfonctions.keys()), state='readonly')
fonction.current(0)
fonction.pack()

# script selection
def selected_item(event):
    try:
        m = int(mois.current())+1
        a = int(annee.get())
        f = dfonctions[fonction.get()]
        f(df_activite, m, a, dbc.get_type_activite())
        plt.show()
    except ValueError as ve:
        pass
    

 
mois.bind('<<ComboboxSelected>>', selected_item)
annee.bind('<<ComboboxSelected>>', selected_item)
fonction.bind('<<ComboboxSelected>>', selected_item)

# boutons
def select_file():
    filetypes = (
        ('text files', '*.csv'),
        ('All files', '*.*')
    )

    filename = fd.askopenfile(
        title='choisir un fichier',
        initialdir=os.getcwd(),
        filetypes=filetypes)
    if filename :
        subprocess.call([sys.executable, "main.py", filename.name])
        fenetre.destroy()
        subprocess.call([sys.executable, "application.py"])

def select_directory():
    filename = fd.askdirectory(
        title='choisir un dossier',
        initialdir=os.getcwd())
    if filename :
        subprocess.call([sys.executable, "main.py", filename])
        fenetre.destroy()
        subprocess.call([sys.executable, "application.py"])

def delete_all() :
    answer = askyesno(title='confirmation',
                    message='voulez-vous supprimer les données ?')
    if answer :
        dbc.delete_database()
        fenetre.destroy()
        subprocess.call([sys.executable, "application.py"])

def change_window_color():
    colors = askcolor(title="couleur")
    fenetre.configure(bg=colors[1])
    frame.configure(bg=colors[1])

def change_frame_color():
    colors = askcolor(title="couleur")
    lf1.configure(bg=colors[1])
    lf2.configure(bg=colors[1])
    lf3.configure(bg=colors[1])

menubar = tk.Menu(fenetre)

import_menu = tk.Menu(menubar, tearoff=0)
menubar.add_cascade(label="importer", menu=import_menu)
import_menu.add_command(label="ajouter un fichier", command=select_file)
import_menu.add_command(label="ajouter un dossier", command=select_directory)

option_menu = tk.Menu(menubar, tearoff=0)
menubar.add_cascade(label="options", menu=option_menu)
option_menu.add_command(label="supprimer la base de donnee", command=delete_all)

color_menu = tk.Menu(menubar, tearoff=0)
menubar.add_cascade(label="changer les couleurs", menu=color_menu)
color_menu.add_command(label="fond", command=change_window_color)
color_menu.add_command(label="interieur", command=change_frame_color)

fenetre.config(menu=menubar)

fenetre.mainloop()
