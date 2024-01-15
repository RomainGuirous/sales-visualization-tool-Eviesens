import pandas as pd
pd.set_option('display.max_rows', 500)
import os
import sys
import shutil
import database_connection as dbc
import app_data_connection as adc
import matplotlib.pyplot as plt
import tkinter as tk
from tkinter import ttk
from tkinter import filedialog as fd
from tkinter.messagebox import askyesno
from tkinter.colorchooser import askcolor
import visualisation as viz
import main
import create_temp_folder as ctf

if not os.path.exists("eviesens.db") :
    dbc.create_database()
    adc.create_app_data()

df_activite=dbc.get_data()

fenetre = tk.Tk()
fenetre.title("Calculs de chiffre d'affaire")
fenetre.geometry("1200x300")
fenetre.minsize(1200, 300)
fenetre.maxsize(1200, 300)

# couleurs
window_color=adc.get_window_color()
frame_color=adc.get_window_color()
label_frame_color=adc.get_frame_color()


fenetre['background']=window_color

frame=tk.Frame(fenetre, bg=frame_color)
frame.place(relx=0.5, rely=0.5, anchor='center')


lf1 = tk.LabelFrame(frame, text="Mois", padx=20, pady=20, bg=label_frame_color)
lf1.grid(row=0, column=0)
lf2 = tk.LabelFrame(frame, text="Annee", padx=20, pady=20, bg=label_frame_color)
lf2.grid(row=0, column=1)
lf3 = tk.LabelFrame(frame, text="Fonction", padx=20, pady=20, bg=label_frame_color)
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
        main.execute(filename.name)


def select_directory():
    filename = fd.askdirectory(
        title='choisir un dossier',
        initialdir=os.getcwd())
    if filename :
        main.execute(filename)


def select_excel():
    filetypes = (
        ('text files', '*.xlsx'),
        ('All files', '*.*')
    )

    filename = fd.askopenfile(
        title='choisir un fichier',
        initialdir=os.getcwd(),
        filetypes=filetypes)
    if filename :
        ctf.create_folder(filename.name)
        main.execute("temp_csv")
        if os.path.isdir("temp_csv"):
            shutil.rmtree("temp_csv")

def delete_all() :
    answer = askyesno(title='confirmation',
                    message='voulez-vous supprimer les données ?')
    if answer :
        dbc.delete_database()

def change_window_color() :
    colors = askcolor(title="couleur")
    fenetre.configure(bg=colors[1])
    frame.configure(bg=colors[1])
    adc.update_window_color(colors[1])

def change_frame_color():
    colors = askcolor(title="couleur")
    lf1.configure(bg=colors[1])
    lf2.configure(bg=colors[1])
    lf3.configure(bg=colors[1])
    adc.update_frame_color(colors[1])

def restore_default_color() :
    adc.restore_default_app_data()
    fenetre.configure(bg=adc.get_window_color())
    frame.configure(bg=adc.get_window_color())
    lf1.configure(bg=adc.get_frame_color())
    lf2.configure(bg=adc.get_frame_color())
    lf3.configure(bg=adc.get_frame_color())

menubar = tk.Menu(fenetre)

import_menu = tk.Menu(menubar, tearoff=0)
menubar.add_cascade(label="importer", menu=import_menu)
import_menu.add_command(label="ajouter un fichier csv", command=select_file)
import_menu.add_command(label="ajouter un dossier de csv", command=select_directory)
import_menu.add_command(label="ajouter un fichier xlsx", command=select_excel)

option_menu = tk.Menu(menubar, tearoff=0)
menubar.add_cascade(label="options", menu=option_menu)
option_menu.add_command(label="supprimer la base de donnee", command=delete_all)

color_menu = tk.Menu(menubar, tearoff=0)
menubar.add_cascade(label="changer les couleurs", menu=color_menu)
color_menu.add_command(label="fond", command=change_window_color)
color_menu.add_command(label="interieur", command=change_frame_color)
color_menu.add_command(label="restaurer les couleurs par defaut", command=restore_default_color)

fenetre.config(menu=menubar)

fenetre.mainloop()