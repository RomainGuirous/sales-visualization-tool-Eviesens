from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import kpi

### CHIFFRE D'AFFAIRE ###
##  CA PAR ATELIER / AN
def show_atelier_an(df, mois, annee, df_all_activite) :
    df_atelier_an=kpi.CA_atelier_an(df, annee)
    fig, ax = plt.subplots()
    y=df_atelier_an["activite_nom"]
    x=df_atelier_an["chiffre_affaire"]
    bars=ax.barh(y, x)

    ax.bar_label(bars)
    plt.gcf().subplots_adjust(left=.25)
    ax.set_title(f"Chiffre d'affaire annuel ({annee}) par atelier")
    plt.show()

# ## CA PAR ATELIER / MOIS
def show_atelier_mois(df, mois, annee, df_all_activite) :
    df_atelier_mois=kpi.CA_atelier_mois(df,mois,annee) #mois,annee à adapter
    fig, ax = plt.subplots()
    y=df_atelier_mois["activite_nom"]
    x=df_atelier_mois["chiffre_affaire"]
    bars=ax.barh(y, x)

    ax.bar_label(bars)
    plt.gcf().subplots_adjust(left=.25)
    de_ou_d = "d'" if mois in (4, 8, 10) else "de "
    ax.set_title(f"Chiffre d'affaire du mois {de_ou_d}{ kpi.l_mois[mois] } {annee} par atelier")
    plt.show()

# CA / VENDEUR
def show_vendeur_an(df, mois, annee, df_all_activite) :
    df_vendeur_an=kpi.CA_vendeur_an(df, annee)
    fig, ax = plt.subplots()
    y=df_vendeur_an["vendeur_nom"]
    x=df_vendeur_an["chiffre_affaire"]
    bars=ax.bar(y, x)

    ax.bar_label(bars)
    plt.gcf().subplots_adjust(left=.25)
    ax.set_title(f"Chiffre d'affaire annuel ({annee}) par vendeur")
    plt.show()

## CA / (VENDEUR, ATELIER)
def show_vendeur_atelier_an(df, mois, annee, df_all_activite) :
    df_vendeur_atelier_an=kpi.CA_vendeur_atelier_an(df, annee)
    fig, ax = plt.subplots()
    l_vendeurs=df_vendeur_atelier_an["vendeur_nom"].to_list() #recupere les noms des vendeurs
    l_vendeurs=list(dict.fromkeys(l_vendeurs)) # retire les doublons

    for v in l_vendeurs :
        df_temp = df_vendeur_atelier_an[df_vendeur_atelier_an["vendeur_nom"]==v] # le dataframe correspondant au vendeur
        y=df_temp["activite_nom"].to_list()
        x=df_temp["chiffre_affaire"].values.tolist()
        bars=ax.barh(y, x, label=v) # cree les barres correspondantes et rajoute la legende associee
        ax.bar_label(bars)

    ax.legend()
    plt.gcf().subplots_adjust(left=.25)
    ax.set_title(f"Chiffre d'affaire annuel ({annee}) par atelier et par vendeur")
    plt.show()

### NOMBRE ACHAT ###
## NA ATELIER / AN
def show_nbr_atelier_an(df, mois, annee, df_all_activite) :
    df_nbr_atelier_an = kpi.nbr_atelier_an(df, annee, df_all_activite)
    fig, ax = plt.subplots()
    y=df_nbr_atelier_an["activite_nom"]
    x=df_nbr_atelier_an["nbr_ateliers"]
    bars=ax.barh(y, x)

    ax.bar_label(bars)
    plt.gcf().subplots_adjust(left=.27)
    ax.set_title(f"nombre d'ateliers commandes ({annee})")
    plt.show()

### NOMBRE ACHAT ###
## NA ATELIER / MOIS
def show_nbr_atelier_mois(df, mois, annee, df_all_activite) :
    df_nbr_atelier_an = kpi.moy_personne_atelier_an(df, annee)
    fig, ax = plt.subplots()
    y=df_nbr_atelier_an["activite_nom"]
    x=df_nbr_atelier_an["nbr_gens"]
    bars=ax.barh(y, x)

    ax.bar_label(bars)
    plt.gcf().subplots_adjust(left=.27)
    ax.set_title(f"nombre de personnes en moyenne ayant participe aux ateliers ({annee})")
    plt.show()

### CA ANNUEL POUR CHAQUE MOIS ###
def show_CA_annuel(df, mois, annee, df_all_activite) :
    df_CA_annuel = kpi.CA_annuel(df, annee)
    fig, ax = plt.subplots()
    x=df_CA_annuel["mois"]
    y=df_CA_annuel["chiffre_affaire"]
    bars=ax.bar(x, y)

    ax.bar_label(bars)
    ax.set_title(f"chiffre d'affaire annuel ({annee})")
    plt.show()

### REVENU NET ANNUEL POUR CHAQUE MOIS ###
def show_revenu_net_annuel(df, mois, annee, df_all_activite) :
    df_revenu_annuel = kpi.revenu_net_annuel(df, annee)
    fig, ax = plt.subplots()
    x=df_revenu_annuel["mois"]
    y=df_revenu_annuel["revenu_net"]
    bars=ax.bar(x, y)

    ax.bar_label(bars)
    ax.set_title(f"revenu net annuel ({annee})")
    plt.show()

### LISTE DES PLUS GROS ACHETEURS
def show_CA_par_client(df, mois, annee, df_all_activite) :
    df_CA_par_client=kpi.CA_par_client(df, annee)
    df_CA_par_client=df_CA_par_client.head(30)

    fig, ax = plt.subplots()

    fig.patch.set_visible(False)
    ax.axis('off')

    ax.set_title(f"top 30 des clients ayant achete des produits ({annee})")
    table = ax.table(cellText=df_CA_par_client.values, colLabels=["nom", "prenom", "argent depensé"], bbox=[0, 0, 1, 1])
    plt.show()