import pandas as pd
pd.set_option('display.max_rows', 500)


filepath="./donnees/Fiche de compte annuelle Eviesens - Janvier-2023.csv"
df=pd.read_csv(filepath)
df_activite = df.iloc[:,24:28]
df_activite=df_activite.dropna()
print(df_activite)













# df["date_formatee"]=pd.to_datetime(df["date"], format="%d/%m/%Y")