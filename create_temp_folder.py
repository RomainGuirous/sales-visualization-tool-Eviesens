import pandas as pd
import re
import shutil
import os
import sys

file_filepath=sys.argv[1]
print(file_filepath)

if os.path.isdir("temp_csv"):
    shutil.rmtree("temp_csv")

os.mkdir('temp_csv')

def is_valid_filename(filename) :
    contains_month=bool(re.search(r"(janvier|f(e|é)vrier|mars|avril|mai|juin|juillet|ao(u|û)t|septembre|octobre|novembre|d(e|é)cembre)", filename.lower()))
    contains_year=bool(re.search(r"[0-9]{4}", filename))
    if contains_month & contains_year :
        return True
    return False

xlsx_file = pd.ExcelFile("donnees/Copie de Fiche de compte annuelle Eviesens.xlsx")
list_sheets=xlsx_file.sheet_names

sheets_to_keep=[]
for i in range(len(list_sheets)) :
    if is_valid_filename(list_sheets[i]) :
        sheets_to_keep.append(list_sheets[i])

for i in range(len(sheets_to_keep)) :
    df = xlsx_file.parse(sheets_to_keep[i])
    df.to_csv ('temp_csv/'+sheets_to_keep[i]+'.csv', index = None, header=True)