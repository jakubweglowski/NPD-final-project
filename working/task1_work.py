from importlib import reload
import numpy as np
import pandas as pd
from load_data import *

import countries
reload(countries)
import countries

from langdetect import detect
from unidecode import unidecode

from datetime import datetime

def load_dataframes(path1, path2, path3, start: int = 1800, end = datetime.now().year):
        
    df1 = load_to_dataframe(path1)
    df1 = df1.drop(columns=["language", "types", "attributes"])

    df2 = load_to_dataframe(path2)
    df2 = df2.loc[:, ["tconst", "titleType", "startYear"]].dropna()
    df2 = df2.loc[df2["startYear"].astype(int).between(start, end)]
    
    df = df1.merge(df2,
              how="inner",
              left_on="titleId",
              right_on="tconst"
    )
    
    # wybieramy tylko "movie" i niektóre kolumny
    df = df.loc[df["titleType"] == "movie", ["titleId", "title", "region", "startYear", "isOriginalTitle"]]
    
    df3 = load_to_dataframe(path3)
    
    return df, df3

# wspieramy rozpoznawanie języka przez bibliotekę 'langdetect'
def mydetect(x):
    try:
        return detect(x).upper()
    except:
        return pd.NA

# funkcja do .apply()
# służy do naprawy błędu przyporządkowywania filmu
# jako pochodzącego z Ekwadoru
def myfunc_EC(x):
    # jeśli język to hiszpański, możemy założyć że film jest rzeczywiście z Ekwadoru
    # jeśli nie, to bierzemy nasz najlepszy strzał jako kraj pochodzenia
    # bo inaczej nie jesteśmy w stanie go odzyskać
    if x[5] != 'ES':
        x[2] = x[5]
    return x
    
def languageModel(df_analysis):
    
    ret_df = df_analysis.copy()
    
    # usuwamy dziwne filmy z Ekwadoru, które tak naprawdę nie są z Ekwadoru
    df_EC = ret_df.loc[ret_df["region"] == 'EC'].copy()
    df_EC.loc[:, "detected"] = df_EC["title"].apply(mydetect)
    df_EC = df_EC.apply(myfunc_EC, axis=1).drop(columns=["detected"]).dropna()
    ret_df.loc[df_EC.index] = df_EC
    
    return ret_df

def prepareDataset1(df, df3):
    
    # 1 Preprocessing
    #
    # zamieniamy wszystkie tytuły na lowercase i dekodujemy narodowe znaki
    df["title"] = df["title"].astype(str).str.lower().apply(lambda x: unidecode(str(x)))
    
    # 2 Wydobycie krajów
    #
    # wydostajemy kandydatów na filmy z których odzyskamy kraje
    # zgodnie z uwagą w PDFie od prowadzących
    df_think = df.loc[df["isOriginalTitle"] == 0].merge(
        df.loc[df["isOriginalTitle"] == 1],
        on="titleId",
        how="left",
        suffixes=("", "_y")
    ).query(
        "title == title_y"
    ).drop(
        labels=["title_y", "region_y", "startYear_y", "isOriginalTitle", "isOriginalTitle_y"],
        axis=1
    )

    # grupujemy po ID tytułu
    # potem zliczamy ile było przypisanych regionów
    # tam, gdzie był jeden, daje się zidentyfikować jednoznacznie
    disambig = (df_think.loc[:, ["titleId", "region"]].groupby(
        by=["titleId"]
    ).count()["region"] == 1)
    
    # ta ramka danych zawiera filmy, dla których można było wydostać kraj
    # uwaga: ten kraj może być identyfikowany źle!
    df_with_known_countries = df_think.loc[df_think["titleId"].isin(disambig.index[disambig == True])]

    df_analysis = df_with_known_countries.merge(df3,
                                                how="left",
                                                left_on="titleId",
                                                right_on="tconst"
    ).drop(
        columns=["tconst"]
    ).dropna()
    
    df_analysis = languageModel(df_analysis)
    df_analysis = df_analysis.loc[df_analysis["region"].isin(list(countries.countries.keys()))]
    
    return df_analysis

def properAnalysis(df_analysis, K = 1000, N = 100, file = None):
    ret_df = df_analysis.loc[df_analysis["numVotes"] > K].sort_values(
        by="averageRating", ascending=False
    ).head(
        N
    ).loc[:, ["titleId", "region"]].groupby(
        by="region"
    ).count().sort_values(
        by=["titleId"],
        ascending=False
    ).index
    
    if file is None:
        print(f"Top {N} movies - ranking")
    else:
        with open(file, "a") as f:
            f.write(f"Top {N} movies - ranking\n")
            
    for i, x in enumerate(list(ret_df)):
        if i == 10:
            break
        
        if file is None:
            print(f"{i+1}. {countries.countries[x]}")
        else:
            with open(file, "a") as f:
                f.write(f"{i+1}. {countries.countries[x]}\n")
            
    return [countries.countries[x] for x in list(ret_df)]

def answerRanks(df_analysis, Ks, Ns, start, end, file = None):
    if file is not None:
        with open(file, "w") as f:
            f.write(f"New analysis: {datetime.now()}\n")
            f.write(f"Performed on period from year {start} to year {end}.\n\n")
    rank = {}
    Ns = list(Ns)
    Ns.append(df_analysis.shape[0])
    for K in Ks:
        if file is None:
            print(f"Threshold: {K}")
        else:
            with open(file, "a") as f:
                f.write(f"Threshold: {K}\n")
            
        rank[K] = {}
        for N in Ns:
            pa = properAnalysis(df_analysis, K = K, N = N, file = file)
            rank[K][N] = pa
            if file is None:
                print()
            else:
                with open(file, "a") as f:
                    f.write("\n")
        if file is None:
            print("-"*40)
        else:
            with open(file, "a") as f:
                f.write("-"*40+"\n\n")
    return rank

def fullTask(path1, path2, path3,
             start: int = None,
             end: int = None,
             Ks: list = [5000],
             Ns: list = range(10, 201, 10),
             verbose: bool = False,
             file: str = None):
    
    if verbose:
        print("Loading data", end="... ")
    df, df3 = load_dataframes(path1, path2, path3, start, end)
    if verbose:
        print("Done!")
        
    if verbose:
        print("Preprocessing", end="... ")
    df_analysis = prepareDataset1(df, df3)
    if verbose:
        print("Done!")
        
    if verbose:
        print("Creating ranking", end="...\n\n")
    rank = answerRanks(df_analysis, Ks, Ns, start, end, file)
    if verbose and file is not None:
        print("Done!")
        
    return rank
    
if __name__ == "__main__":  
    rank = fullTask(path1="data/title.akas.tsv.gz",
                    path2="data/title.basics.tsv.gz",
                    path3="data/title.ratings.tsv.gz",
                    Ks = [1000],
                    Ns = range(10, 201, 10),
                    verbose = True,
                    file = None)