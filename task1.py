# from importlib import reload
# import load_data
# reload(load_data)
import numpy as np
import pandas as pd
from load_data import *
import task2
from langdetect import detect
import translitcodec
import codecs

def load_dataframes(path1, path2, path3):
    df1 = load_to_dataframe(path1)
    df1 = df1.drop(columns=["language", "types", "attributes"])

    df2 = load_to_dataframe(path2)
    df2 = df2.loc[:, ["tconst", "titleType"]]
    
    df = df1.merge(df2,
              how="left",
              left_on="titleId",
              right_on="tconst"
    )
    
    # wybieramy tylko "movie" i niektóre kolumny
    df = df.loc[df["titleType"] == "movie", ["titleId", "title", "region", "isOriginalTitle"]]
    
    df3 = load_to_dataframe(path3)
    
    return df, df3

# wspieramy rozpoznawanie języka przez bibliotekę 'langdetect'
def mydetect(x):
    try:
        return detect(x).upper()
    except:
        return pd.NA

# funkcje do .apply()
def myfunc_EC(x):
    # jeśli język to hiszpański, możemy założyć że film jest rzeczywiście z Ekwadoru
    # jeśli nie, to bierzemy nasz najlepszy strzał jako kraj pochodzenia
    # bo inaczej nie jesteśmy w stanie go odzyskać
    if x[5] != 'ES':
        x[2] = x[5]
    return x

def myfunc_TR(x):
    # jeśli język to turecki, możemy założyć że film jest rzeczywiście z Turcji
    # jeśli nie, to bierzemy nasz najlepszy strzał jako kraj pochodzenia
    # bo inaczej nie jesteśmy w stanie go odzyskać
    if x[5] != 'TR':
        x[2] = x[5]
    return x
    
def languageModel(df_analysis):
    
    ret_df = df_analysis.copy()
    
    # usuwamy dziwne filmy z Ekwadoru
    df_EC = ret_df.loc[ret_df["region"] == 'EC'].copy()
    df_EC.loc[:, "detected"] = df_EC["title"].apply(mydetect)
    df_EC = df_EC.apply(myfunc_EC, axis=1).drop(columns=["detected"]).dropna()
    ret_df.loc[df_EC.index] = df_EC
    
    return ret_df

def prepareDataset(df, df3):
    
    # 1 Preprocessing
    #
    # zamieniamy wszystkie tytuły na lowercase i dekodujemy narodowe znaki
    df["title"] = df["title"].astype(str).str.lower().apply(lambda x: codecs.encode(str(x), 'translit/short'))

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
        labels=["title_y", "region_y", "isOriginalTitle", "isOriginalTitle_y"],
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
    
    return df_analysis

def properAnalysis(df_analysis, K = 1000, N = 100):
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
    
    print(f"Top {N} countries - ranking")
    for i, x in enumerate(list(ret_df)):
        try:
            print(f"{i+1}. {task2.countries[x]}")
        except:
            print(f"{i+1}. {x}")
    
    