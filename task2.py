import wbgapi as wb
import pandas as pd
from datetime import datetime

from importlib import reload

import task1
reload(task1)
from task1 import *

import countries as ct

def WorldBankRank(indicator: str, year = datetime.now().year-1):
    
    countries_df = wb.economy.DataFrame()
    countries = dict(countries_df.loc[countries_df["aggregate"] == 0].iloc[:, 0])
    
    if indicator.lower() == 'gdp':
        df = wb.data.DataFrame(['NY.GDP.MKTP.CD'], time=range(year, year+1)).iloc[:, 0].dropna()
    elif indicator.lower() == 'gdp pc':
        df = wb.data.DataFrame(['NY.GDP.PCAP.CD'], time=range(year, year+1)).iloc[:, 0].dropna()
    elif indicator.lower() == 'population':
        df = wb.data.DataFrame(['SP.POP.TOTL'], time=range(year, year+1)).iloc[:, 0].dropna()
        
    df = df.loc[df.index.isin(list(countries.keys()))]
    df.index = pd.Index([countries[x] for x in df.index])

    df_rank = list(df.sort_values(ascending=False).index)
    
    # ujednolicenie nazw krajów względem IMDb
    df_rank[df_rank.index("Russian Federation")] = "Russia"
    df_rank[df_rank.index("Korea, Rep.")] = "South Korea"
    df_rank[df_rank.index("Turkiye")] = "Turkey"
    df_rank[df_rank.index("Iran, Islamic Rep.")] = "Iran, Islamic Republic of"
    df_rank[df_rank.index("Hong Kong SAR, China")] = "Hong Kong"
    df_rank[df_rank.index("Macao SAR, China")] = "Macao"
    df_rank[df_rank.index("Slovak Republic")] = "Slovakia"
    
    return df_rank

def hegemonyAnalysis(path1, path2, path3,
                     K: int = 1000,
                     year: int = datetime.now().year-1,
                     verbose: bool = False):
    if verbose:
        print("Loading data", end="... ")
    df, df3 = load_dataframes(path1, path2, path3)
    if verbose:
        print("Done!")
        
    if verbose:
        print("Preprocessing", end="... ")
    df_analysis = prepareDataset(df, df3)
    if verbose:
        print("Done!")
    
    df_analysis = df_analysis.loc[df_analysis["numVotes"] > K]

    if verbose:
        print("Computing cinematic impacts", end="... ")
    # weak cinematic impact
    rank_weak = df_analysis.groupby(by="region")["numVotes"].mean().astype(int)
    rank_weak.index = [ct.countries[x] for x in rank_weak.index]
    rank_weak = list(rank_weak.sort_values(ascending=False).index)
    
    # strong cinematic impact
    df_analysis["score"] = df_analysis["averageRating"] * df_analysis["numVotes"]
    rank_strong = df_analysis.groupby(by="region")["score"].sum()/df_analysis.groupby(by="region")["numVotes"].sum()
    rank_strong.index = [ct.countries[x] for x in rank_strong.index]
    rank_strong = list(rank_strong.sort_values(ascending=False).index)
    
    # list of all countries in both IMDb impact rankings
    country_list = rank_strong.copy()
    country_list.extend(rank_weak)
    country_list = list(set(country_list))
    if verbose:
        print("Done!")

    if verbose:
        print("Computing hegemony dataframes", end="... ")
    # dataframes with hegemonies
    rank_dict = {}
    for indic in ['GDP', 'Population', 'GDP PC']:
        print(f"Analyzing {indic.lower()}...")
        rank_dict[indic] = {}
        rank = WorldBankRank(indic, year)
        for country in country_list:
            try:
                impact_weak = rank.index(country) - rank_weak.index(country)
                impact_strong = rank.index(country) - rank_strong.index(country)
            except ValueError:
                print(f"Country '{country}' not found in {indic.lower()} World Bank Database.")
                impact_weak = pd.NA
                impact_strong = pd.NA
                
            rank_dict[indic][country] = {'weak impact': impact_weak, 'strong impact': impact_strong}
        rank_dict[indic] = pd.DataFrame.from_dict(rank_dict[indic]).T.dropna().sort_index()
    if verbose:
        print("Done!")
        
    return rank_dict
    