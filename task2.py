import wbgapi as wb
import pandas as pd

def gdpRank():
    
    gdp = wb.data.DataFrame(['NY.GDP.MKTP.CD'], mrv=1).iloc[:, 0].dropna()
    countries_df = wb.economy.DataFrame()
    countries = dict(countries_df.loc[countries_df["aggregate"] == 0].iloc[:, 0])

    gdp = gdp.loc[gdp.index.isin(list(countries.keys()))]
    gdp.index = pd.Index([countries[x] for x in gdp.index])

    gdp_rank = list(gdp.sort_values(ascending=False).index)
    
    return gdp_rank