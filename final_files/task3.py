from final_files.load_data import *

def prepareDataset3(path1, path2, path3, Y, K, n_films, verbose: bool = False):
    if verbose:
        print("Loading data", end="... ")
    # path_to_file = "data/name.basics.tsv.gz"
    df1 = load_to_dataframe(path1)

    df1.loc[:, 'isDirector'] = df1["primaryProfession"].astype(str).str.contains("director")
    df1.loc[:, 'isActor'] = df1["primaryProfession"].astype(str).str.contains("actor") | df1["primaryProfession"].astype(str).str.contains("actress")
    df1 = df1.loc[df1["isDirector"] == True].drop(columns=["primaryProfession", "isDirector"])
    df1 = df1.loc[(df1["birthYear"].notna()) & (df1["knownForTitles"].notna())]
    df1 = df1.loc[(df1["birthYear"].astype(int) > Y) & (df1["deathYear"].isna())]
    df1 = df1.loc[df1["knownForTitles"].apply(lambda x: len(x.split(",")) >= n_films)]

    # path_to_file = "data/title.ratings.tsv.gz"
    df2 = load_to_dataframe(path2)

    # path_to_file = "data/title.basics.tsv.gz"
    df3 = load_to_dataframe(path3)

    df3 = df3.loc[df3["startYear"].notna()]
    df3 = df3.loc[(df3["titleType"] == 'movie') & (df3["startYear"].astype(int) > Y), ["tconst"]]
    df2 = df2.merge(df3,
                    on="tconst",
                    how="right")
    df2 = df2.dropna().loc[df2["numVotes"] > K]
    if verbose:
        print("Done!")
    
    return df1, df2

##################################################
def performAnalysis(df1, df2, verbose: bool = False):
    if verbose:
        print("Analyzing data", end="... ")
    dict_df = {}
    for num, i in enumerate(df1.index):
        if num % 1500 == 0:
            print(f"Za nami {(num/df1.shape[0])*100:.2f}%")
        knownFor = df1.loc[i, :].knownForTitles.split(",")
        stats = dict(df2.loc[df2["tconst"].isin(knownFor)].loc[:, ["averageRating", "numVotes"]].describe().loc[["mean", "std"], :])
        dict_df[(df1.loc[i, "nconst"], "averageRating")] = dict(stats['averageRating'])
        dict_df[(df1.loc[i, "nconst"], "numVotes")] = dict(stats['numVotes'])
        
    ###################################################

    df = pd.DataFrame.from_dict(dict_df, orient="index").dropna().reset_index()
    df = df.rename(columns={'level_0': "nconst", "level_1": "stat"})
    df_analysis = df.merge(df1.loc[:, ["nconst", "isActor"]], on="nconst", how="left")
    if verbose:
        print("Done!")
    
    return df_analysis.drop(columns=["nconst"]).groupby(by=["stat", "isActor"]).mean()