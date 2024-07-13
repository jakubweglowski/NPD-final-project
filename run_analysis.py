import argparse
import pandas as pd
from datetime import datetime

from final_files.task1 import fullTask
from final_files.task2 import prepareDataset2, hegemonyAnalysis
from final_files.task3 import prepareDataset3, performAnalysis

parser = argparse.ArgumentParser(
    description=
    "IMDb analysis; \n\nTask 1: given some top number of best movies, create ranking of countries; \n\nTask 2: given all movies and two measures of cinematic impact, create film hegemony dataframe; \n\nTask 3: given all directors with a given minimal number of movies, born after a given year and still alive, check whether the ones that are also actors are better rated"
)
parser.add_argument("data_folder",
                    type=str, 
                    help="""Path to folder containing files:
                    'title.akas.tsv.gz',
                    'title.basics.tsv.gz',
                    'title.ratings.tsv.gz',
                    'name.basics.tsv.gz'""")

parser.add_argument("--start_year",
                    type=int,
                    help="Specify beginning of the period of analysis. If not given, defaults to the smallest possible.")

parser.add_argument("--end_year",
                    type=int,
                    help="Specify end of the period of analysis. If not given, defaults to the current year.")

parser.add_argument("--minNumVotes",
                    type=int,
                    help="Specify how many votes a movie has to have to be included in analysis. If not given, defaults to 5000.")

parser.add_argument("--answerFile",
                    type=str,
                    help="[for Task 1] Specify path to file where the movie rankings will be saved. If not given, rankings are printed to the console.")

parser.add_argument("--minBirthYear",
                    type=int,
                    help="[for Task 3] Specify year of birth of the oldest director to be included in analysis. If not given, defaults to 1970")

parser.add_argument("--minMovies",
                    type=int,
                    help="[for Task 3] Specify minimal number of movies a director in known for to be included in analysis. If not given, defaults to 4.")

args = parser.parse_args()

if args.data_folder[-1] != "/":
    args.data_folder += "/"
    
if args.start_year is None:
    start = 1800
else:
    start = args.start_year
    
if args.end_year is None:
    end = datetime.now().year
else:
    end = args.end_year
assert start <= end and start >= 1800 and end >= 1800, "Incorrect period of analysis"
    
if args.minNumVotes is None:
    K = 5000
else:
    K = args.minNumVotes
assert K >= 0, "Minimal number of votes has to be non-negative integer."
    
if args.minBirthYear is None:
    Y = 1970
else:
    Y = args.minBirthYear
    assert Y < end, "Directors' birth year has to be smaller then end of period of analysis."
    
if args.minMovies is None:
    n_films = 4
else:
    n_films = args.minMovies
assert n_films >= 0, "Minimal number of movies has to be non-negative integer."
    
################################
# Task 1
print("Task 1...")
path1 = args.data_folder + "title.akas.tsv.gz"
path2 = args.data_folder + "title.basics.tsv.gz"
path3 = args.data_folder + "title.ratings.tsv.gz"
fullTask(path1, path2, path3, start, end, Ks = [K], verbose=True, file=args.answerFile)

################################
# Task 2
# now we automatically save dataframes to .csv, we do not ask if this should be done like in Task 1
print("Task 2...")
df_analysis, end = prepareDataset2(path1, path2, path3, start, end, verbose=True)
ha = hegemonyAnalysis(df_analysis, end, K, verbose=True)
ha['GDP'].to_csv('task2_gdp.csv', sep=';', mode='w', encoding='utf-8')
ha['Population'].to_csv('task2_population.csv', sep=';', mode='w', encoding='utf-8')
ha['GDP PC'].to_csv('task2_gdppc.csv', sep=';', mode='w', encoding='utf-8')

################################
# Task 3
print("Task 3...")
path1 = args.data_folder + "name.basics.tsv.gz"
path2 = args.data_folder + "title.ratings.tsv.gz"
path3 = args.data_folder + "title.basics.tsv.gz"

df1, df2 = prepareDataset3(path1, path2, path3, Y, K, n_films, verbose = True)
performAnalysis(df1, df2, verbose = True).to_csv('task3.csv', sep=';', mode='w', encoding='utf-8')
