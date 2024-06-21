import requests
import pandas as pd

def download_file(url: str, filename: str, output_path: str) -> None:
    
    if url[-1] != "/":
        url += "/"
        
    assert filename[-7:] == ".tsv.gz", "File should have a '.tsv.gz' extension specified."
        
    response = requests.get(url+filename, stream=True)
    if response.status_code == 200:
        with open(output_path, "wb") as f:
            f.write(response.raw.read())

def load_to_dataframe(path_to_file: str) -> pd.DataFrame:
    return pd.read_csv(path_to_file,
                       compression="gzip",
                       header=0,
                       sep="\t",
                       on_bad_lines="warn")

if __name__ == "__main__":
    url = "https://datasets.imdbws.com/" # should end with "/"
    filename = "name.basics.tsv.gz"
    output_path = f"data/{filename}"
    
    download_file(url, filename, output_path)
    
    path_to_file = output_path
    df = load_to_dataframe(path_to_file)
    print(df.head())
    
    