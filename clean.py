import pandas as pd

if __name__ == "__main__":

    df = pd.read_csv("final.csv", index_col=0)

    for row in df[:5]:
        print(df[row])