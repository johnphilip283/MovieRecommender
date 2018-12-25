import pandas as pd


def clean_rating(row):

    if row["Rated"] in ("N/A", "NOT RATED", "PASSED", "UNRATED", "APPROVED"):
        row["Rated"] = ""

    if str(row["Rotten Tomatoes"]).endswith("%"):
        row["Rotten Tomatoes"] = row["Rotten Tomatoes"][:-1]

    if "/" in str(row["Metacritic"]):
        row["Metacritic"] = row["Metacritic"].split("/")[0]

    return row


if __name__ == "__main__":

    df = pd.read_csv("final.csv", index_col=0)

    df = df[[x for x in df.columns.tolist() if x not in ("Internet Movie Database", "Runtime")]]

    df.apply(clean_rating, axis=1)

    df.to_csv("final.csv")

    df[:int(len(df) / 2)].to_csv("souped_up_movie_dataset_1.csv")

    df[int(len(df) / 2):].to_csv("souped_up_movie_dataset_2.csv")
