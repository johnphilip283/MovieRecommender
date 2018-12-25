import pandas as pd
import csv
import os
import datetime
import requests
from time import sleep

API_URL = "http://www.omdbapi.com"
API_KEY = "d8a2854c"


def ping_api(movie_name, year=None, api_key=API_KEY):

    request_params = {'apikey': api_key, 'type': 'movie', 't': movie_name, 'plot': 'full'}

    if year:
        request_params['y'] = year

    r = requests.get(API_URL, params=request_params)

    return r.json()


# Concatenate the dataframes into one big dataset.
df = pd.concat([pd.read_csv("wiki_dataset_1.csv", index_col=0),
                pd.read_csv("wiki_dataset_2.csv", index_col=0)], ignore_index=True)

# Create a CSV that I can use to read the data.
df.to_csv("intermediate.csv")

imputation_targets = ("Unknown", "N/A", "unknown", "")

# Imputes missing data from the wikipedia plot summary dataset with OMDB-scraped data, and supplements it with
# extra data.
with open("intermediate.csv") as read_file, open("final.csv", 'a') as write_file:

    reader = csv.DictReader(read_file)

    supplementary_data = ["Runtime", "imdbRating", "imdbVotes", "Rated", "Rotten Tomatoes", "Metacritic"]

    writer = csv.DictWriter(write_file, fieldnames=df.columns.tolist() + supplementary_data)
    writer.writeheader()
    movie_json = {}

    for row in reader:

        row = dict(row)

        try:
            # Ping the API
            movie_json = ping_api(row["Title"],
                                  year=row["Release Year"] if row["Release Year"] not in imputation_targets else None,
                                  api_key=API_KEY)

            # If the movie doesn't exist in the OMDB database, then skip this row, since we can't add any extra data
            # to it.
            if movie_json["Response"] == "False":

                # Repeat the queries
                if movie_json["Error"] == "Request limit reached!":
                    print("Timed out: JSON response is: {}".format(movie_json))

                    sleep(3600 * 24)
                    break

                elif movie_json["Error"] == "Movie not found!":
                    print("Not found: JSON response is: {}".format(movie_json))

                print("Empty API response for {}. ".format(row["Title"]))

                continue

            print("{}:\t\tImputing data for {}.".format(str(datetime.datetime.now()), row["Title"]))

            # If any of these fields are imputable, impute them.
            if row["Director"] in imputation_targets:
                row["Director"] = movie_json["Director"] if movie_json["Director"] != "N/A" else ""

            if row["Release Year"] in imputation_targets:
                row["Release Year"] = movie_json["Year"]

            if row["Genre"] in imputation_targets:
                row["Genre"] = movie_json["Genre"]

            if row["Cast"] in imputation_targets:
                row["Cast"] = movie_json["Actors"] if movie_json["Actors"] != "N/A" else ""

            if row["Plot"] in imputation_targets:
                row["Plot"] = movie_json["Plot"]

            # Supplement with additional data
            row["imdbRating"] = movie_json["imdbRating"] if movie_json["imdbRating"] != "N/A" else ""
            row["imdbVotes"] = movie_json["imdbVotes"] if movie_json["imdbVotes"] != "N/A" else ""
            row["Rated"] = movie_json["Rated"] if movie_json["Rated"] not in ("N/A", "NOT RATED", "PASSED", "UNRATED", "APPROVED") else ""

            # Go through the ratings list, and add the appropriate fields.
            if "Ratings" in movie_json:
                for rating in movie_json["Ratings"]:
                    row[rating["Source"]] = rating["Value"] if rating["Source"] in supplementary_data else ""

            writer.writerow({key: val for key, val in row.items() if key != ""})

        except KeyError as e:

            print("Movie JSON: {}".format(movie_json))
            print("Row: {}".format(row) + "\n")
            print("{}:\t\tRan into a key error for row {}.".format(str(datetime.datetime.now()), row["Title"]))

        except (KeyboardInterrupt, Exception):

            print("Writing files ...")
            break

df = pd.read_csv("final.csv", index_col=0)

# Write out the new dataframe to a CSV.
df[:int(len(df) / 2)].to_csv("souped_up_movie_dataset_1.csv")

df[int(len(df) / 2):].to_csv("souped_up_movie_dataset_2.csv")

if os.path.isfile("intermediate.csv"):
    os.remove("intermediate.csv")
