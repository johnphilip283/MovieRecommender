import pandas as pd
import sqlite3

conn = sqlite3.connect(':memory:')


def query_for_join_table(type, movie_id, val_id):

    return """
           insert into movie_has_{0} (movie_id, {0}_id) values ({1}, {2})
           """.format(type, movie_id, val_id)


def query(query_string, conn=conn):

    cur = conn.cursor()
    cur.execute(query_string)

    return cur.fetchall()


def id_from_name(name, table):

    return query('select {0}_id from {0} where name="{1}"'.format(table, name.strip().title()))


def add_name_to_table(name, table):

    query('insert into {0} (name) values ("{1}")'.format(table, name.strip().title()))

    return id_from_name(name, table)


def get_id(val, table):

    val = val.lower().title()

    ids = id_from_name(val, table)

    if len(ids) == 0:

        # val doesn't exist yet, so insert them, then get back their id
        ids = add_name_to_table(val, table)

    # We should be guaranteed to have one entry in ids
    assert len(ids) == 1

    return int(ids[0][0])


if __name__ == "__main__":

    # Setting up the main database, loading in the table schema
    with open("movie.sql") as f:
        script = f.read()

    cur = conn.cursor()
    cur.executescript(script)

    # Start reading through the big CSV
    df = pd.read_csv("final.csv")

    for index, row in df.iterrows():

        row = {key: (str(val).replace('"', '') if str(val) != "nan" else "") for key, val in dict(row).items()}

        movie_id = index + 1

        print("Working on movie #{}: {}".format(movie_id, row["Title"]))

        try:
            INSERT_MOVIE_QUERY = 'insert into movie (movie_id, origin, year, title, wiki_link, plot, imdb_rating, imdb_votes, rated, rotten_tomato_rating, metacritic_rating) values ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})'.format(movie_id,
                                           '"{}"'.format(row["Origin/Ethnicity"].strip().title()) if row["Origin/Ethnicity"] else "NULL",
                                           int(row["Release Year"]) if row["Release Year"] else "NULL",
                                           '"{}"'.format(row["Title"].strip().title()) if row["Title"] else "NULL",
                                           '"{}"'.format(row["Wiki Page"]) if row["Wiki Page"] else "NULL",
                                           '"{}"'.format(row["Plot"].strip().capitalize()) if row["Plot"] else "NULL",
                                           float(row["imdbRating"]) / 10 if row["imdbRating"] else "NULL",
                                           row["imdbVotes"].replace(",", "") if row["imdbVotes"] else "NULL",
                                           '"{}"'.format(row["Rated"].strip()) if row["Rated"] else "NULL",
                                           int(row["Rotten Tomatoes"]) / 100 if row["Rotten Tomatoes"] else "NULL",
                                           int(row["Metacritic"]) / 100 if row["Metacritic"] else "NULL")
        except ValueError as e:

            # There is a strange row in the dataset that just has the header names repeated again. If we run into this,
            # then move onto the next row. If that isn't the problem, then raise the exception again.
            if row["Release Year"] == "Release Year":
                continue
            else:
                raise e

        query(INSERT_MOVIE_QUERY)

        for label, table in [("Director", "director"), ("Cast", "actor"), ("Genre", "genre")]:
            if row[label]:
                val_list = row[label].strip().split(',')
                for val in val_list:
                    if val:
                        query(query_for_join_table(table, movie_id, get_id(val, table)))

    with open('dump.sql', 'w') as f:
        for line in conn.iterdump():
            f.write('%s\n' % line)
