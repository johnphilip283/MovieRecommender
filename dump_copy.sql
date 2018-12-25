BEGIN TRANSACTION;
CREATE TABLE actor (
	actor_id integer primary key,
  name     varchar(50)
);
CREATE TABLE director (
	director_id integer primary key,
  name        varchar(50)
);
CREATE TABLE genre (
	genre_id integer primary key,
  name     varchar(50)
);
CREATE TABLE movie (
	movie_id              integer primary key,
	origin                varchar(50),
  year                  year,
  title                 varchar(100),
  wiki_link             varchar(100),
  plot                  text,
  imdb_rating           decimal,
  imdb_votes            integer,
  rated                 varchar(5),
  rotten_tomato_rating  int,
  metacritic_rating     int
);
CREATE TABLE movie_has_actor (
	movie_id integer not null,
  actor_id integer not null,
  foreign key (movie_id) references movies (movie_id),
  foreign key (actor_id) references actors (actor_id)
);
CREATE TABLE movie_has_director (
	movie_id    integer not null,
	director_id integer not null,
  foreign key (movie_id) references movies (movie_id),
  foreign key (director_id) references directors (director_id)
);
CREATE TABLE movie_has_genre (
	movie_id integer not null,
  genre_id integer not null,
  foreign key (movie_id) references movies (movie_id),
  foreign key (genre_id) references genres (genre_id)
);
COMMIT;
