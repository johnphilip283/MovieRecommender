drop table if exists movie;
create table movie (
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

drop table if exists director;
create table director (
	director_id integer primary key,
  name        varchar(50)
);

drop table if exists movie_has_director;
create table movie_has_director (
	movie_id    integer not null,
	director_id integer not null,
  foreign key (movie_id) references movies (movie_id),
  foreign key (director_id) references directors (director_id)
);

drop table if exists genre;
create table genre (
	genre_id integer primary key,
  name     varchar(50)
);

drop table if exists movie_has_genre;
create table movie_has_genre (
	movie_id integer not null,
  genre_id integer not null,
  foreign key (movie_id) references movies (movie_id),
  foreign key (genre_id) references genres (genre_id)
);

drop table if exists actor;
create table actor (
	actor_id integer primary key,
  name     varchar(50)
);

drop table if exists movie_has_actor;
create table movie_has_actor (
	movie_id integer not null,
  actor_id integer not null,
  foreign key (movie_id) references movies (movie_id),
  foreign key (actor_id) references actors (actor_id)
);