####Group6 
###ETL Process


# -- START R CODE --

# Load needed packages
install.packages('RPostgreSQL')
require('RPostgreSQL')

#Load the PostgreSQL driver:
drv <- dbDriver('PostgreSQL')

# list credentials here
hostname = 'cu-spring2020-group6.cggz75b61mlh.us-east-2.rds.amazonaws.com'
username = 'postgres'
pwd = 'postgres'
database = 'postgres'

#Create a connection 

con <- dbConnect(drv, dbname = 'movie_schema_new',
                 host = hostname, port = 5432,
                 user = 'postgres', password = 'postgres')
### CREATE TABLES
stmt = "
CREATE TABLE types(
type_id integer,
type varchar(10),
PRIMARY KEY (type_id)
);

CREATE TABLE tv_ratings(
rating_id integer,
rating varchar(10),
PRIMARY KEY (rating_id)
);

CREATE TABLE genres(
genre_id integer,
listed_in text,
PRIMARY KEY (genre_id)
);

CREATE TABLE actors(
actor_id integer,
actor_name text,
PRIMARY KEY (actor_id)
);

CREATE TABLE countries(
country_id integer,
country varchar(300),
PRIMARY KEY (country_id)
);

CREATE TABLE duration(
duration_id integer,
duration varchar(10),
PRIMARY KEY (duration_id)
);

 CREATE TABLE show (
  show_id integer PRIMARY KEY,
  title text,
  release_year numeric(4,0),
  duration varchar(10),
  description text);
  
  CREATE TABLE date_added(
  show_id integer PRIMARY KEY,
  date_added date,
  FOREIGN KEY (show_id) REFERENCES show (show_id));

  CREATE TABLE customer_rating(
  show_id integer PRIMARY KEY,
  rating_level text);
  
   CREATE TABLE directors (
  director_id integer PRIMARY KEY,
  name varchar(200));
  
  CREATE TABLE movie_director(
  show_id integer,
  director_id integer,
  PRIMARY KEY (show_id, director_id),
  FOREIGN KEY (show_id) REFERENCES show (show_id),
  FOREIGN KEY (director_id ) REFERENCES directors(director_id));
  
CREATE TABLE customer_ratings(
show_id integer PRIMARY KEY,
average_rating numeric(2,1),
votes_number integer,
FOREIGN KEY (show_id) REFERENCES show (show_id)
);

CREATE TABLE show_duration(
show_id integer,
duration_id integer,
PRIMARY KEY (show_id, duration_id),
FOREIGN KEY (show_id) REFERENCES show (show_id),
FOREIGN KEY (duration_id ) REFERENCES duration(duration_id)
);

CREATE TABLE show_genres (
show_id integer,
genre_id integer,
PRIMARY KEY (show_id, genre_id),
FOREIGN KEY (show_id) REFERENCES show (show_id),
FOREIGN KEY (genre_id) REFERENCES genres(genre_id)
);

CREATE TABLE show_countries(
show_id integer,
country_id integer,
PRIMARY KEY (show_id, country_id),
FOREIGN KEY (show_id) REFERENCES show (show_id),
FOREIGN KEY (country_id) REFERENCES countries(country_id)
);

CREATE TABLE show_type(
show_id integer,
type_id integer,
PRIMARY KEY (show_id, type_id),
FOREIGN KEY (show_id) REFERENCES show (show_id),
FOREIGN KEY (type_id ) REFERENCES types(type_id)
);

CREATE TABLE show_cast(
show_id integer,
actor_id integer,
PRIMARY KEY (show_id, actor_id),
FOREIGN KEY (show_id) REFERENCES show (show_id),
FOREIGN KEY (actor_id) REFERENCES actors(actor_id)
);
"



# Execute the statement and get the results:
rs <- dbSendQuery(con, stmt)


### INSERT VALUES INTO TABLES
install.packages("splitstackshape", dep=T)
install.packages("ggplot2", dep=T)

#Load the csv file in a dataframe, df:

df <- read.csv('/Users/siyuan/Desktop/netflix_titles.csv')

#ETL of TABLE show
length(unique(data$show_id)) == nrow(data)

show_table = data[c('show_id', 'title', 'release_year', 'duration', 'description')]
dbWriteTable(con, name = 'show', value = show_table, row.names = F, append = T)


##ETL OF TABLE types
type_df <- data.frame('type' = unique(df$type))
type_df$type_id <- 1:nrow(type_df)
head(type_df)
types_df <- type_df[c('type_id', 'type')]
#load customer data to the database
dbWriteTable(con, name="types", value=types_df, row.names=FALSE, append=TRUE)

##ETL OF TABLE genres
#clean the data
listed_in <- as.factor(df$listed_in)
df$listed_in[is.na(df$listed_in)] <- 0
#split genres
s_genres = strsplit(as.character(df$listed_in), split=",", fixed=TRUE)
#create genres_id
genres_df <- data.frame('listed_in' = unique(s_genres))
genres_df$genres_id <- 1:nrow(genres_df)
# Create a new expanded dataframe
df_genres = data.frame(
  genres_id=rep(df$genres_id, sapply(s_genres, length)),
  listed_in=unlist(s_genres))
# Create temporary dataframe with unique cast
genres = data.frame('genres_id'= df_genres$genres_id,
                  'genres'= df_genres$genres)
genres_uni = unique(genres)
genres_uni$genres_id = 1:nrow(genres_uni)

dbWriteTable(con, name="cast", value=cast_uni, row.names=FALSE, append=TRUE)


##ETL OF TABLE show_type
show_type_list <- sapply(df$type,function(x) type_df$type_id [type_df$type == x]  )
head(show_type_list)

df$type_id<-show_type_list

show_type <- df[c('show_id','type_id')]
show_type

dbWriteTable(con, name="show_type", value=show_type , row.names=FALSE, append=TRUE)

#ETL of show_genres  
show_genres_list <- sapply(df$listed_in,function(x) genres_df$genres_id [genres_df$genres== x]  )
head(show_genres_list)
df$genre_id<-show_genres_list
df
show_genres <-  df[c('show_id','genre_id')]
head(show_genres)
dbWriteTable(con, name="show_genres", value=show_genres , row.names=FALSE, append=TRUE)

                         
##ETL OF TABLE tv_ratings
rating_df <- data.frame('rating' = unique(df$rating))
rating_df$rating_id <- 1:nrow(rating_df)
head(rating_df)
tv_ratings_df <- rating_df[c('rating_id','rating')]
#load customer data to the database
dbWriteTable(con, name="tv_ratings", value=tv_ratings_df, row.names=FALSE, append=TRUE)


##ETL OF TABLE show_rating

rating <- sapply(df$rating,function(x) rating_df$rating_id [rating_df$rating== x]  )
head(rating)

df$rating_id<-rating

show_rating <- df[c('show_id','rating_id')]
show_rating

dbWriteTable(con, name="show_rating", value=show_rating , row.names=FALSE, append=TRUE)


##ETL OF TABLE countries
country_df<-data.frame('country' = unique(df$country))
country_df$countriy_id <- 1:nrow(country_df)
country_type <- df[c('country_id','country')]

dbWriteTable(con, name="countries", value=country_type , row.names=FALSE, append=TRUE)

##ETL OF TABLE show_countries
df$country_id <- 1:nrow(df)
show_countries <- df[c('show_id', 'country_id')]

dbWriteTable(con, name="show_countries", value=show_countries  , row.names=FALSE, append=TRUE)



##ETL OF TABLE actors
#split cast
df$actor_id <- 1:nrow(df)
#create a subset of df corresponding to the tarders database table.
actors_df <- df[c('actor_id', 'actor_name')]
#load customer data to the database
dbWriteTable(con, name="actors", value=actors_df, row.names=FALSE, append=TRUE)


##ETL OF TABLE show_cast
df$actor_id <-1:nrow(df)
show_cast <- df[c('show_id', 'actor_id')]

dbWriteTable(con, name="show_cast", value=show_cast  , row.names=FALSE, append=TRUE)



##ETL OF TABLE duration
df$duration_id <- 1:nrow(df)
#create a subset of df corresponding to the tarders database table.
duration_df <- df[c('duration_id', 'duration')]
#load customer data to the database
dbWriteTable(con, name="duration", value=duration_df, row.names=FALSE, append=TRUE)



##ETL OF TABLE duration
duration_df <- data.frame('duration' = unique(df$duration))

duration_df$duration_id <- 1:nrow(duration_df)                                          
                        
head(duration_df)
nrow(duration_df)
duration_df <- duration_df[c('duration_id', 'duration')]
#load customer data to the database
dbWriteTable(con, name="duration", value=duration_df, row.names=FALSE, append=TRUE)


##ETL OF TABLE show_duration
duration_list <- sapply(df$duration,function(x) duration_df$duration_id [duration_df$duration == x]  )
head(duration_list)

df$duration_id<-duration_list

show_duration <- df[c('show_id','duration_id')]
show_duration 

dbWriteTable(con, name="show_duration", value=show_duration , row.names=FALSE, append=TRUE)


# date_added Table
date_add_table = df[c('show_id', 'date_added')]
dbWriteTable(con, name = 'date_added', value = date_add_table, row.names = F, append = T)

# customer_rating Table
customer_rating_table = df[c('show_id', 'rating')]
names(customer_rating_table)[2] = 'rating_level'
dbWriteTable(con, name = 'customer_rating', value = customer_rating_table, row.names = F, append = T)

# directors and movie_director Tables
library('splitstackshape')
library(dplyr)
directors_name = strsplit(as.character(df$director), split = ',', fixed = T)
test = data.frame(show_id = rep(df$show_id, sapply(directors_name, length)),
                  name = unlist(directors_name))

test$name = trimws(test$name) # remove white space
test$name[is.na(test$name)] = 'unknown' # replace na value

name_data = data.frame(name = unique(test$name))
name_data$director_id = 1:nrow(name_data)

director_id_list = sapply(test$name, function(x) name_data$director_id[name_data$name == x])
test$director_id = director_id_list
movie_director_table = test[c('show_id', 'director_id')][!duplicated(test[c('show_id', 'director_id')]),]
dbWriteTable(con, name = 'directors', value = name_data, row.names = F, append = T)
dbWriteTable(con, name = 'movie_director', value = movie_director_table, row.names = F, append = T)


#close the connection
dbDisconnect(con)
closeAllConnections()


# -- END R CODE --
