import requests
import sqlite3
from datetime import datetime

## Begin database creation, connection, and insert of API data from above

database = "/Users/michaelrevelo/desktop/pyth/movies.db"
conn_movie_db = sqlite3.connect(database)
cur_movie_db = conn_movie_db.cursor()

## Connect to and get data from API

file_path = "/Users/michaelrevelo/desktop/pyth/marvel_movies.txt"

with open(file_path, 'r') as f:
    list_of_marvel_movies = f.readlines() ## The marvel_movies file is a plain text file

clean_list_of_marvel_movies = []

for i in list_of_marvel_movies:
    clean_movie = i.replace("\n", "")
    clean_list_of_marvel_movies.append(clean_movie)

api_key = '6734ac97'
base_url = 'https://www.omdbapi.com/'

## Example query ?t=Captain+America

for movie in clean_list_of_marvel_movies:
    full_api_query = base_url + '?apikey=' + api_key + '&t=' + movie.replace(" ", "+")
    r = requests.get(full_api_query) ## Native variable naming convention for requests library
    response = r.json()    
    try:
        assert response['Response'] == 'True'
        movie_name = response['Title']
        print(movie_name)
        release_date = datetime.strptime(response['Released'], '%d %b %Y') 
        print(release_date)
        movie_rating = float(response['imdbRating'])
        print(movie_rating)
        sql_statement = """ INSERT OR REPLACE INTO movies (movie_name, release_date, movie_rating) VALUES ('{0}', '{1}', {2})""".format(movie_name, release_date, movie_rating)
        print("Attempting to write data to movies table...")
        cur_movie_db.execute(sql_statement)
        conn_movie_db.commit()
    except AssertionError:
        pass




## For BIG data
"""
  1. Create a large dataset (flat file)
  2. Send that file to cloud storage (AwS s3, Google Cloud Storage, Azure Storage)
  3. Execute a Snowflake COPY statement


## Python pseudocode to get the CSV of your data
import pandas as pd

all_movies_names = []
all_ratings = []
all_dates = []

for each movie ...:
    Append each movie's data elements to the appropriate list



movie_data_to_write_to_table_df = pd.Dataframe(
                                 'movie_name' = all_movies_names,
                                 'release_date' = all_dates,
                                 ...
                                )

movie_data_to_write_to_table_file = pd.to_csv(movie_data_to_write_to_table_df, file = '/local_path/to/file.csv')

## AWS CLI S3 BASH Command to send something to s3
$ aws s3 cp {local_file} {s3_bucket_location}
$ aws s3 cp path/to/file.csv s3://name_of_s3_bucket/file.csv


## Snowflake COPY Command

copy into mytable from 's3://mybucket/./../a.csv';


"""



