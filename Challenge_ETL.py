import json
import pandas as pd
import numpy as np
import os
import re
from sqlalchemy import create_engine
from config import db_password
import time

def ETL_function(Wiki_file, Kaggle_metafile, Rating_file):
    # Starting Extraction phase of the process
    print("--- Starting Extraction phase of the process ---")

    try:
        # Load JSON Wikipedia dataset
        with open(Wiki_file, mode='r') as file:
            wiki_movies_raw = json.load(file)
        print(f"JSON file size is: {len(wiki_movies_raw)}.")

        # Read ratings Kaggle CSV dataset
        ratings = pd.read_csv(Rating_file)

        # Read Kaggle metadata CSV dataset
        kaggle_metadata = pd.read_csv(Kaggle_metafile, low_memory=False)
    except Exception as e:
        print("The following exception occured while trying to load source files:")
        print(e)

    # Starting Transformation phase of the process
    print("--- Starting Extraction phase of the process ---")

    # Making a list of movies that have a 'Director' ('Dyrected by') value and 'imdb_link'
    wiki_movies = [movie for movie in wiki_movies_raw
                    if ('Director' in movie or 'Directed by' in movie) 
                    and 'imdb_link' in movie
                    and 'No. of episodes' not in movie]
    print(f"Size of the movies list that has Director value and imdb_link: {len(wiki_movies)}")

    # Loading wiki_movies into a DataFrame
    wiki_movies_df = pd.DataFrame(wiki_movies)
    print(f"Number of columns in the movies DataFrame is {len(wiki_movies_df.columns)}.")

    # Create a list of columns holding alternate title data
    alt_keys = ['Also known as', 'Arabic', 'Cantonese', 'Chinese', 'French', 'Hangul', 'Hebrew', 'Hepburn', 
                'Japanese', 'Literally', 'Mandarin', 'McCune–Reischauer', 'Original title', 'Polish', 
                'Revised Romanization', 'Romanized', 'Russian', 'Simplified', 'Traditional', 'Yiddish']

    # Create a function extracting all the alternative titles 
    # into one column and unifying other column names 
    def clean_movie(movie):
        movie = dict(movie)     # create a non-destructive copy
        alt_titles = {}         # create empty dict to hold alternative titles
    
        # Looping through the keys in the list of alt title columns
        for key in alt_keys:
            # if the column found in the movie
            if key in movie:
                alt_titles[key] = movie[key]
                movie.pop(key)
    
        # After looping through all the keys - add the list of alt titles to the movie
        if len(alt_titles) > 0:
            movie['Alt titles'] = dict(alt_titles)
            print(f"The following alt titles were found: {alt_titles}")
        
        # Defining a function to consolidate columns with the same data into one column
        def change_column_name(old_name, new_name):
            if old_name in movie:
                movie[new_name] = movie.pop(old_name)

        # Merging columns with a slightly different names
        change_column_name('Adaptation by', 'Writer(s)')
        change_column_name('Country of origin', 'Country')
        change_column_name('Directed by', 'Director')
        change_column_name('Distributed by', 'Distributor')
        change_column_name('Edited by', 'Editor(s)')
        change_column_name('Length', 'Running time')
        change_column_name('Original release', 'Release date')
        change_column_name('Music by', 'Composer(s)')
        change_column_name('Produced by', 'Producer(s)')
        change_column_name('Producer', 'Producer(s)')
        change_column_name('Productioncompanies ', 'Production company(s)')
        change_column_name('Productioncompany ', 'Production company(s)')
        change_column_name('Released', 'Release date')
        change_column_name('Screen story by', 'Writer(s)')
        change_column_name('Screenplay by', 'Writer(s)')
        change_column_name('Story by', 'Writer(s)')
        change_column_name('Theme music composer', 'Composer(s)')
        change_column_name('Written by', 'Writer(s)')
            
        return movie

    # Cleaning all the movies in a wiki_movies list   
    clean_movies = [clean_movie(movie) for movie in wiki_movies]

    # Set the wiki_movies_df to be the DataFrame created from clean_movies
    wiki_movies_df = pd.DataFrame(clean_movies)
    print(f"Current number of columns in the wiki_movies_df is {len(wiki_movies_df.columns)}.")

    # Extract IMDB IDs for each movie using regular 
    if "imdb_link" in wiki_movies_df:
        wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
        print(f"Current length of the wiki_movies DataFrame is {len(wiki_movies_df)}.")

        # Delete rows with the duplicate IMDB IDs
        wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)
        print(f"Length of the wiki_movies DataFrame after removing duplicate IMDB IDs is {len(wiki_movies_df)}.")
    else:
        print("The Wiki dataset doesn't have 'imdb_link' column.")

    # Create a list of columns containing less than 90% null values
    wiki_columns_to_keep = [ column for column in wiki_movies_df.columns 
                                if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9 ]
    # Delete the columns containing 90% or more null values
    wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]
    print(f"Number of columns containing less than 90% NULLs in the DataFrame is {len(wiki_movies_df.columns)}.")

    # Create a function to parse dollars in monetary data values
    def parse_dollars(s):
        # if s is not a string, return NaN
        if type(s) != str:
            return np.nan

        # if input is of the form $###.# million
        if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):
            # remove dollar sign and " million"
            s = re.sub('\$|\s|[a-zA-Z]','', s)
            # convert to float and multiply by a million
            value = float(s) * 10**6
            return value

        # if input is of the form $###.# billion
        elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):
            # remove dollar sign and " billion"
            s = re.sub('\$|\s|[a-zA-Z]','', s)
            # convert to float and multiply by a billion
            value = float(s) * 10**9
            return value

        # if input is of the form $###,###,### or $###.###
        elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):
            # remove dollar sign and commas or periods
            s = re.sub('\$|,|\.','', s)
            # convert to float
            value = float(s)
            return value

        else:
            # otherwise, return NaN
            return np.nan

    # Create regular expression for the different forms of box office
    form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
    form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'

    # Making a list of box_office values
    if "Box office" in wiki_movies_df:
        box_office = wiki_movies_df['Box office'].dropna()
        
        # For the values which are not the string type join the list values into strings with the '' separator
        box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)

        # Replace the numbers given as a range with just one number
        box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)

        # Extract numeric box office values from strings and turn them into decimal numbers
        wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', 
                                                            flags=re.IGNORECASE)[0].apply(parse_dollars)

        # Delete the old 'Box office' column from wiki_movies DataFrame
        wiki_movies_df.drop('Box office', axis=1, inplace=True)
    else:
        print("The Wiki dataset doesn't have 'Box office' column!")

    # Create a Series from the non-null values of the column 'Budget'
    if "Budget" in wiki_movies_df:
        budget = wiki_movies_df['Budget'].dropna()

        # Convert any lists to strings
        budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)

        # Remove any values between a dollar sign and a hyphen (for budgets given in ranges)
        budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)

        # Delete numbers in square brackets for budget
        budget = budget.str.replace(r'\[\d+\]\s*', '')

        # Extract numeric budget values from strings and turn them into decimal numbers
        wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
        # Delete the old 'Budget' column
        wiki_movies_df.drop('Budget', axis=1, inplace=True)
    else:
        print("Wiki dataset doesn't have 'Budget' column!")

    if "Release date" in wiki_movies_df:
        # Create a Series from the non-null values of the column 'Release date' and turn all lists into strings
        release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)

        # Create regular expressions to match different formats of the date
        date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
        date_form_two = r'\d{4}.[01]\d.[123]\d'
        date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
        date_form_four = r'\d{4}'

        # Extract the dates, turn them into a unified date format and create new column
        wiki_movies_df['release_date'] = pd.to_datetime(
                            release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], 
                            infer_datetime_format=True)
        # Delete the old 'Release date' column
        wiki_movies_df.drop('Release date', axis=1, inplace=True)
    else:
        print("Wiki dataset doesn't have 'Release date' column!")

    if "Running time" in wiki_movies_df:
        # Create a Series from the non-null values of the column 'Running time' and turn all lists into strings
        running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)

        # Extracting the known values of the running time
        running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')

        # Turning the strings into columns with numeric values and replacing empty strings with 0
        running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)

        # Create new 'running_time' with the unified time values
        wiki_movies_df['running_time'] = running_time_extract.apply(
                                                    lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
        # Delete the old 'Running time' column
        wiki_movies_df.drop('Running time', axis=1, inplace=True)
    else:
        print("Wiki dataset doesn't have 'Running time' column!")

    # Cleaning the Kaggle Metadata file
    if "adult" in kaggle_metadata:
        # Only keep rows where column 'adult' is False, and then drop the 'adult' column.
        kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')

    if "video" in kaggle_metadata:
        # Convert 'video' column into Boolean format
        kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'

    # Convert 'budget', 'id' and 'popularity' into numeric values
    if "budget" in kaggle_metadata:
        kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
    if "id" in kaggle_metadata:
        kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
    if "popularity" in kaggle_metadata:
        kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')

    # Convert 'release date' into the datetime format
    if "release_date" in kaggle_metadata:
        kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])

    # Converting 'timestamp' column of the ratings DataFrame into datetime format
    if "timestamp" in ratings:
        ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')

    try:
        # Merge Wikipedia and Kaggle metadata DataFrames using INNER JOIN (default)
        movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki', '_kaggle'])

        # Looking for the duplicate columns and choosing wchich one to keep and which to drop:
        # Two movies data got merged together. Dropping the row.
        movies_df = movies_df.drop(movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & (movies_df['release_date_kaggle'] < '1965-01-01')].index)

        # -----------------------------------------------------------------------------------------------
        #       Wikipedia       |      Kaggle         |        Resolution                               |
        # -----------------------------------------------------------------------------------------------
        #  title_wiki           | title_kaggle        | Drop Wikipedia.                                 |
        # -----------------------------------------------------------------------------------------------
        #  running_time         |  runtime            | Keep Kaggle, fill in zeros with Wikipedia data. |
        # -----------------------------------------------------------------------------------------------
        #  budget_wiki          | budget_kaggle       | Keep Kaggle, fill in zeros with Wikipedia data. |
        # -----------------------------------------------------------------------------------------------
        #  box_office           |  revenue            | Keep Kaggle, fill in zeros with Wikipedia data. |
        # -----------------------------------------------------------------------------------------------
        #  release_date_wiki    | release_date_kaggle | Drop Wikipedia.                                 |
        # -----------------------------------------------------------------------------------------------
        #  Language             | original_language   | Drop Wikipedia.                                 |
        # -----------------------------------------------------------------------------------------------
        # Production company(s) | production_companies | Drop Wikipedia.                                |
        # -----------------------------------------------------------------------------------------------

        # Execution of the above plan: dropping Wikipedia columns
        movies_df.drop(columns=['title_wiki', 'release_date_wiki', 'Language', 'Production company(s)'], inplace=True)

        # Create a function to replace missing Kaggle values with Wikipedia values
        def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
            # DataFrame.apply(... , axis = 1) means "apply function to each row"
            df[kaggle_column] = df.apply(
                lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column], axis = 1)
            df.drop(columns=wiki_column, inplace=True)

        # Replace missing Kaggle values with Wikipedia values and dropping Wikipedia columns
        fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
        fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
        fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')

        # Reorder the columns to make the dataset easier to read for the hackathon participants.
        movies_df = movies_df[['imdb_id', 'id', 'title_kaggle', 'original_title', 'tagline', 'belongs_to_collection', 'url', 'imdb_link', 
                            'runtime', 'budget_kaggle', 'revenue', 'release_date_kaggle', 'popularity', 'vote_average', 'vote_count', 
                            'genres', 'original_language', 'overview', 'spoken_languages', 'Country',
                            'production_companies', 'production_countries', 'Distributor',
                            'Producer(s)', 'Director', 'Starring', 'Cinematography', 'Editor(s)', 'Writer(s)', 'Composer(s)', 'Based on'
                            ]]

        movies_df.rename({'id': 'kaggle_id',
                        'title_kaggle': 'title',
                        'url': 'wikipedia_url',
                        'budget_kaggle': 'budget',
                        'release_date_kaggle': 'release_date',
                        'Country': 'country',
                        'Distributor': 'distributor',
                        'Producer(s)': 'producers',
                        'Director': 'director',
                        'Starring': 'starring',
                        'Cinematography': 'cinematography',
                        'Editor(s)': 'editors',
                        'Writer(s)': 'writers',
                        'Composer(s)': 'composers',
                        'Based on': 'based_on'},
                        axis='columns', inplace=True)
    except Exception as e:
        print("The following exception occured while merging Wiki and Kaggle metadata datasets:")
        print(e)

    try:
        # Group the rows by movie and rating value, we'll change the 'user_ID' column to counts of people given the movie certain rating
        rating_counts = ratings.groupby(['movieId', 'rating'], as_index=False).count().rename({'userId':'count'}, axis=1)

        # Reorginizing rows and columns as follows:
        rating_counts = rating_counts.pivot(index='movieId', columns='rating', values='count')

        # Rename the columns
        rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]

        # Merge movies_df and rating_counts DataFrames using LEFT JOIN to keep all the data from movies_df DataFrame
        movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')
        # Fill the NaN values for the ratings columns with zeros
        movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)
    except Exception as e:
        print("The following exception occured while processing ratings dataset:")
        print(e)    

    # Starting Loading phase of the process
    print("--- Starting Loading phase of the process ---")

    # Create a connection to the local Postgres Server
    db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"
    engine = create_engine(db_string)

    try:
        # Loading movies data DataFrame to PostgreSQL
        movies_df.to_sql(name='movies', con=engine, if_exists="append")
    except Exception as e:
        print(f"The following exception occured while trying to load movie data to SQL:")
        print(e)

    # Loading ratings data to PostgreSQL in chunks
    rows_imported = 0
    start_time = time.time()
    for data in pd.read_csv(Rating_file, chunksize=1000000):
        
        print(f'Importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
        try:
            data.to_sql(name='ratings', con=engine, if_exists='append')
            rows_imported += len(data)
            print(f'Done. {time.time() - start_time} total seconds elapsed.') 
        except Exception as e:
            print(f"The following exception occured while trying to load rating data to SQL:")
            print(e)


# Testing ETL_function with our datasets
JSON_file = os.path.join("Resources/", "wikipedia.movies.json")
kaggle_file_1 = os.path.join("Resources/", "movies_metadata.csv")
kaggle_file_2 = os.path.join("Resources/", "ratings.csv")

ETL_function(JSON_file, kaggle_file_1, kaggle_file_2)


