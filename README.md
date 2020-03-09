# Movies-ETL
Performing ETL process for the movies data.

## Extraction
First we downloaded and read the source data files:

- wikipedia.movies.json
- movies_metadata.csv
- ratings.csv

## Transformation
Using the basic steps of data cleaning proces <b>inspect-plan-execute</b> we do the following transformations to the wikipedia.movies
JSON file data set:
<ol>
  <li>We only take movies that have 'Director' (or 'Dyrected by') and 'imdb_link' keys not empty and that are not TV shows 
  (i.e don't have 'No. of episodes' key).</li>
  <li>We put all the alternative titles (or titles in different languages) into a dictionary, delete all the alt titles from the movies
  and add the dictionary with alt titles to the movie as a separate column called 'Alt titles'.</li>
  <li>Consolidate the columns with a slightly different names into one column (for example, 'Director' and 'Derected by', 'Editor' and
  'Edited by' and etc.).</li>
  <li>Using regular expression we extract IMDB IDs for each movie into separate column 'imdb_id' and then delete the rows with duplicat
  IMDB IDs.</li>
  <li>Then we delete columns containing mostly NULL values (more than 6,000 null values).</li>
  <li>Extract numeric box office values from strings or arrays, turn them into decimal numbers and replacing the old 'Box office'
  column with the new one 'box_office'.</li>
  <li>Extract numeric budget values from strings or arrays, turn them into decimal numbers and replacing the old 'Budget' column 
  with the new one 'budget'.</li>
  <li>Extract the 'Release date' and 'Running time' values from strings or arrays, turn them into date format and number of minutes
  accordingly and replace the old 'Release date' and 'Running time' columns with the new ones.</li>
</ol>
Cleaning the kaggle_metadata.csv file:
<ol>
  <li> Only keep the rows where column 'adult' is False (the movies apropriate for children) and then drop the 'adult' column.</li>
  <li></li>
  <li></li>
  <li></li>
</ol>
