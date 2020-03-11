# Movies-ETL
Performing ETL process for the movies data.

## Extraction
First we downloaded and read the source data files:

- wikipedia.movies.json
- movies_metadata.csv
- ratings.csv

## Transformation
Using the basic steps of data cleaning proces <b>inspect-plan-execute</b> we do the following transformations to the 
<b><i>wikipedia.movies.json</i></b> file data set:
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

Cleaning the <b><i>kaggle_metadata.csv</i></b> file data:
<ol>
  <li>Only keep the rows where column 'adult' is False (the movies apropriate for children) and then drop the 'adult' column.</li>
  <li>Convert 'video' column into Boolean format.</li>
  <li>Convert 'budget', 'id' and 'popularity' columns into numeric values.</li>
  <li>Convert 'release date' into the datetime format.</li>
</ol>
Then we converted 'timestamp' column of the <b><i>ratings.csv</i></b> DataFrame into datetime format and generated descriptive
statistics for the column 'rating'. ...
Next we have merged two Wikipedia and Kaggle metadata DataFrames using INNER JOIN. The following columns have the duplicate information.
We had to explore the data in the columns to make a decision about resolution.
<table>
  <thead>
    <tr>
      <th>Wikipedia</th>
      <th>Kaggle</th>
      <th>Resolution</th>
   </tr>
  </thead>
  <tbody>
    <tr>
      <td>title_wiki</td>
      <td>title_kaggle</td>
      <td>Kaggle is a bit more consistent. Drop Wikipedia.</td>
    </tr>
    <tr>
      <td>running_time</td>
      <td>runtime</td>
      <td>Wikipedia had more outliers. Keep Kaggle, fill in zeros with Wikipedia data.</td>
    </tr>
    <tr>
      <td>budget_wiki</td>
      <td>budget_kaggle</td>
      <td>Keep Kaggle, fill in zeros with Wikipedia data.</td>
    </tr>
    <tr>
      <td>box_office</td>
      <td>revenue</td>
      <td>Keep Kaggle, fill in zeros with Wikipedia data.</td>
    </tr>
    <tr>
      <td>release_date_wiki</td>
      <td>release_date_kaggle</td>
      <td>Wikipedia is missing 11 release dates, Kaggle isn't missing any data. Drop Wikipedia.</td>
    </tr>
    <tr>
      <td>Language</td>
      <td>oroginal_language</td>
      <td>Wikipedia has more info about different languages, but also has NaN values. Kaggle has all the data present. 
      We drop Wikipedia.</td>
    </tr>
    <tr>
      <td>Production company(s)</td>
      <td>production_companies</td>
      <td>Drop Wikipedia.</td>
    </tr>
  </tbody>
</table>
After checking all the columns of the Metadata DataFrame we found out that the 'video' column contains only one value 'False' for 
each row which doesn't really provide any information so we drop the 'video' column.

Then we rearrange the columns considering them into the following groups:
<ol>
  <li>Identifying information ('imdb_id', 'id', 'title_kaggle', 'original_title', 'tagline', 'belongs_to_collection', 'url',
    'imdb_link' columns);</li>
  <li>Quantitative facts ('runtime', 'budget_kaggle', 'revenue', 'release_date_kaggle', 'popularity', 'vote_average', 'vote_count' 
    columns);</li>
  <li>Qualitative facts ('genres', 'original_language', 'overview', 'spoken_languages', 'Country' columns);</li>
  <li>Business data ('production_companies', 'production_countries', 'Distributor' columns);</li>
  <li>People ('Producer(s)', 'Director', 'Starring', 'Cinematography', 'Editor(s)', 'Writer(s)', 'Composer(s)', 'Based on' columns).</li>
</ol>
