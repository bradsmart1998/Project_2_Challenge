# ETL Project: Video Games Global Sales

![Video_game_image](https://github.com/bradsmart1998/Project_2_Challenge/blob/main/Screenshots/video_dino.jpg)

## Contents
* [Project Summary](#proposal-header)
* [Project Aims](#aims)
* [Sources of Data](#data)
* [Required Dependencies](#Rq-Dep)
* [Data Extraction](#Data-ext)
* [First Stage of Transformation](#Trans-1)
* [Loading and SQL Join](#Loading-1)
* [The Second Stage of Transformation](#Trans-2)
* [Additional Data Extraction from the API](#API-data)


## <a id="proposal-header"></a>Project Summary
The purpose of the project was to find at least two data sources, and use these to create a larger database which could queried to provide detailed information. We have chosen to look at video games data. We initially sourced two data sets from Kaggle – one which provided information about sales data and another which provided information about reviews of the games. As these did not provide the full detail we wanted for our final data based we sourced a further data set from an API which gave a higher quality of review data. 

The original data sources were CSV files - these underwent some basic cleaning before being combined in PostgreSQL. They were then further cleaned and combined with the additional API data in a Jupyter Notebook, before being uploaded into the PostgreSQL data base and some basic queries performed to ensure the ETL process had been completed correctly. 

## <a id="aims"></a>Aims of the Project
The aim was to create a detailed data set which would be able to provide information about video games, their genre, the plaform they are aviaible on and information about the ratings. The image below shows an example of the completed database on PostgreSL. 

![completed_table](https://github.com/bradsmart1998/Project_2_Challenge/blob/main/Screenshots/Example_of_completed_table.png)

The full file can be found here 
[Final Games Database](https://github.com/bradsmart1998/Project_2_Challenge/blob/main/data/Final_gamed_data_fromPostgreSQL.csv)

## <a id="data"></a>Sources of Data
The inital data was collected from [Kaggle.com](https://www.kaggle.com/datasets) and further data was collected from [RAWG.io](https://rawg.io/) 
All the data can be found in the [Data](https://github.com/bradsmart1998/Project_2_Challenge/tree/main/data) folder contained in the repository.

## <a id="Rq-Dep"></a> Required Dependencies
The following dependencies are required to complete the ETL process 
*  SQLAlchemy
* Pandas
* Requests
*  JSON
*  NumPy

## <a id="Data-ext"></a> Data Extraction
Both files from Kaggle.com are CSV files. Both files were converted into DataFrames 
```ruby 
# Data Source 2 - Videogames Review from https://www.kaggle.com/datasets/muhammadadiltalay/imdb-video-games
csv_file_2 = "imdb-videogames.csv"
games_review_df = pd.read_csv(csv_file_2)
games_review_df.head()
```

## <a id="Trans-1"></a> First Stage of Transformation
A data base table was created for each of the CSV files to allow for the data to be uploaded to PostgreSQL
```ruby
CREATE TABLE imdb_video_games (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    year INTEGER,
    certificate VARCHAR(255),
    rating DEC,
    votes INTEGER
);
```
The headings of the table were changed to ensure that these matched the database criteria, unwanted columns were dropped and commas were removed from the votes column to ensure that these would match the format specified by the database table. 
```ruby
#Remove the comma in the votes values
votes_df = pd.DataFrame(new_games_review_df['votes'].str.replace(",",""))
votes_df

#Join the Dataframe
new_games_review_df_2 = pd.merge(new_games_review_df, votes_df, left_index=True, right_index=True )

#Remove and rename colums
new_games_review_df_2.drop('votes_x', axis=1, inplace=True)

new_games_review_df_3 = new_games_review_df_2.rename(columns={'name': 'name', 'year' : 'year', 'certificate' :'certificate', 'rating': 'rating', 'votes_y' : 'votes'})
new_games_review_df_3.head()
```

## <a id="Loading-1"></a> Loading and SQL Join
The two sets of data from Kaggle were then loaded into the SQL Database. 
```Ruby
#Dataframe 1
new_games_sales_df_2.to_sql(name='video_game_sales', con=engine, if_exists='append', index=False)
#Dataframe 2
new_games_review_df_3.to_sql(name='imdb_video_games', con=engine, if_exists='append', index=False)
```
The desired columns were then joined in SQL to create a joined table on the name column. 
```ruby
CREATE TABLE joined_games AS
SELECT imdb_video_games.name, imdb_video_games.year, imdb_video_games.certificate, imdb_video_games.rating, imdb_video_games.votes, video_game_sales.platform, video_game_sales.publishers, video_game_sales.developer, video_game_sales.global_sales

FROM imdb_video_games
LEFT JOIN video_game_sales ON video_game_sales.name=imdb_video_games.name;
```
## <a id="Trans-2"></a> The Second Stage of Transformation
The data from the joined table was then loaded into a Jupiter Notebook 
```ruby
%load_ext sql
%sql $engine.url
data = %sql SELECT * FROM joined_games

# Make a data frame using the table from Postgres
games_df = pd.DataFrame(data)

```
The rows with no values for global sales were removed and these figures were converted in to float figures to ensure they were able to be used for calculations. Duplicate values were removed. 
```ruby
# Remove any rows which don't have a value for global sales 
games_df_values = games_df[games_df['global_sales'] > 0]

# Convert the Decimal figures from the database to float figures so these can be used in calculations
num_list = games_df_values['global_sales'].apply(pd.to_numeric, downcast='float')

#Convert the numerical figures into a dataframe
global_2 = pd.DataFrame(num_list)

# Merge the numerical figures into the data frame
merged_games_df = pd.merge(games_df_values, global_2, left_index=True, right_index=True)
merged_games_df.head()

unique_games_df = merged_games_df.drop_duplicates(subset=['name'], keep = 'first')
unique_games_df.head()

```


## <a id="API-data"></a>Additional Data Extraction from the API
On closer inspection of the data set we found that we still wanted to add in a genre column and a more accurate release date as the original data set only contained the year. We removed the more inaccurate columns from the Dataframe we had created.

To get more data on video games, We used the [RAWG API](https://rawg.io/apidocs). This API has information on over 500,000 games from over 50 different platforms.  To begin to obtain this data, this imported the API key from a hidden config file. 

The next step to extract data from this API was to build our query. I started with getting the base URL and then I identified the params needed to create the query URL. The issue that we had with extracting the data from this API, was that there were only 20 results on each page. Therefore, to get enough data, we needed to iterate through the pages(starting at page 1).

![API1](https://github.com/bradsmart1998/Project_2_Challenge/blob/main/Screenshots/Ap1.png)

We then ran the query URL on google to make sure that it worked, and then identified the data that we wanted to extract. We wanted to obtain the Metacritic rating, the release date, and the game genre from this API. We also extracted the video game name so that it could join to our other datasets, and we could identify what game the data referred to. To do this we created an empty lists to store the data.

![API2](https://github.com/bradsmart1998/Project_2_Challenge/blob/main/Screenshots/API2.png)

We then created the code to extract the data from the API. This started with creating a loop to travel through each page. For each page, we ran the query URL to get a response and then converted the response to a JSON. From there we looped through each game, identified the appropriate information in the JSON and appended it to the appropriate list.  To get the genre information, we extracted the first genre for each game as some games had sub-genres. 

![API3](https://github.com/bradsmart1998/Project_2_Challenge/blob/main/Screenshots/API3.png)

We then put these lists into a data frame and exported it to a CSV file. From there we could load this data into our table.

![API4](https://github.com/bradsmart1998/Project_2_Challenge/blob/main/Screenshots/API4.png)

