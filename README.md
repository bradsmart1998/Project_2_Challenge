# ETL Project: Video Games Global Sales

![Video_game_image](https://github.com/bradsmart1998/Project_2_Challenge/blob/main/Screenshots/video_dino.jpg)

## Contents
* [Project Summary](#proposal-header)
* [Project Aims](#aims)
* [Sources of Data](#data)
* [Required Dependencies](#Rq-Dep)
* [Data Extraction](#Data-ext)
* [First Stage of Transformation](#Trans_1)


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

## <a id="Trans_1"></a> First Stage of Transformation
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


