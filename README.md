# ETL Project: Video Games Global Sales

![Video_game_image](https://github.com/bradsmart1998/Project_2_Challenge/blob/main/Screenshots/video_dino.jpg)

## Contents
* [Project Summary](#proposal-header)
* [Project Aims](#aims)
* [Sources of Data](#data)


## <a id="proposal-header"></a>Project Summary
The purpose of the project was to find at least two data sources, and use these to create a larger database which could queried to provide detailed information. We have chosen to look at video games data. We initially sourced two data sets from Kaggle – one which provided information about sales data and another which provided information about reviews of the games. As these did not provide the full detail we wanted for our final data based we sourced a further data set from an API which gave a higher quality of review data. 

The original data sources were CSV files - these underwent some basic cleaning before being combined in PostgreSQL. They were then further cleaned and combined with the additional API data in a Jupyter Notebook, before being uploaded into the PostgreSQL data base and some basic queries performed to ensure the ETL process had been completed correctly. 

## <a id="aims"></a>Aims of the Project
The aim was to create a detailed data set which would be able to provide information about video games, their genre, the plaform they are aviaible on and information about the ratings. The image below shows an example of the completed database on PostgreSL. 

![completed_table](https://github.com/bradsmart1998/Project_2_Challenge/blob/main/Screenshots/Example_of_completed_table.png)

The full file can be found here 
[Final Games Database](https://github.com/bradsmart1998/Project_2_Challenge/blob/main/data/Final_gamed_data_fromPostgreSQL.csv)

## <a id="data"></a>Sources of Data
The inital data was collected from [Kaggle.com](https://www.kaggle.com/datasets) and further data was collected from
