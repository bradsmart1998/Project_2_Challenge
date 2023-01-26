drop table if exists video_game_sales;

CREATE TABLE video_game_sales (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    platform VARCHAR(255),
    year INTEGER,
    publishers VARCHAR(255),
    developer VARCHAR(255),
    global_sales DEC
);
SELECT * FROM video_game_sales;

drop table if exists imdb_video_games;
CREATE TABLE imdb_video_games (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    year INTEGER,
    certificate VARCHAR(255),
    rating DEC,
    votes INTEGER
);
SELECT * FROM imdb_video_games;

CREATE TABLE joined_games AS
SELECT imdb_video_games.name, imdb_video_games.year, imdb_video_games.certificate, imdb_video_games.rating, imdb_video_games.votes,
	 video_game_sales.platform, video_game_sales.publishers, video_game_sales.developer, video_game_sales.global_sales
FROM imdb_video_games
LEFT JOIN video_game_sales ON video_game_sales.name=imdb_video_games.name;


drop table if exists final_game_data;

CREATE TABLE final_game_data (
	id SERIAL PRIMARY KEY,
	Name VARCHAR(255),
	certificate VARCHAR(255),
	Platform VARCHAR(255),
	Publishers VARCHAR(255),
	Global_Sales_Millions FLOAT,
	Metacritic_Rating VARCHAR(255),
	Release_Date DATE,
	Genre VARCHAR(255)	
);

SELECT * FROM final_game_data