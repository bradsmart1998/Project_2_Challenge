SELECT * FROM final_game_data

SELECT 
	name,
	platform,
	global_sales_millions
FROM 
	final_game_data
WHERE
	platform = 'Wii'
ORDER BY
	global_sales_millions
;

SELECT
	name,
	platform,
	publishers
FROM
	final_game_data
WHERE
	release_date BETWEEN '1986-01-01' and '1986-12-31'
;

SELECT
	name,
	platform,
	metacritic_rating
	genre
FROM
	final_game_data
WHERE
	genre = 'Action'
AND
	release_date BETWEEN '2013-01-01' and '2013-12-31'
;

SELECT 
	name,
	platform
	genre
FROM
	final_game_data
WHERE
	name LIKE 'Super%'
;