-- Виконується автоматично при першому старті контейнера MySQL
CREATE DATABASE IF NOT EXISTS olympic_dataset;
USE olympic_dataset;

CREATE TABLE IF NOT EXISTS athlete_event_results (
    edition      TEXT,
    edition_id   INT,
    country_noc  VARCHAR(10),
    sport        TEXT,
    event        TEXT,
    result_id    BIGINT,
    athlete      TEXT,
    athlete_id   INT,
    pos          VARCHAR(20),
    medal        VARCHAR(10),
    isTeamSport  VARCHAR(10)
);

LOAD DATA INFILE '/var/lib/mysql-files/athlete_event_results.csv'
INTO TABLE athlete_event_results
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(edition, @edition_id, country_noc, sport, event, @result_id, athlete, @athlete_id, pos, @medal, @isTeamSport)
SET edition_id  = NULLIF(@edition_id, ''),
    result_id   = NULLIF(@result_id, ''),
    athlete_id  = NULLIF(@athlete_id, ''),
    medal       = NULLIF(TRIM(@medal), ''),
    isTeamSport = TRIM(TRAILING '\r' FROM @isTeamSport);

-- Користувач Airflow має створювати власну схему для таблиці результатів
GRANT ALL PRIVILEGES ON *.* TO 'airflow'@'%';
FLUSH PRIVILEGES;
