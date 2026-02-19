/*Создаем таблицу flights c типом mergetree и партицией по месяцам запланированной даты вылета
Заполняем ее из flights_remote*/

DROP TABLE startde_student_data.marija_shkurat_wrn7887_flights;

CREATE TABLE startde_student_data.marija_shkurat_wrn7887_flights
(
    `flight_id` UInt32,
    `flight_no` String,
    `scheduled_departure` DateTime,
    `scheduled_arrival` DateTime,
    `departure_airport` String,
    `arrival_airport` String,
    `status` String,
    `aircraft_code` String,
    `actual_departure` Nullable(DateTime),
    `actual_arrival` Nullable(DateTime)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(scheduled_departure)
ORDER BY flight_id

INSERT INTO startde_student_data.marija_shkurat_wrn7887_flights
SELECT * FROM startde_student_data.marija_shkurat_wrn7887_flights;
