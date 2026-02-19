/*Чтобы эффективно планировать рейсы и улучшать клиентский сервис, 
 команде аналитиков требуется централизованное место хранения всей информация о маршрутах.
Создадим материализованное представление routes со следующими полями с запросом на базе таблицы flights:

flight_no (первичный ключ) — номер рейса
departure_airport — код аэропорта вылета
arrival_airport — код аэропорта прилета
aircraft_code — код самолета
duration — продолжительность полета, которая будет рассчитана как разница между запланированными временами вылета и прилета*/


CREATE MATERIALIZED VIEW startde_student_data.marija_shkurat_wrn7887_routes
(
    `flight_no` String,
    `departure_airport` String,
    `arrival_airport` String,
    `aircraft_code` String,
    `duration` Int64
)
ENGINE = MergeTree
ORDER BY flight_no
AS SELECT
    flight_no,
    any(departure_airport) AS departure_airport,
    any(arrival_airport) AS arrival_airport,
    any(aircraft_code) AS aircraft_code,
    any(dateDiff('second',
 scheduled_departure,
 scheduled_arrival)) AS duration
FROM startde_student_data.marija_shkurat_wrn7887_flights
GROUP BY flight_no;


INSERT INTO startde_student_data.marija_shkurat_wrn7887_flights
SELECT * FROM startde.flights;


SELECT *
FROM startde_student_data.marija_shkurat_wrn7887_flights;
