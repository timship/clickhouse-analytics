/*Нам поручили загрузить данные о перелетах из PG и сделать по ним агрегационную таблицу для аналитиков.
Необходимо настроить интеграцию с PG. 
Создать репликационный движок к таблице flights, назовем ее flights_remote*/

CREATE TABLE startde_student_data.marija_shkurat_wrn7887_flights_remote
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
ENGINE = PostgreSQL('startde.postgres.karpov.courses:5432', 'demo', 'flights', 'marija_shkurat_wrn7887', '[HIDDEN]', 'bookings')
