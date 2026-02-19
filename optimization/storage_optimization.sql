/* Оптимизация таблицы. Оптимизация хранения

Оптимизация хранения
>> Шаг 1
Для уменьшения объема таблицы приведем типы полей в порядок
Проверка всех текстовых полей на оптимальные типы
UInt8 \ Int8 - для небольших целочисленных
UInt32 - для больших целочисленных
Datetime - для даты

>> Шаг 2
Для уменьшения объема таблицы приведем типы полей в порядок
В таблице есть текстовые поля фиксированной длины, для них можно использовать FixedString
Необходимо заменить все такие поля

>> Шаг 3
json в строке хранить менее эффективно, удалим contact_data и заменим их на
passenger_phone и passenger_email

>> Шаг 4
Для времени и температур добавим CODEC(Delta, ZSTD)
Чтобы изменение было более плавное, добавим в сортировку таблицы поля scheduled_departure, departure_airport, arrival_airport

>> Шаг 5
все текстовые поля с низкой кардинальностью заменим на LowCardinality 
(аэропорт прилета\вылета, код самолета) или на Enum8(статус полета, погодные условия в аэропорту)

В результате преобразований у нас должна получиться таблица определённого размера (менее 555Mb) 
с нужными партициями, сортировкой и индексами.
*/

ALTER TABLE startde_student_data.marija_shkurat_wrn7887_fct_flights_weather_mart_opt
MODIFY COLUMN amount String;

CREATE OR REPLACE TABLE startde_student_data.marija_shkurat_wrn7887_fct_flights_weather_mart_opt
(
	--flights
	flight_id 					UInt32,
    flight_no 					FixedString(6),
    scheduled_departure			DateTime CODEC(Delta(4), ZSTD(1)),
    scheduled_arrival 			DateTime CODEC(Delta(4), ZSTD(1)),
    departure_airport 			LowCardinality(FixedString(3)),
    arrival_airport 			LowCardinality(FixedString(3)),
    status 						Enum8('Scheduled' = 1,
									 'Cancelled' = 2,
									 'Arrived' = 3,
									 'Departed' = 4),
    aircraft_code 				LowCardinality(FixedString(6)),
    actual_departure 			Nullable(DateTime) CODEC(Delta(4), ZSTD(1)),
    actual_arrival 				Nullable(DateTime) CODEC(Delta(4), ZSTD(1)),
    
    --ticket_flights_raw
	fare_conditions 			LowCardinality(String),
	amount 						String CODEC(ZSTD(1)),

	--tickets_raw
	ticket_no 					FixedString(13),
	book_ref 					FixedString(6),
	passenger_id 				FixedString(12),
	passenger_name 				LowCardinality(String),
	passenger_email 			LowCardinality(String),
	passenger_phone 			LowCardinality(String),
	
	--bookings_raw
	book_date 					DateTime CODEC(Delta(4), ZSTD(1)),
	total_amount 				LowCardinality(String) CODEC(ZSTD(1)),
	
	--weather_data_hourly
	departure_temperature		Float32 CODEC(Delta(4), ZSTD(1)), 
	departure_humidity 			UInt8, 
	departure_wind_speed 		Float32 CODEC(Delta(4), ZSTD(1)),
	departure_condition			Enum8('Clear' = 1,
									 'Cloudy' = 2,
									 'Rain' = 3,
									 'Snow' = 4,
									 'Fog' = 5,
									 'Thunderstorm' = 6),
	
	arrival_temperature			Float32 CODEC(Delta(4), ZSTD(1)), 
	arrival_humidity 			UInt8, 
	arrival_wind_speed 			Float32 CODEC(Delta(4), ZSTD(1)),
	arrival_condition			Enum8('Clear' = 1,
									 'Cloudy' = 2,
									 'Rain' = 3,
									 'Snow' = 4,
									 'Fog' = 5,
									 'Thunderstorm' = 6),
	INDEX idx_departure_airport departure_airport TYPE minmax GRANULARITY 4,

    INDEX idx_arrival_airport arrival_airport TYPE minmax GRANULARITY 4								 
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(scheduled_departure)
PRIMARY KEY flight_id
ORDER BY (flight_id,scheduled_departure, departure_airport, arrival_airport);

INSERT INTO startde_student_data.marija_shkurat_wrn7887_fct_flights_weather_mart_opt
(
	flight_id,
	flight_no,
	scheduled_departure,
	scheduled_arrival,
	departure_airport,
	arrival_airport,
	status,
	aircraft_code,
	actual_departure,
	actual_arrival,
	fare_conditions,
	amount,
	ticket_no,
	book_ref,
	passenger_id,
	passenger_name,
	passenger_email,
	passenger_phone,
	book_date,
	total_amount,
	departure_temperature,
	departure_humidity,
	departure_wind_speed,
	departure_condition,
	arrival_temperature,
	arrival_humidity,
	arrival_wind_speed,
	arrival_condition
)
SELECT
	toUInt32(flight_id) 								as 	flight_id,
    toString(flight_no) 								as 	flight_no,				
    scheduled_departure			::DateTime 				as scheduled_departure,
    scheduled_arrival 			::DateTime 				as scheduled_arrival,
    toString(departure_airport) 						as departure_airport,
    toString(arrival_airport) 			 				as arrival_airport,
    CAST(status as 	Enum8('Scheduled' = 1,'Cancelled' = 2,'Arrived' = 3,'Departed' = 4)) 	as status,
    toString(aircraft_code) 							as aircraft_code,
    actual_departure 			::Nullable(DateTime) 	as actual_departure,
    actual_arrival 				::Nullable(DateTime)	as actual_arrival,
	toString(fare_conditions) 							as fare_conditions,
	toString(amount) 									as amount,
	toString(ticket_no) 								as ticket_no,
	toString(book_ref) 									as book_ref,
	toString(passenger_id) 								as passenger_id,
	toString(passenger_name) 							as passenger_name,
	JSONExtractString(contact_data,'email') 			as passenger_email,
	JSONExtractString(contact_data,'phone')				as passenger_phone,
	parseDateTimeBestEffort(book_date) 					as book_date,
	toString(total_amount) 								as total_amount,
	toFloat32(departure_temperature)					as departure_temperature, 
	toUInt8(departure_humidity) 						as departure_humidity, 
	toFloat32(departure_wind_speed) 					as departure_wind_speed,
	CAST(departure_condition AS Enum8('Clear' = 1,'Cloudy' = 2,'Rain' = 3,'Snow' = 4,'Fog' = 5,'Thunderstorm' = 6)) as departure_condition,
	toFloat32(arrival_temperature)					as arrival_temperature, 
	toUInt8(arrival_humidity) 						as arrival_humidity, 
	toFloat32(arrival_wind_speed) 					as arrival_wind_speed,
	CAST(arrival_condition AS Enum8('Clear' = 1,'Cloudy' = 2,'Rain' = 3,'Snow' = 4,'Fog' = 5,'Thunderstorm' = 6)) as arrival_condition
FROM startde_student_data.marija_shkurat_wrn7887_fct_flights_weather_mart;

