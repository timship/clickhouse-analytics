/*Создание витрины fct_flights_weather_mart
Используем ASOF join для поиска ближайшего погодного условия ко времени актуального вылета (departure_) и прилета (arrival_)

Добавим в витрину fct_flights_weather_mart столбцы
temperature(Float32) 
humidity (UInt8) 
wind_speed (Float32)
condition(String)

для погоды в аэропорте вылета (departure_) и аэропорте прилета (arrival_).*/

CREATE OR REPLACE TABLE startde_student_data.marija_shkurat_wrn7887_fct_flights_weather_mart
(
	--flights
	flight_id 					UInt32,
    flight_no 					String,
    scheduled_departure			DateTime,
    scheduled_arrival 			DateTime,
    departure_airport 			String,
    arrival_airport 			String,
    status 						String,
    aircraft_code 				String,
    actual_departure 			DateTime,
    actual_arrival 				DateTime,
    
    --ticket_flights_raw
	fare_conditions 			String,
	amount 						String,

	--tickets_raw
	ticket_no 					String,
	book_ref 					String,
	passenger_id 				String,
	passenger_name 				String,
	contact_data 				String,
	
	--bookings_raw
	book_date 					String NOT NUll,
	total_amount 				String NOT NULL,
	
	--weather_data_hourly
	departure_temperature		Float32, 
	departure_humidity 			UInt8, 
	departure_wind_speed 		Float32,
	departure_condition			String,
	
	arrival_temperature			Float32, 
	arrival_humidity 			UInt8, 
	arrival_wind_speed 			Float32,
	arrival_condition			String
)
ENGINE = MergeTree()
ORDER BY (ticket_no, flight_id);

INSERT INTO startde_student_data.marija_shkurat_wrn7887_fct_flights_weather_mart
SELECT 
	--flights
	ffm.flight_id			::UInt32, 
    ffm.flight_no			::String,
    ffm.scheduled_departure	::DateTime,
    ffm.scheduled_arrival	::DateTime, 
    ffm.departure_airport	::String,
    ffm.arrival_airport		::String, 
    ffm.status				::String,
    ffm.aircraft_code		::String, 
    ffm.actual_departure	::DateTime,
    ffm.actual_arrival		::DateTime,
    
    --ticket_flights_raw
	ffm.fare_conditions		::String,
	ffm.amount				::String,
	
	--tickets_raw
	ffm.ticket_no			::String,
	ffm.book_ref			::String,
	ffm.passenger_id		::String,
	ffm.passenger_name		::String,
	ffm.contact_data		::String,
	
	--bookings_raw
	ffm.book_date			::String,
	ffm.total_amount		::String,
	
	--weather_data_hourly
	wdh1.temperature		::Float32 as departure_temperature, 
	wdh1.humidity 			::UInt8 as departure_humidity, 
	wdh1.wind_speed 		::Float32 as departure_wind_speed,
	wdh1.condition			::String as departure_condition,
	
	wdh2.temperature		::Float32 as arrival_temperature, 
	wdh2.humidity 			::UInt8 as arrival_humidity, 
	wdh2.wind_speed 		::Float32 as arrival_wind_speed,
	wdh2.condition			::String as arrival_condition
	
FROM startde_student_data.marija_shkurat_wrn7887_fct_flights_mart as ffm
ASOF LEFT JOIN startde.weather_data_hourly as wdh1 
  on ffm.departure_airport = wdh1.airport AND ffm.actual_departure >= wdh1.timestamp
ASOF LEFT JOIN startde.weather_data_hourly as wdh2 
  on ffm.arrival_airport = wdh2.airport AND actual_arrival >= wdh2.timestamp
WHERE ffm.status = 'Arrived';  
